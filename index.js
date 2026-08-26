// ============================================================================
// BINANCE FLOW MONITOR + MEXC FUTURES AUTO-EXECUTION (merged, single process)
// ============================================================================
// Сигнал детектується і угода відкривається в ОДНОМУ процесі: жодного
// проміжного Telegram-бота, що читає повідомлення іншого бота. Функція
// executeTrade() викликається напряму з обробника WebSocket-повідомлень.
// ============================================================================

const config = require('./config/settings');
const logger = require('./utils/logger');
const { msUntilNextMinute, msUntilNextUtcTime, isWithinTradingHours, getCurrentDate } = require('./utils/helpers');

const mexc = require('./services/mexc.service');
const mexcUserStream = require('./services/mexc-user-stream.service');
const telegram = require('./services/telegram.service');
const positionService = require('./services/position.service');
const riskService = require('./services/risk.service');

const { TradeAggregator, SignalEngine, CooldownManager } = require('./core/signal-engine');
const MultiWebSocketManager = require('./core/websocket-manager');
const OpenInterestTracker = require('./core/open-interest-tracker');

// Версійна мітка — щоб при діагностиці одразу бачити в логах, яка саме
// версія коду реально запущена (а не гадати після кожного фіксу).
const BOT_BUILD = '2026-08-16-circuit-breakers';

// tradeAggregator створюється в start(), але потрібен і для моніторингу BTC
// (checkBtcCrashCircuitBreaker) — тому оголошений на рівні модуля.
let tradeAggregator = null;

// OI-трекер стежить і за торговими символами (для per-symbol фільтра), і за
// BTCUSDT (для ринкового circuit breaker), якщо він увімкнений.
const oiTrackedSymbols = [...config.getEnabledSymbols()];
if (config.btcCircuitBreaker.enabled && !oiTrackedSymbols.includes(config.btcCircuitBreaker.monitorSymbol)) {
  oiTrackedSymbols.push(config.btcCircuitBreaker.monitorSymbol);
}

const openInterestTracker = new OpenInterestTracker(
  oiTrackedSymbols,
  config.WINDOW_SECONDS,
  config.oiFilter.pollIntervalMs
);

// OI-ФІЛЬТР ПІДТВЕРДЖЕННЯ: див. детальний коментар у core/open-interest-tracker.js.
// Блокує вхід, якщо OI ще помітно росте (свіжі позиції відкриваються,
// рух ще не факт що видихся); дозволяє, якщо OI плаский/падає (позиції
// закриваються — ознака виснаження). Правило симетричне для LONG і SHORT,
// тому напрямок сигналу тут не потрібен.
function checkOiConfirmation(symbol) {
  const cfg = config.oiFilter;
  if (!cfg.enabled) return { pass: true, reason: 'filter_disabled', changePercent: null };

  const stats = openInterestTracker.getChangeStats(symbol, config.WINDOW_SECONDS * 1000);
  if (!stats.sufficientData) {
    return { pass: cfg.passOnInsufficientData, reason: 'insufficient_data', changePercent: null };
  }

  const pass = stats.changePercent <= cfg.maxIncreasePercent;
  return {
    pass,
    reason: pass ? 'oi_flat_or_falling' : 'oi_still_rising',
    changePercent: stats.changePercent
  };
}

// ----------------------------------------------------------------------------
// CIRCUIT BREAKERS
// 1) BTC crash+OI: якщо BTC різко рухається і OI по ньому одночасно росте
//    (свіжі позиції відкриваються -> рух ще має "паливо", схоже на
//    каскад/маніпуляцію) -> стоп усіх нових входів по ВСІХ символах на
//    певний час + примусове закриття щойно (< N хв тому) відкритих позицій.
// 2) Серія збиткових угод поспіль (по всіх символах) -> ескалаційна
//    зупинка: перший раз за день — на кілька годин, другий раз за той
//    самий день — до початку наступного робочого вікна.
// В ОБОХ випадках сигнали й далі детектуються і показуються в Telegram як
// "проігноровані" (validateEntry повертає причину, яка йде через звичайний
// шлях formatSignalSkipped) — це навмисно, щоб потім можна було вручну
// оцінити, чи спрацювання було доречним.
// Circuit breaker НЕ обходиться навіть тестовою угодою (isTest) — це
// запобіжник реальної небезпеки, а не питання зручності тестування.
// ----------------------------------------------------------------------------
const circuitBreaker = {
  haltedUntil: null,
  haltReason: null,
  consecutiveLosses: 0,
  lossBreakerCountToday: 0,
  lossBreakerDate: getCurrentDate()
};

function isTradingHalted() {
  if (!circuitBreaker.haltedUntil) return false;
  if (Date.now() >= circuitBreaker.haltedUntil) {
    logger.info(`[CIRCUIT BREAKER] Halt завершено (був: ${circuitBreaker.haltReason})`);
    circuitBreaker.haltedUntil = null;
    circuitBreaker.haltReason = null;
    return false;
  }
  return true;
}

function getHaltStatusText() {
  if (!circuitBreaker.haltedUntil) return null;
  const untilStr = new Date(circuitBreaker.haltedUntil).toISOString().substring(11, 19);
  return `${circuitBreaker.haltReason} (до ${untilStr} UTC)`;
}

function triggerHalt(durationMs, reason) {
  circuitBreaker.haltedUntil = Date.now() + durationMs;
  circuitBreaker.haltReason = reason;
  const untilStr = new Date(circuitBreaker.haltedUntil).toISOString().substring(11, 19);
  logger.error(`[CIRCUIT BREAKER] 🛑 ТОРГІВЛЮ ЗУПИНЕНО до ${untilStr} UTC: ${reason}`);
  telegram.send(
    `🛑 <b>ТОРГІВЛЮ ТИМЧАСОВО ЗУПИНЕНО</b>\n\n` +
    `<b>Причина:</b> ${reason}\n` +
    `<b>До:</b> ${untilStr} UTC\n\n` +
    `Сигнали й далі детектуватимуться і показуватимуться як проігноровані (для подальшого аналізу) — просто без відкриття угод.`
  ).catch(() => {});
}

// ---- 1) BTC CRASH + OI ----
async function forceCloseRecentPositions(maxAgeMinutes, reason) {
  const maxAgeMs = maxAgeMinutes * 60 * 1000;
  const now = Date.now();
  for (const [symbol, tracked] of positionService.getAllOpenPositions()) {
    if (now - tracked.openedAt <= maxAgeMs) {
      logger.warn(`[CIRCUIT BREAKER] Примусово закриваю ${symbol} (відкрито ${Math.round((now - tracked.openedAt) / 1000)}с тому)`);
      await positionService.forceClosePosition(symbol, reason);
    }
  }
}

function checkBtcCrashCircuitBreaker() {
  const cfg = config.btcCircuitBreaker;
  if (!cfg.enabled) return;
  if (isTradingHalted()) return; // вже зупинено, нема сенсу перевіряти знову
  if (!tradeAggregator) return;

  const stats = tradeAggregator.getStats(cfg.monitorSymbol);
  if (!stats) return; // ще нема даних по BTC

  if (Math.abs(stats.priceChange) < cfg.minMovePercent) return; // рух ще не жорсткий

  const oiStats = openInterestTracker.getChangeStats(cfg.monitorSymbol, config.WINDOW_SECONDS * 1000);
  if (!oiStats.sufficientData) return;

  // Той самий напрямок-незалежний критерій, що й у per-symbol OI-фільтрі:
  // ціна різко рухається (в будь-який бік) + OI одночасно росте -> нові
  // позиції відкриваються в напрямку руху -> рух ще має "паливо".
  if (oiStats.changePercent < cfg.minOiIncreasePercent) return;

  const reason = `BTC ${stats.priceChange >= 0 ? '+' : ''}${stats.priceChange.toFixed(2)}% за ${Math.round(stats.duration)}с, ` +
    `OI ${oiStats.changePercent >= 0 ? '+' : ''}${oiStats.changePercent.toFixed(2)}% — ймовірний каскад/маніпуляція`;

  triggerHalt(cfg.haltDurationMs, reason);
  forceCloseRecentPositions(cfg.forceCloseMaxAgeMinutes, `BTC circuit breaker: ${reason}`)
    .catch(err => logger.error(`[CIRCUIT BREAKER] Помилка примусового закриття: ${err.message}`));
}

// ---- 2) СЕРІЯ ЗБИТКОВИХ УГОД ПОСПІЛЬ ----
function resetLossBreakerDateIfNeeded() {
  const today = getCurrentDate();
  if (today !== circuitBreaker.lossBreakerDate) {
    circuitBreaker.lossBreakerDate = today;
    circuitBreaker.lossBreakerCountToday = 0;
  }
}

positionService.setOnPositionClosedCallback((closedData) => {
  const cfg = config.lossStreakCircuitBreaker;
  if (!cfg.enabled) return;

  resetLossBreakerDateIfNeeded();

  if (closedData.pnl < 0) {
    circuitBreaker.consecutiveLosses++;
    logger.warn(`[CIRCUIT BREAKER] Збиткова угода поспіль: ${circuitBreaker.consecutiveLosses}/${cfg.maxConsecutiveLosses}`);

    if (circuitBreaker.consecutiveLosses >= cfg.maxConsecutiveLosses) {
      circuitBreaker.lossBreakerCountToday++;
      circuitBreaker.consecutiveLosses = 0; // рахуємо серію заново після halt

      if (circuitBreaker.lossBreakerCountToday >= cfg.maxOccurrencesPerDay) {
        const untilTomorrow = msUntilNextUtcTime(config.tradingHours.startHour, 0);
        triggerHalt(untilTomorrow, `Друга серія з ${cfg.maxConsecutiveLosses} стоп-лоссів поспіль за сьогодні — торгівля зупинена до завтра`);
      } else {
        triggerHalt(cfg.firstHaltHours * 60 * 60 * 1000, `${cfg.maxConsecutiveLosses} стоп-лосси поспіль`);
      }
    }
  } else {
    if (circuitBreaker.consecutiveLosses > 0) {
      logger.info(`[CIRCUIT BREAKER] Прибуткова угода — лічильник збиткової серії скинуто (був ${circuitBreaker.consecutiveLosses})`);
    }
    circuitBreaker.consecutiveLosses = 0;
  }
});

// ----------------------------------------------------------------------------
// Денна статистика / ліміти
// ----------------------------------------------------------------------------
const daily = {
  date: getCurrentDate(),
  tradesOpened: 0,
  signalsDetected: 0,
  signalsSkipped: 0
};

function resetDailyIfNeeded() {
  const today = getCurrentDate();
  if (today !== daily.date) {
    daily.date = today;
    daily.tradesOpened = 0;
    daily.signalsDetected = 0;
    daily.signalsSkipped = 0;
  }
}

// ----------------------------------------------------------------------------
// Валідація перед входом (викликається і одразу при сигналі, і ще раз
// безпосередньо перед виконанням — стан міг змінитись за час очікування
// початку наступної хвилини)
// ----------------------------------------------------------------------------
function validateEntry(symbol, { skipTradingHours = false, skipOiFilter = false } = {}) {
  const cfg = config.getSymbolConfig(symbol);
  if (!cfg || !cfg.enabled) return `Символ ${symbol} вимкнено в конфізі`;

  // Circuit breaker — НЕ обходиться навіть тестовою угодою (isTest), це
  // запобіжник реальної небезпеки, а не питання зручності тестування.
  if (isTradingHalted()) {
    return `Circuit breaker: ${getHaltStatusText()}`;
  }

  if (!skipTradingHours && !isWithinTradingHours(config.tradingHours)) {
    return `Поза робочими годинами (${config.tradingHours.startHour}:00–${config.tradingHours.endHour}:00 UTC)`;
  }

  if (positionService.hasOpenPosition(symbol)) return `Вже є відкрита позиція по ${symbol}`;

  if (positionService.getOpenPositionsCount() >= config.trading.maxOpenPositions) {
    return `Досягнуто ліміт відкритих позицій (${config.trading.maxOpenPositions})`;
  }

  resetDailyIfNeeded();
  if (daily.tradesOpened >= config.trading.maxDailyTrades) {
    return `Досягнуто денний ліміт угод (${config.trading.maxDailyTrades})`;
  }

  if (!skipOiFilter) {
    const oiCheck = checkOiConfirmation(symbol);
    if (!oiCheck.pass) {
      const pct = oiCheck.changePercent !== null ? `${oiCheck.changePercent >= 0 ? '+' : ''}${oiCheck.changePercent.toFixed(2)}%` : 'н/д';
      return `OI-фільтр: ще росте (${pct}, поріг ≤${config.oiFilter.maxIncreasePercent}%) — рух ще має "паливо"`;
    }
  }

  return null; // валідно
}

// ----------------------------------------------------------------------------
// Обробник сигналу — викликається СИНХРОННО з WebSocket-менеджера
// ----------------------------------------------------------------------------
function handleSignal(symbol, stats, interpretation) {
  resetDailyIfNeeded();
  daily.signalsDetected++;

  const direction = interpretation.direction;
  const cfg = config.getSymbolConfig(symbol);

  logger.info(`[SIGNAL] ${symbol} ${direction} | vol=$${stats.totalVolume.toFixed(0)} dom=${stats.dominance.toFixed(1)}% Δ=${stats.priceChange.toFixed(2)}%`);

  const earlyReason = validateEntry(symbol);
  if (earlyReason) {
    daily.signalsSkipped++;
    logger.warn(`[SIGNAL] ${symbol} проігноровано одразу: ${earlyReason}`);
    telegram.send(telegram.formatSignalSkipped(symbol, direction, earlyReason)).catch(() => {});
    return;
  }

  const delayMs = config.execution.entryAtNextMinute
    ? msUntilNextMinute(config.execution.entryBufferMs)
    : 0;

  const plannedAt = new Date(Date.now() + delayMs);
  logger.info(`[SIGNAL] ${symbol} ${direction} вхід заплановано на ${plannedAt.toTimeString().split(' ')[0]} (без Telegram-сповіщення — лише по факту відкриття)`);

  setTimeout(() => {
    executeTrade(symbol, direction, cfg).catch(error => {
      logger.error(`[TRADE] ${symbol} executeTrade fatal error: ${error.message}`);
      telegram.send(telegram.formatError(`executeTrade(${symbol})`, error)).catch(() => {});
    });
  }, Math.max(0, delayMs));
}

// ----------------------------------------------------------------------------
// ЗАХИСТ ПОЗИЦІЇ (TP/SL)
// ⚠️ Раніше тут була функція attachProtectionWithRetry() — окремий виклик
// stoporder/place ПІСЛЯ відкриття позиції, з 3 спробами. Прибрано: TP/SL
// тепер кріпиться АТОМАРНО одним запитом разом із входом (див.
// mexc.openMarketOrderWithProtection у executeTrade нижче) — окремий крок
// і, відповідно, retry-логіка для нього більше не потрібні.
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// АВАРІЙНЕ ЗАКРИТТЯ ПОЗИЦІЇ
// ⚠️ Більше НЕ викликається автоматично в основному потоці executeTrade().
// Раніше TP/SL ставився ОКРЕМИМ запитом ПІСЛЯ відкриття позиції — якщо він
// падав, позиція лишалась відкритою без захисту, і сюди приходили аварійно
// закривати. Тепер TP/SL кріпиться АТОМАРНО, одним запитом разом із самим
// входом (mexc.openMarketOrderWithProtection) — тому проміжного стану
// "позиція відкрита, TP/SL ще не поставлено" структурно більше не існує:
// або весь запит проходить (вхід + захист разом), або не проходить
// жодна частина (позиції взагалі немає, нема чого закривати).
// Лишаю функцію як утиліту на майбутнє (напр. для ручного аварійного
// скрипта), просто вона зараз нізвідки не викликається автоматично.
// ----------------------------------------------------------------------------
async function emergencyClosePosition({ mexcSymbol, direction, vol, entryPrice }) {
  logger.error(`[TRADE] ⚠️ КРИТИЧНО: TP/SL не встановлено для ${mexcSymbol} після кількох спроб — аварійно закриваю позицію по ринку`);
  try {
    // Беремо СВІЖУ ціну, а не ту, що була на момент входу (могла пройти
    // хвилина+ через 3 спроби TP/SL) — "price" тут діє як price-protection
    // межа для маркет-ордера, застаріле значення підвищує шанс відхилення.
    let closePrice = entryPrice;
    try {
      const freshTicker = await mexc.getTicker(mexcSymbol);
      closePrice = freshTicker.lastPrice;
    } catch (tickerError) {
      logger.warn(`[TRADE] Не вдалось отримати свіжу ціну для аварійного закриття, використовую ціну входу: ${tickerError.message}`);
    }

    await mexc.closePositionMarket({ symbol: mexcSymbol, direction, vol, price: closePrice });
    await telegram.send(
      `🚨 <b>АВАРІЙНЕ ЗАКРИТТЯ ПОЗИЦІЇ</b>\n\n` +
      `<b>${mexcSymbol} ${direction}</b>: не вдалось поставити TP/SL після кількох спроб — ` +
      `позицію закрито по ринку для безпеки, щоб не лишати її без захисту.\n\n` +
      `Перевір вручну в MEXC, що позиція дійсно закрита, і подивись логи — можливо, TP/SL стабільно падає з тією ж помилкою.`
    );
  } catch (closeError) {
    logger.error(`[TRADE] КРИТИЧНО: не вдалось навіть аварійно закрити позицію: ${closeError.message}`);
    await telegram.send(
      `🚨🚨 <b>КРИТИЧНА ПОМИЛКА — ПОЗИЦІЯ БЕЗ ЗАХИСТУ</b>\n\n` +
      `<b>${mexcSymbol} ${direction}</b>: TP/SL не встановлено, аварійне закриття ТЕЖ не вдалось ` +
      `(${closeError.message}).\n\n⚠️ ЗАЙДИ В MEXC ВРУЧНУ НЕГАЙНО І ЗАКРИЙ/ЗАХИСТИ ПОЗИЦІЮ!`
    );
  }
}

// ----------------------------------------------------------------------------
// Виконання угоди на MEXC на початку хвилини
// ----------------------------------------------------------------------------
async function executeTrade(symbol, direction, cfg, options = {}) {
  // Повторна валідація — стан міг змінитись за час очікування
  const reason = validateEntry(symbol, { skipTradingHours: options.isTest, skipOiFilter: options.isTest });
  if (reason) {
    daily.signalsSkipped++;
    logger.warn(`[TRADE] ${symbol} скасовано перед виконанням: ${reason}`);
    await telegram.send(telegram.formatSignalSkipped(symbol, direction, `(на момент виконання) ${reason}`));
    return;
  }

  const mexcSymbol = cfg.mexcSymbol;
  const oiCheckForLog = checkOiConfirmation(symbol);
  const oiLogStr = oiCheckForLog.changePercent !== null ? `${oiCheckForLog.changePercent >= 0 ? '+' : ''}${oiCheckForLog.changePercent.toFixed(2)}%` : 'н/д';
  logger.info(`[TRADE] ${options.isTest ? '[ТЕСТ] ' : ''}Виконую вхід: ${mexcSymbol} ${direction} | OI Δ=${oiLogStr}`);

  // Баланс і актуальна ціна (в DRY_RUN баланс — умовний, ціна — реальна з публічного API)
  const balance = config.trading.dryRun
    ? parseFloat(process.env.DRY_RUN_BALANCE || '1000')
    : await mexc.getUSDTBalance();

  if (!balance || balance <= 0) {
    throw new Error(`Нульовий або недоступний баланс USDT (${balance})`);
  }

  const contractInfo = await mexc.getContractDetail(mexcSymbol);
  const ticker = await mexc.getTicker(mexcSymbol);
  const entryPriceEstimate = ticker.lastPrice;

  const plan = riskService.calculatePositionParameters(balance, entryPriceEstimate, direction, contractInfo);

  if (config.trading.dryRun) {
    logger.info(`[DRY RUN] Відкрив би: ${mexcSymbol} ${direction} | ${plan.contracts} контрактів @ ~${plan.entryPrice}`);
    positionService.addOpenPosition({
      symbol,
      mexcSymbol,
      direction,
      entryPrice: plan.entryPrice,
      contracts: plan.contracts,
      contractSize: contractInfo.contractSize,
      leverage: plan.leverage,
      requiredMargin: plan.requiredMargin,
      riskAmount: plan.riskAmount,
      stopLossPrice: plan.stopLossPrice,
      stopLossOrderPrice: plan.stopLossOrderPrice,
      takeProfitPrice: plan.takeProfitPrice,
      takeProfitOrderPrice: plan.takeProfitOrderPrice,
      oiChangePercent: oiCheckForLog.changePercent,
      positionId: `DRY_RUN_${Date.now()}`
    });
    await telegram.send(telegram.formatPositionOpened({ ...plan, symbol: mexcSymbol, oiChangePercent: oiCheckForLog.changePercent }));
    daily.tradesOpened++;
    return;
  }

  // ---- РЕАЛЬНА ТОРГІВЛЯ ----
  const positionType = direction === 'LONG' ? 1 : 2; // 1 long, 2 short
  const side = direction === 'LONG' ? 1 : 3;          // 1 open long, 3 open short

  await mexc.setLeverage({
    symbol: mexcSymbol,
    leverage: config.risk.leverage,
    openType: config.trading.openType,
    positionType
  });

  // Вхід і TP/SL — ОДНИМ атомарним запитом (plan.stopLossPrice/plan.takeProfitPrice
  // вже пораховані riskService.calculatePositionParameters() вище під
  // orientovnu ціну входу). Проміжного стану "позиція відкрита без
  // захисту" тут структурно не існує — або проходить все разом, або
  // нічого не відкривається взагалі.
  const order = await mexc.openMarketOrderWithProtection({
    symbol: mexcSymbol,
    side,
    vol: plan.contracts,
    leverage: config.risk.leverage,
    openType: config.trading.openType,
    price: entryPriceEstimate,
    positionMode: config.trading.positionMode,
    stopLossPrice: plan.stopLossPrice,
    takeProfitPrice: plan.takeProfitPrice
  });

  const filled = await waitForFill(order.orderId);
  if (!filled || parseFloat(filled.dealVol) <= 0) {
    throw new Error(`Ринковий ордер ${order.orderId} не заповнився (можливо спрацював price-protection MEXC)`);
  }

  const actualEntryPrice = parseFloat(filled.dealAvgPrice);
  const actualVol = parseFloat(filled.dealVol);
  const positionId = filled.positionId;

  const openedPosition = {
    symbol,
    mexcSymbol,
    positionId,
    direction,
    entryPrice: actualEntryPrice,
    contracts: actualVol,
    contractSize: contractInfo.contractSize,
    leverage: config.risk.leverage,
    requiredMargin: plan.requiredMargin,
    riskAmount: plan.riskAmount,
    stopLossPrice: plan.stopLossPrice,
    takeProfitPrice: plan.takeProfitPrice,
    oiChangePercent: oiCheckForLog.changePercent
  };

  positionService.addOpenPosition(openedPosition);
  daily.tradesOpened++;

  await telegram.send(telegram.formatPositionOpened({ ...openedPosition, symbol: mexcSymbol })).catch(err => {
    logger.error(`[TELEGRAM] ${err.message}`);
  });

  logger.info(`[TRADE] ✅ ${mexcSymbol} ${direction} відкрито @ ${actualEntryPrice} з прикріпленим TP/SL, positionId=${positionId}`);
  logger.warn(`[TRADE] ⚠️ Перший тест нового атомарного підходу — перевір вручну в MEXC, що TP/SL справді відображаються на позиції.`);
}

// Полінг статусу ордера доки не заповниться (ринкові ордери заповнюються майже миттєво)
async function waitForFill(orderId, attempts = 10, intervalMs = 300) {
  for (let i = 0; i < attempts; i++) {
    const order = await mexc.getOrder(orderId);
    if (order && parseFloat(order.dealVol) > 0 && (order.state === 3 || parseFloat(order.dealVol) >= parseFloat(order.vol))) {
      return order;
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }
  // остання спроба повернути що є (частково заповнений ордер краще, ніж нічого)
  return mexc.getOrder(orderId);
}

// ----------------------------------------------------------------------------
// ДЕННА СТАТИСТИКА — надсилається о завершенні робочого вікна (endHour UTC).
// Якщо на той момент ще є відкриті позиції — чекаємо, поки закриються ВСІ
// (positionService повідомить через onAllClosedCallback), і тільки тоді
// надсилаємо підсумок. У DRY_RUN працює завдяки симульованому закриттю
// позицій (див. position.service.js) — статистика буде умовною
// (без реальних комісій/сліпеджу), про що прямо написано в повідомленні.
// ----------------------------------------------------------------------------
let statsPending = false;

async function sendDailyStats() {
  statsPending = false;
  const stats = positionService.getDailyStats();
  logger.info(`[STATS] Підсумок дня: ${stats.total} угод, ${stats.wins}W/${stats.losses}L, PnL=${stats.pnl.toFixed(2)}`);
  await telegram.send(telegram.formatDailyStats(stats));
}

function triggerDailyStatsCheck() {
  if (positionService.getOpenPositionsCount() === 0) {
    sendDailyStats().catch(err => logger.error(`[STATS] Помилка надсилання: ${err.message}`));
  } else {
    statsPending = true;
    logger.info(`[STATS] Робоче вікно завершилось, але ще ${positionService.getOpenPositionsCount()} відкритих позицій — чекаю закриття перед відправкою підсумку`);
  }
}

function scheduleDailyStats() {
  const delay = msUntilNextUtcTime(config.tradingHours.endHour, 0);
  logger.info(`[STATS] Наступний підсумок дня заплановано через ${Math.round(delay / 60000)} хв (${config.tradingHours.endHour}:00 UTC)`);
  setTimeout(() => {
    triggerDailyStatsCheck();
    scheduleDailyStats(); // на наступну добу
  }, delay);
}

// Коли всі позиції закриті (і десь очікується статистика) — надсилаємо одразу,
// не чекаючи наступного дня.
positionService.setOnAllClosedCallback(() => {
  if (statsPending) {
    sendDailyStats().catch(err => logger.error(`[STATS] Помилка надсилання: ${err.message}`));
  }
});

// ----------------------------------------------------------------------------
// ТЕСТОВА УГОДА ПРИ СТАРТІ (опційно, ENABLE_STARTUP_TEST_TRADE=true)
// Запускає ТОЙ САМИЙ шлях виконання, що й реальний сигнал (ризик-менеджмент,
// вхід по ринку, спроби TP/SL, аварійне закриття) — щоб відразу побачити,
// чи все проходить без помилок, не чекаючи природного сигналу.
// ----------------------------------------------------------------------------
async function runStartupTestTrade() {
  const symbol = config.testTrade.symbol;
  const direction = config.testTrade.direction;
  const cfg = config.getSymbolConfig(symbol);

  if (!cfg || !cfg.enabled) {
    logger.error(`[TEST] Символ ${symbol} для тестової угоди не знайдено/вимкнено в конфізі — пропускаю тест`);
    return;
  }

  if (config.trading.dryRun) {
    logger.warn('[TEST] ENABLE_STARTUP_TEST_TRADE=true, але DRY_RUN=true — placeTpSl НЕ буде реально викликано, тест нічого не скаже про роботу TP/SL на біржі.');
  }

  logger.warn(`[TEST] 🧪 ЗАПУСК ТЕСТОВОЇ УГОДИ: ${symbol} ${direction}`);
  await telegram.send(
    `🧪 <b>ТЕСТОВА УГОДА (ENABLE_STARTUP_TEST_TRADE=true)</b>\n\n` +
    `Відкриваю ${symbol} ${direction}, щоб перевірити вхід/TP/SL/аварійне закриття без очікування реального сигналу.\n\n` +
    `⚠️ Не забудь вимкнути ENABLE_STARTUP_TEST_TRADE після перевірки — інакше це повторюватиметься на кожному рестарті бота.`
  );

  try {
    await executeTrade(symbol, direction, cfg, { isTest: true });
    logger.info('[TEST] ✅ Тестова угода відпрацювала без фатальних помилок — перевір Telegram/MEXC, чи справді встановлено TP/SL (чи спрацювало аварійне закриття, якщо ні).');
  } catch (error) {
    logger.error(`[TEST] ❌ Тестова угода впала з помилкою: ${error.message}`);
    await telegram.send(`❌ <b>Тестова угода впала з помилкою:</b> ${error.message}`);
  }
}

// ----------------------------------------------------------------------------
// STARTUP
// ----------------------------------------------------------------------------
async function start() {
  const symbols = config.getEnabledSymbols();

  console.log('='.repeat(70));
  console.log('BINANCE FLOW MONITOR + MEXC FUTURES AUTO-EXECUTION');
  console.log(`Build: ${BOT_BUILD}`);
  console.log('='.repeat(70));
  console.log(`Символів: ${symbols.length} | Вікно: ${config.WINDOW_SECONDS}с`);
  console.log(`Ризик: ${config.risk.percentOfDeposit}% депозиту | Плече: ${config.risk.leverage}x`);
  console.log(`TP: +${config.risk.takeProfitPercent}% | SL: -${config.risk.stopLossPercent}% (рух ціни)`);
  console.log(`Вхід на початку наступної хвилини: ${config.execution.entryAtNextMinute}`);
  console.log(`Робочі години: ${config.tradingHours.enabled ? `${config.tradingHours.startHour}:00–${config.tradingHours.endHour}:00 UTC` : 'вимкнено (24/7)'}`);
  console.log(`DRY RUN: ${config.trading.dryRun ? 'УВІМКНЕНО (реальні ордери НЕ відправляються)' : 'вимкнено — ЖИВА ТОРГІВЛЯ'}`);
  console.log(`Моніторинг позицій: ${config.trading.dryRun ? `симуляція по тікеру кожні ${config.monitoring.dryRunIntervalMs / 1000}с` : `WS push (миттєво) + REST-страховка кожні ${config.monitoring.liveFallbackIntervalMs / 1000}с`}`);
  console.log(`Тестова угода при старті: ${config.testTrade.enabled ? `🧪 УВІМКНЕНО (${config.testTrade.symbol} ${config.testTrade.direction}) — вимкни після перевірки!` : 'вимкнено'}`);
  console.log(`OI-фільтр: ${config.oiFilter.enabled ? `увімкнено (блок якщо OI Δ > +${config.oiFilter.maxIncreasePercent}%)` : 'вимкнено'}`);
  console.log(`BTC circuit breaker: ${config.btcCircuitBreaker.enabled ? `увімкнено (|ΔBTC|≥${config.btcCircuitBreaker.minMovePercent}% + OI≥+${config.btcCircuitBreaker.minOiIncreasePercent}% -> стоп ${config.btcCircuitBreaker.haltDurationMs/3600000}год)` : 'вимкнено'}`);
  console.log(`Loss streak circuit breaker: ${config.lossStreakCircuitBreaker.enabled ? `увімкнено (${config.lossStreakCircuitBreaker.maxConsecutiveLosses} SL поспіль -> стоп ${config.lossStreakCircuitBreaker.firstHaltHours}год, 2-й раз/день -> до завтра)` : 'вимкнено'}`);
  console.log('='.repeat(70));

  try {
    await mexc.connect();
  } catch (error) {
    logger.error(`[MEXC] Не вдалось підключитись: ${error.message}`);
    if (!config.trading.dryRun) process.exit(1);
  }

  try {
    await telegram.send(
      `🚀 <b>Бот запущено</b>\n\n` +
      `Build: <code>${BOT_BUILD}</code>\n` +
      `Символів: ${symbols.length}\n` +
      `Ризик: ${config.risk.percentOfDeposit}% / Плече: ${config.risk.leverage}x\n` +
      `TP +${config.risk.takeProfitPercent}% / SL -${config.risk.stopLossPercent}%\n` +
      `Робочі години: ${config.tradingHours.enabled ? `${config.tradingHours.startHour}:00–${config.tradingHours.endHour}:00 UTC` : '24/7'}\n` +
      `Режим: ${config.trading.dryRun ? '🧪 DRY RUN' : '🔴 LIVE'}`
    );
  } catch (error) {
    logger.error(`[TELEGRAM] ${error.message}`);
  }

  tradeAggregator = new TradeAggregator(config.WINDOW_SECONDS);
  const signalEngine = new SignalEngine();
  const cooldownManager = new CooldownManager();

  // BTC додається до WS-підписки лише для моніторингу (немає в SYMBOL_CONFIGS,
  // тому shouldAlert/interpretSignal завжди повертають false для нього —
  // BTC ніколи не торгується сам, тільки впливає на circuit breaker).
  const wsSymbols = config.btcCircuitBreaker.enabled && !symbols.includes(config.btcCircuitBreaker.monitorSymbol)
    ? [...symbols, config.btcCircuitBreaker.monitorSymbol]
    : symbols;

  const wsManager = new MultiWebSocketManager(
    wsSymbols, tradeAggregator, signalEngine, cooldownManager, handleSignal,
    () => isWithinTradingHours(config.tradingHours)
  );
  wsManager.connectAll();

  if (config.oiFilter.enabled || config.btcCircuitBreaker.enabled) {
    openInterestTracker.start();
  }

  let btcCheckInterval = null;
  if (config.btcCircuitBreaker.enabled) {
    btcCheckInterval = setInterval(() => {
      try { checkBtcCrashCircuitBreaker(); } catch (err) {
        logger.error(`[CIRCUIT BREAKER] BTC check error: ${err.message}`);
      }
    }, config.btcCircuitBreaker.checkIntervalMs);
  }

  // Якщо не вдалось авторизуватись у приватному WS-стрімі MEXC (наприклад,
  // ключ без прав на futures) — це критично для live-режиму: без нього
  // залишиться лише рідкісна REST-страховка як єдиний спосіб дізнатись про
  // закриття позиції, тому явно попереджаємо в Telegram.
  mexcUserStream.setOnAuthFailed(() => {
    telegram.send(
      `⚠️ <b>Не вдалось авторизуватись у приватному WS-стрімі MEXC</b>\n\n` +
      `Закриття позицій відстежуватимуться лише через рідкісну REST-страховку ` +
      `(раз на ${Math.round(config.monitoring.liveFallbackIntervalMs / 1000)}с) — з затримкою. ` +
      `Перевір права API-ключа (Futures Trading + KYC).`
    ).catch(() => {});
  });

  // Моніторинг відкритих позицій працює 24/7 незалежно від робочих годин —
  // реальну (чи симульовану) відкриту позицію не можна "заморозити" на ніч,
  // її треба безпечно довести до TP/SL і закрити.
  positionService.startMonitoring(config.monitoring.dryRunIntervalMs, config.monitoring.liveFallbackIntervalMs);

  scheduleDailyStats();

  if (config.testTrade.enabled) {
    // невелика затримка, щоб WS/MEXC-з'єднання встигли стабілізуватись
    // перед першим реальним запитом на біржу
    setTimeout(() => {
      runStartupTestTrade().catch(err => logger.error(`[TEST] Fatal: ${err.message}`));
    }, 5000);
  }

  const shutdown = async () => {
    logger.info('[SHUTDOWN] Зупинка...');
    wsManager.closeAll();
    openInterestTracker.stop();
    if (btcCheckInterval) clearInterval(btcCheckInterval);
    positionService.stopMonitoring();
    try {
      await telegram.send(`⛔ Бот зупинено\n\nВідкритих позицій: ${positionService.getOpenPositionsCount()}\nУгод сьогодні: ${daily.tradesOpened}`);
    } catch (_) {}
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  start().catch(error => {
    logger.error(`[FATAL] ${error.message}`);
    process.exit(1);
  });
}

module.exports = {
  start, handleSignal, executeTrade,
  // Внутрішні речі, виставлені лише для тестування — не впливають на
  // публічний API/поведінку продакшена.
  _internal: {
    validateEntry,
    isTradingHalted,
    checkBtcCrashCircuitBreaker,
    circuitBreaker,
    setTradeAggregatorForTest: (ta) => { tradeAggregator = ta; }
  }
};
