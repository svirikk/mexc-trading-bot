if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// ============================================================================
// INDIVIDUAL SYMBOL FILTERS (перенесено 1:1 з твого index.js)
// MEXC symbol format використовує підкреслення: ADA_USDT замість ADAUSDT.
// ============================================================================
const SYMBOL_CONFIGS = {
  ADAUSDT: {
    mexcSymbol: 'ADA_USDT',
    minVolumeUSD: 1_000_000,
    minDominance: 65.0,
    minPriceChange: 0.5,
    cooldownMinutes: 5,
    enabled: true
  },
  TAOUSDT: {
    mexcSymbol: 'TAO_USDT',
    minVolumeUSD: 1_500_000,
    minDominance: 70.0,
    minPriceChange: 0.6,
    cooldownMinutes: 5,
    enabled: true
  },
  HYPEUSDT: {
    mexcSymbol: 'HYPE_USDT',
    minVolumeUSD: 2_000_000,
    minDominance: 70.0,
    minPriceChange: 1,
    cooldownMinutes: 5,
    enabled: true
  },
  PEPEUSDT: {
    mexcSymbol: 'PEPE_USDT',
    minVolumeUSD: 1_000_000,
    minDominance: 65.0,
    minPriceChange: 0.6,
    cooldownMinutes: 5,
    enabled: true
  },
  WIFUSDT: {
    mexcSymbol: 'WIF_USDT',
    minVolumeUSD: 1_500_000,
    minDominance: 65.0,
    minPriceChange: 0.5,
    cooldownMinutes: 5,
    enabled: true
  },
  BONKUSDT: {
    mexcSymbol: 'BONK_USDT',
    minVolumeUSD: 1_000_000,
    minDominance: 65.0,
    minPriceChange: 0.5,
    cooldownMinutes: 5,
    enabled: true
  },
  DOGEUSDT: {
    mexcSymbol: 'DOGE_USDT',
    minVolumeUSD: 5_000_000,
    minDominance: 70.0,
    minPriceChange: 0.75,
    cooldownMinutes: 5,
    enabled: true
  },
  XRPUSDT: {
    mexcSymbol: 'XRP_USDT',
    minVolumeUSD: 5_000_000,
    minDominance: 70.0,
    minPriceChange: 1,
    cooldownMinutes: 5,
    enabled: true
  },
  UNIUSDT: {
    mexcSymbol: 'UNI_USDT',
    minVolumeUSD: 1_000_000,
    minDominance: 65.0,
    minPriceChange: 0.5,
    cooldownMinutes: 5,
    enabled: true
  }
};

function requireEnvForLive() {
  if (process.env.DRY_RUN === 'true') return; // у dry-run ключі не обов'язкові
  const required = ['MEXC_API_KEY', 'MEXC_API_SECRET', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'];
  const missing = required.filter(k => !process.env[k]);
  if (missing.length) {
    throw new Error(`Відсутні обов'язкові змінні оточення: ${missing.join(', ')}`);
  }
}
requireEnvForLive();

const config = {
  SYMBOL_CONFIGS,

  getEnabledSymbols() {
    return Object.keys(SYMBOL_CONFIGS).filter(s => SYMBOL_CONFIGS[s].enabled);
  },

  getSymbolConfig(symbol) {
    return SYMBOL_CONFIGS[symbol] || null;
  },

  WINDOW_SECONDS: parseInt(process.env.WINDOW_SECONDS) || 180,
  STATS_LOG_INTERVAL: parseInt(process.env.STATS_LOG_INTERVAL) || 60,
  MAX_RECONNECTS: parseInt(process.env.MAX_RECONNECTS) || 10,

  BINANCE_WS: 'wss://fstream.binance.com/market', // до 2026-04-23 був /ws — легасі більше не працює для market-стрімів (aggTrade)
  BINANCE_REST: 'https://fapi.binance.com',

  telegram: {
    token: process.env.TELEGRAM_BOT_TOKEN,
    chatId: process.env.TELEGRAM_CHAT_ID
  },

  mexc: {
    apiKey: process.env.MEXC_API_KEY,
    apiSecret: process.env.MEXC_API_SECRET,
    baseURL: process.env.MEXC_BASE_URL || 'https://api.mexc.com'
  },

  risk: {
    percentOfDeposit: parseFloat(process.env.RISK_PERCENT_OF_DEPOSIT || '1'),
    leverage: parseInt(process.env.LEVERAGE || '20'),
    takeProfitPercent: parseFloat(process.env.TAKE_PROFIT_PERCENT || '0.8'),
    stopLossPercent: parseFloat(process.env.STOP_LOSS_PERCENT || '0.5'),
    slFillBufferPercent: parseFloat(process.env.SL_FILL_BUFFER_PERCENT || '0.03'),
    tpFillBufferPercent: parseFloat(process.env.TP_FILL_BUFFER_PERCENT || '0.03')
  },

  trading: {
    openType: parseInt(process.env.OPEN_TYPE || '1'), // 1 isolated, 2 cross
    positionMode: parseInt(process.env.POSITION_MODE || '1'), // 1 hedge, 2 one-way
    maxOpenPositions: parseInt(process.env.MAX_OPEN_POSITIONS || '3'),
    maxDailyTrades: parseInt(process.env.MAX_DAILY_TRADES || '20'),
    dryRun: process.env.DRY_RUN === 'true'
  },

  execution: {
    entryAtNextMinute: process.env.ENTRY_AT_NEXT_MINUTE !== 'false',
    entryBufferMs: parseInt(process.env.ENTRY_BUFFER_MS || '350')
  },

  tradingHours: {
    enabled: process.env.ENABLE_TRADING_HOURS !== 'false',
    startHour: parseInt(process.env.TRADING_HOURS_START_UTC || '2'),
    endHour: parseInt(process.env.TRADING_HOURS_END_UTC || '14')
  },

  monitoring: {
    // DRY_RUN: як часто перевіряти тікер для симуляції TP/SL
    dryRunIntervalMs: parseInt(process.env.DRY_RUN_MONITOR_INTERVAL_MS || '20000'),
    // LIVE: основний шлях — WS push (миттєво). Це лише РІДКІСНА страховка
    // REST-запитом на випадок, якщо WS пропустить подію чи довго перепідключається.
    liveFallbackIntervalMs: parseInt(process.env.LIVE_FALLBACK_POLL_MS || '60000')
  },

  // ТЕСТОВА УГОДА ПРИ СТАРТІ — щоб одразу перевірити вхід/TP/SL/аварійне
  // закриття, не чекаючи реального сигналу. За замовчуванням ВИМКНЕНО.
  // ⚠️ Не залишай увімкненим надовго: якщо процес рестартує (напр. crash
  // loop у контейнері), тестова угода відкриватиметься ЗНОВУ на кожному старті.
  testTrade: {
    enabled: process.env.ENABLE_STARTUP_TEST_TRADE === 'true',
    symbol: (process.env.TEST_TRADE_SYMBOL || 'ADAUSDT').toUpperCase(),
    direction: ['LONG', 'SHORT'].includes((process.env.TEST_TRADE_DIRECTION || '').toUpperCase())
      ? process.env.TEST_TRADE_DIRECTION.toUpperCase()
      : 'LONG'
  },

  // OI-ФІЛЬТР ПІДТВЕРДЖЕННЯ: блокує вхід, якщо Open Interest ще помітно
  // росте (свіжі позиції відкриваються в напрямку руху -> рух ще не факт
  // що видихся), дозволяє вхід, якщо OI плаский/падає (позиції
  // закриваються -> ознака виснаження). Симетрично для LONG і SHORT.
  oiFilter: {
    enabled: process.env.ENABLE_OI_FILTER !== 'false',
    // Максимальний дозволений приріст OI за вікно (%), вище якого вхід блокується
    maxIncreasePercent: parseFloat(process.env.OI_FILTER_MAX_INCREASE_PERCENT || '0.5'),
    pollIntervalMs: parseInt(process.env.OI_POLL_INTERVAL_MS || '15000'),
    // Що робити, якщо даних ще замало (бот щойно стартував):
    // true = пропустити вхід (не блокувати), false = заблокувати про всяк випадок
    passOnInsufficientData: process.env.OI_FILTER_PASS_ON_INSUFFICIENT_DATA === 'true'
  },

  // BTC CIRCUIT BREAKER: якщо BTC різко впав (чи зріс) і Open Interest по
  // ньому одночасно росте (свіжі позиції відкриваються -> рух ще має
  // "паливо", схоже на каскад/маніпуляцію) — зупиняємо ВСІ нові входи по
  // ВСІХ символах на певний час, і примусово закриваємо позиції, відкриті
  // зовсім недавно (щоб не "пролитися" разом з ринком).
  btcCircuitBreaker: {
    enabled: process.env.ENABLE_BTC_CIRCUIT_BREAKER !== 'false',
    monitorSymbol: 'BTCUSDT',
    // Поріг "жорсткого руху" BTC за вікно WINDOW_SECONDS, у % (додатне число)
    minMovePercent: parseFloat(process.env.BTC_CRASH_MIN_MOVE_PERCENT || '1.0'),
    // Мінімальний приріст OI по BTC за те саме вікно, що підтверджує
    // "рух ще має паливо" (не просто закриття позицій)
    minOiIncreasePercent: parseFloat(process.env.BTC_CRASH_MIN_OI_INCREASE_PERCENT || '0.5'),
    haltDurationMs: parseFloat(process.env.BTC_CRASH_HALT_HOURS || '1') * 60 * 60 * 1000,
    // Позиції, відкриті менше ніж стільки хвилин тому на момент спрацювання,
    // будуть примусово закриті по ринку
    forceCloseMaxAgeMinutes: parseFloat(process.env.BTC_CRASH_FORCE_CLOSE_MAX_AGE_MINUTES || '3'),
    checkIntervalMs: parseInt(process.env.BTC_CRASH_CHECK_INTERVAL_MS || '5000')
  },

  // LOSS STREAK CIRCUIT BREAKER: N збиткових угод поспіль (по всіх символах
  // разом, без урахування напрямку) -> зупинка торгів. Лічильник скидається
  // на 0 при будь-якій прибутковій угоді. Друге спрацювання за той самий
  // календарний день -> зупинка до початку наступного робочого вікна.
  lossStreakCircuitBreaker: {
    enabled: process.env.ENABLE_LOSS_STREAK_CIRCUIT_BREAKER !== 'false',
    maxConsecutiveLosses: parseInt(process.env.LOSS_STREAK_MAX_CONSECUTIVE || '3'),
    firstHaltHours: parseFloat(process.env.LOSS_STREAK_FIRST_HALT_HOURS || '2'),
    maxOccurrencesPerDay: parseInt(process.env.LOSS_STREAK_MAX_PER_DAY || '2')
  }
};


// ---- validation ----
if (config.risk.percentOfDeposit <= 0 || config.risk.percentOfDeposit > 100) {
  throw new Error('RISK_PERCENT_OF_DEPOSIT must be between 0 and 100');
}
if (config.risk.leverage <= 0 || config.risk.leverage > 125) {
  throw new Error('LEVERAGE must be between 1 and 125');
}
if (config.trading.maxOpenPositions <= 0) {
  throw new Error('MAX_OPEN_POSITIONS must be greater than 0');
}
if (config.tradingHours.enabled && (config.tradingHours.startHour < 0 || config.tradingHours.startHour > 23 || config.tradingHours.endHour < 0 || config.tradingHours.endHour > 23)) {
  throw new Error('TRADING_HOURS_START_UTC / TRADING_HOURS_END_UTC must be between 0 and 23');
}
if (config.testTrade.enabled && !SYMBOL_CONFIGS[config.testTrade.symbol]) {
  throw new Error(`TEST_TRADE_SYMBOL=${config.testTrade.symbol} не знайдено серед SYMBOL_CONFIGS`);
}

module.exports = config;
