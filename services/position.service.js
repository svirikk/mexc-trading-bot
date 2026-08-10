const mexc = require('./mexc.service');
const mexcUserStream = require('./mexc-user-stream.service');
const telegram = require('./telegram.service');
const config = require('../config/settings');
const logger = require('../utils/logger');
const { formatDuration, getCurrentDate } = require('../utils/helpers');

class PositionService {
  constructor() {
    this.openPositions = new Map(); // symbol -> tracked position data
    this.closedToday = [];
    this.monitorInterval = null;
    this.lastResetDate = getCurrentDate();
    this.onAllClosedCallback = null;
  }

  hasOpenPosition(symbol) {
    return this.openPositions.has(symbol);
  }

  getOpenPositionsCount() {
    return this.openPositions.size;
  }

  // Викликається щоразу, коли кількість відкритих позицій доходить до 0
  // (використовується для відкладеної денної статистики — почекати,
  // поки всі позиції закриються, і тільки тоді надіслати підсумок).
  setOnAllClosedCallback(fn) {
    this.onAllClosedCallback = fn;
  }

  addOpenPosition(data) {
    this.openPositions.set(data.symbol, { ...data, openedAt: Date.now() });
    logger.info(`[POSITION] Додано в моніторинг: ${data.symbol} ${data.direction}`);
  }

  removeOpenPosition(symbol) {
    const p = this.openPositions.get(symbol);
    this.openPositions.delete(symbol);
    if (this.openPositions.size === 0 && this.onAllClosedCallback) {
      try { this.onAllClosedCallback(); } catch (err) {
        logger.error(`[POSITION] onAllClosedCallback error: ${err.message}`);
      }
    }
    return p;
  }

  // ---------------------------------------------------------------------
  // СТАРТ МОНІТОРИНГУ
  // DRY_RUN: немає реального акаунта, що торгує -> симулюємо закриття
  //          порівнянням свіжого тікера з TP/SL, з опитуванням раз на
  //          dryRunIntervalMs (за замовч. 20с).
  // LIVE:    ОСНОВНИЙ шлях — приватний user-data WebSocket MEXC
  //          (push.personal.position), який штовхає закриття миттєво,
  //          без жодного REST-запиту. REST-полінг лишається лише як рідкісна
  //          СТРАХОВКА (liveFallbackIntervalMs, за замовч. 60с) на випадок,
  //          якщо WS пропустить подію або довго перепідключається.
  // ---------------------------------------------------------------------
  startMonitoring(dryRunIntervalMs = 20000, liveFallbackIntervalMs = 60000) {
    if (this.monitorInterval) return;

    if (config.trading.dryRun) {
      logger.info(`[POSITION] DRY RUN: симуляція закриття по тікеру кожні ${dryRunIntervalMs}мс`);
      this.monitorInterval = setInterval(() => this.checkPositions().catch(err => {
        logger.error(`[POSITION] Monitor loop error: ${err.message}`);
      }), dryRunIntervalMs);
      return;
    }

    logger.info(`[POSITION] LIVE: підписуюсь на push-оновлення позицій (WS user-data), REST-страховка кожні ${liveFallbackIntervalMs}мс`);
    mexcUserStream.setOnPositionUpdate((data) => this.handlePositionPush(data));
    mexcUserStream.connect();

    this.monitorInterval = setInterval(() => this.checkPositionsRestFallback().catch(err => {
      logger.error(`[POSITION] Fallback poll error: ${err.message}`);
    }), liveFallbackIntervalMs);
  }

  stopMonitoring() {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
    mexcUserStream.close();
  }

  // ---------------------------------------------------------------------
  // LIVE, ОСНОВНИЙ ШЛЯХ: push.personal.position прилітає миттєво, як тільки
  // TP/SL спрацював на біржі. state: 1 holding, 2 system custody, 3 closed.
  // closeAvgPrice/closeProfitLoss — реальні цифри від біржі, рахувати нічого
  // не треба.
  // ---------------------------------------------------------------------
  async handlePositionPush(data) {
    try {
      let matched = null;
      let matchedSymbol = null;
      for (const [symbol, tracked] of this.openPositions.entries()) {
        if (String(tracked.positionId) === String(data.positionId)) {
          matched = tracked;
          matchedSymbol = symbol;
          break;
        }
      }
      if (!matched) return; // не наша позиція (або вже оброблена)

      if (data.state === 3) {
        logger.info(`[POSITION][WS] ${matchedSymbol} закрито (push, миттєво)`);
        await this.finalizeClosedPosition(matchedSymbol, matched, {
          exitPrice: parseFloat(data.closeAvgPrice),
          pnl: parseFloat(data.closeProfitLoss ?? data.realised ?? 0)
        });
      }
    } catch (error) {
      logger.error(`[POSITION] Помилка обробки WS push: ${error.message}`);
    }
  }

  // ---------------------------------------------------------------------
  // LIVE, СТРАХОВКА: те саме, що раніше робилось кожні 20с, але тепер
  // рідко і лише як резерв на випадок збою WS-стріму.
  // ---------------------------------------------------------------------
  async checkPositionsRestFallback() {
    this.maybeResetDaily();
    if (this.openPositions.size === 0) return;

    for (const [symbol, tracked] of this.openPositions.entries()) {
      try {
        const positions = await mexc.getOpenPositions(tracked.mexcSymbol);
        const still = positions.find(p => p.positionId === tracked.positionId);

        if (!still || parseFloat(still.holdVol) === 0) {
          logger.warn(`[POSITION] ${symbol} закрито, але WS-подія не прийшла вчасно — підхоплюю через REST-страховку`);
          await this.handleClosedViaRest(symbol, tracked);
        }
      } catch (error) {
        logger.error(`[POSITION] Fallback перевірка ${symbol}: ${error.message}`);
      }
    }
  }

  async handleClosedViaRest(symbol, tracked) {
    let exitPrice = null;
    let pnl = null;

    try {
      const history = await mexc.getHistoricalPositions({ symbol: tracked.mexcSymbol, pageSize: 5 });
      const match = Array.isArray(history) ? history.find(h => h.positionId === tracked.positionId) : null;
      if (match) {
        exitPrice = parseFloat(match.closeAvgPrice);
        pnl = parseFloat(match.closeProfitLoss ?? match.realised ?? 0);
      }
    } catch (e) {
      logger.warn(`[POSITION] Не вдалось отримати історію для ${symbol}: ${e.message}`);
    }

    await this.finalizeClosedPosition(symbol, tracked, { exitPrice, pnl: pnl ?? 0 });
  }

  // ---------------------------------------------------------------------
  // DRY RUN: реальної позиції на біржі немає, тому "закриття" симулюється
  // порівнянням свіжої ринкової ціни (публічний тікер, працює без ключів)
  // з розрахованими TP/SL. Це ВАЖЛИВО, а не косметика: без цього симуляція
  // ніколи не звільняла б слот у openPositions, і після 2-3 сигналів бот
  // почав би ігнорувати геть усі наступні через ліміт MAX_OPEN_POSITIONS.
  // ---------------------------------------------------------------------
  async checkPositions() {
    this.maybeResetDaily();
    if (this.openPositions.size === 0) return;

    for (const [symbol, tracked] of this.openPositions.entries()) {
      try {
        await this.checkDryRunPosition(symbol, tracked);
      } catch (error) {
        logger.error(`[POSITION] Помилка перевірки ${symbol}: ${error.message}`);
      }
    }
  }

  async checkDryRunPosition(symbol, tracked) {
    const ticker = await mexc.getTicker(tracked.mexcSymbol);
    const price = ticker.lastPrice;

    let hit = null;
    if (tracked.direction === 'LONG') {
      if (price >= tracked.takeProfitPrice) hit = { type: 'TP', exitPrice: tracked.takeProfitOrderPrice ?? tracked.takeProfitPrice };
      else if (price <= tracked.stopLossPrice) hit = { type: 'SL', exitPrice: tracked.stopLossOrderPrice ?? tracked.stopLossPrice };
    } else {
      if (price <= tracked.takeProfitPrice) hit = { type: 'TP', exitPrice: tracked.takeProfitOrderPrice ?? tracked.takeProfitPrice };
      else if (price >= tracked.stopLossPrice) hit = { type: 'SL', exitPrice: tracked.stopLossOrderPrice ?? tracked.stopLossPrice };
    }

    if (!hit) return; // ще триває

    const contractSize = tracked.contractSize || 1;
    const priceDiff = tracked.direction === 'LONG'
      ? (hit.exitPrice - tracked.entryPrice)
      : (tracked.entryPrice - hit.exitPrice);
    const pnl = priceDiff * tracked.contracts * contractSize;

    await this.finalizeClosedPosition(symbol, tracked, {
      exitPrice: hit.exitPrice,
      pnl,
      hitType: hit.type,
      simulated: true
    });
  }

  // ---------------------------------------------------------------------
  // Спільна фіналізація закриття незалежно від джерела (WS push /
  // REST-страховка / DRY_RUN симуляція) — один шлях запису статистики й
  // сповіщення, щоб не дублювати логіку в трьох місцях.
  // ---------------------------------------------------------------------
  async finalizeClosedPosition(symbol, tracked, { exitPrice, pnl, hitType = null, simulated = false }) {
    const durationSec = Math.floor((Date.now() - tracked.openedAt) / 1000);

    const closedData = {
      ...tracked,
      exitPrice,
      pnl: pnl ?? 0,
      hitType,
      simulated,
      duration: formatDuration(durationSec)
    };

    this.closedToday.push(closedData);
    this.removeOpenPosition(symbol);

    await telegram.send(telegram.formatPositionClosed(closedData));
    logger.info(`[POSITION] Закрито: ${symbol} | PnL: ${closedData.pnl} | ${closedData.duration}${simulated ? ' [DRY RUN]' : ''}`);
  }

  maybeResetDaily() {
    const today = getCurrentDate();
    if (today !== this.lastResetDate) {
      this.lastResetDate = today;
      this.closedToday = [];
    }
  }

  getDailyStats() {
    const total = this.closedToday.length;
    const wins = this.closedToday.filter(p => p.pnl >= 0).length;
    const pnl = this.closedToday.reduce((s, p) => s + p.pnl, 0);
    return { total, wins, losses: total - wins, pnl };
  }
}

module.exports = new PositionService();
