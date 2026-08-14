const axios = require('axios');
const config = require('../config/settings');
const logger = require('../utils/logger');
const { sleep } = require('../utils/helpers');

// ============================================================================
// OPEN INTEREST TRACKER — фільтр-підтвердження для входу.
//
// ІДЕЯ (по суті класичний "price + OI" аналіз):
//   Ціна вгору + OI вгору  -> відкриваються НОВІ лонги -> рух ще має "паливо",
//                             ризиковано фейдити ЗАРАЗ (не входимо в SHORT)
//   Ціна вгору + OI вниз/пласко -> закриваються шорти (шорт-сквіз) ->
//                             рух, скоріш за все, видихається -> вхід ОК
//   Ціна вниз + OI вгору   -> відкриваються НОВІ шорти -> рух ще має "паливо",
//                             ризиковано фейдити ЗАРАЗ (не входимо в LONG)
//   Ціна вниз + OI вниз/пласко -> закриваються лонги (ліквідація/панічний
//                             продаж) -> рух, скоріш за все, видихається -> вхід ОК
//
// Правило симетричне й не залежить від напрямку сигналу: блокуємо вхід,
// якщо OI ще ПОМІТНО росте (свіжі позиції відкриваються, рух не факт що
// видихся), дозволяємо, якщо OI плаский чи падає (позиції закриваються —
// ознака виснаження). Саме це описав запит: "після сигналу ціна ще довго
// йде в тому ж напрямку" — це і є ситуація "OI ще росте".
//
// Використовує Binance (той самий ринок, що дав сам сигнал) для
// методологічної консистентності — не MEXC.
// ============================================================================
class OpenInterestTracker {
  constructor(symbols, windowSeconds, pollIntervalMs) {
    this.symbols = symbols;
    this.windowMs = windowSeconds * 1000;
    this.pollIntervalMs = pollIntervalMs;
    this.history = new Map(); // symbol -> [{ts, oi}]
    this.timer = null;
  }

  start() {
    logger.info(`[OI] Старт полінгу Open Interest (кожні ${this.pollIntervalMs}мс, ${this.symbols.length} символів)`);
    this.pollAll();
    this.timer = setInterval(() => this.pollAll(), this.pollIntervalMs);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
  }

  async pollAll() {
    for (const symbol of this.symbols) {
      try {
        const res = await axios.get(`${config.BINANCE_REST}/fapi/v1/openInterest`, {
          params: { symbol },
          timeout: 5000
        });
        const oi = parseFloat(res.data.openInterest);

        if (!this.history.has(symbol)) this.history.set(symbol, []);
        const arr = this.history.get(symbol);
        arr.push({ ts: Date.now(), oi });

        const cutoff = Date.now() - this.windowMs - this.pollIntervalMs * 2;
        this.history.set(symbol, arr.filter(p => p.ts >= cutoff));
      } catch (error) {
        logger.warn(`[OI] ${symbol} помилка полінгу: ${error.message}`);
      }
      await sleep(150); // невелика затримка між символами, щоб не бити лімітом одним махом
    }
  }

  /**
   * Зміна OI за вікно (від найстарішого запису в межах windowMs до
   * найсвіжішого). Повертає sufficientData:false, якщо історії ще замало
   * (бот щойно стартував).
   */
  getChangeStats(symbol, windowMs) {
    const arr = this.history.get(symbol) || [];
    if (arr.length < 2) return { sufficientData: false };

    const cutoff = Date.now() - windowMs;
    const withinWindow = arr.filter(p => p.ts >= cutoff);
    const reference = withinWindow.length ? withinWindow[0] : arr[0];
    const latest = arr[arr.length - 1];

    if (!reference || !latest || reference.oi === 0) return { sufficientData: false };

    const changePercent = ((latest.oi - reference.oi) / reference.oi) * 100;

    return {
      sufficientData: true,
      changePercent,
      currentOI: latest.oi,
      referenceOI: reference.oi
    };
  }
}

module.exports = OpenInterestTracker;
