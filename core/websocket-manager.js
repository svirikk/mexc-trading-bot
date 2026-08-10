const WebSocket = require('ws');
const config = require('../config/settings');
const logger = require('../utils/logger');

class MultiWebSocketManager {
  /**
   * @param {string[]} symbols
   * @param {TradeAggregator} tradeAggregator
   * @param {SignalEngine} signalEngine
   * @param {CooldownManager} cooldownManager
   * @param {(symbol: string, stats: object, interpretation: object) => void} onSignal
   *        Викликається СИНХРОННО в момент детекції — без Telegram-посередника.
   * @param {() => boolean} [isActiveFn]
   *        Якщо повертає false — трейди й далі накопичуються (вікно лишається
   *        "теплим"), але оцінка сигналів/кулдаун/onSignal пропускаються
   *        (режим "сну" поза робочими годинами).
   */
  constructor(symbols, tradeAggregator, signalEngine, cooldownManager, onSignal, isActiveFn = null) {
    this.symbols = symbols;
    this.tradeAggregator = tradeAggregator;
    this.signalEngine = signalEngine;
    this.cooldownManager = cooldownManager;
    this.onSignal = onSignal;
    this.isActiveFn = isActiveFn;

    this.connections = new Map();
    this.tradeCount = 0;
    this.lastStatsLog = Date.now();
    this.reconnectAttempts = new Map();
  }

  connectAll() {
    logger.info(`[WS] Підключення до ${this.symbols.length} символів...`);
    this.symbols.forEach((symbol, i) => {
      setTimeout(() => this.connectSymbol(symbol), i * 200);
    });
  }

  connectSymbol(symbol) {
    const streamName = `${symbol.toLowerCase()}@aggTrade`;
    // Новий шлях Binance для market-стрімів (легасі /ws більше не працює з 2026-04-23)
    const url = `${config.BINANCE_WS}/ws/${streamName}`;
    const ws = new WebSocket(url);

    ws.on('open', () => {
      logger.info(`[WS] ${symbol} підключено`);
      this.reconnectAttempts.set(symbol, 0);
    });

    ws.on('message', (data) => this.handleMessage(symbol, data));

    ws.on('error', (error) => logger.error(`[WS] ${symbol} error: ${error.message}`));

    ws.on('close', () => {
      logger.warn(`[WS] ${symbol} з'єднання закрито`);
      this.reconnectSymbol(symbol);
    });

    this.connections.set(symbol, ws);
  }

  handleMessage(symbol, data) {
    try {
      const trade = JSON.parse(data);
      const price = parseFloat(trade.p);
      const quantity = parseFloat(trade.q);
      const timestamp = trade.T;
      const isBuyerMaker = trade.m;

      this.tradeAggregator.addTrade(symbol, timestamp, price, quantity, isBuyerMaker);
      this.tradeCount++;

      // Поза робочими годинами: вікно й далі накопичується (щоб не було
      // "холодного старту" на відкритті вікна), але сигнали не оцінюються
      // і не витрачають кулдаун — жодних сповіщень уві сні.
      if (this.isActiveFn && !this.isActiveFn()) {
        this.logStats();
        return;
      }

      const stats = this.tradeAggregator.getStats(symbol);
      const cfg = config.getSymbolConfig(symbol);

      if (stats && cfg && stats.totalVolume >= cfg.minVolumeUSD * 0.5) {
        if (this.signalEngine.shouldAlert(symbol, stats)) {
          if (this.cooldownManager.canAlert(symbol, stats)) {
            const interpretation = this.signalEngine.interpretSignal(stats);
            this.cooldownManager.recordAlert(symbol, stats);
            this.tradeAggregator.resetSymbol(symbol);

            // Прямий виклик — жодного проміжного Telegram-повідомлення,
            // яке треба було б знову розпарсити іншим ботом.
            try {
              this.onSignal(symbol, stats, interpretation);
            } catch (err) {
              logger.error(`[WS] onSignal handler error for ${symbol}: ${err.message}`);
            }
          }
        }
      }

      this.logStats();
    } catch (error) {
      logger.error(`[WS] ${symbol} parse error: ${error.message}`);
    }
  }

  logStats() {
    const now = Date.now();
    if (now - this.lastStatsLog < config.STATS_LOG_INTERVAL * 1000) return;

    const active = this.tradeAggregator.getActiveCount();
    const totalTrades = this.tradeAggregator.getTotalTrades();
    const connected = Array.from(this.connections.values()).filter(ws => ws.readyState === WebSocket.OPEN).length;

    logger.info(`[STATS] Connected: ${connected}/${this.symbols.length} | Active: ${active} | Trades: ${totalTrades} | Rate: ${(this.tradeCount / config.STATS_LOG_INTERVAL).toFixed(0)}/s`);

    this.tradeCount = 0;
    this.lastStatsLog = now;
  }

  reconnectSymbol(symbol) {
    const attempts = this.reconnectAttempts.get(symbol) || 0;
    if (attempts >= config.MAX_RECONNECTS) {
      logger.error(`[WS] ${symbol} вичерпано спроби перепідключення`);
      return;
    }
    this.reconnectAttempts.set(symbol, attempts + 1);
    setTimeout(() => {
      logger.info(`[WS] ${symbol} перепідключення (${attempts + 1}/${config.MAX_RECONNECTS})...`);
      this.connectSymbol(symbol);
    }, 5000 * (attempts + 1));
  }

  closeAll() {
    for (const ws of this.connections.values()) ws.close();
    this.connections.clear();
  }
}

module.exports = MultiWebSocketManager;
