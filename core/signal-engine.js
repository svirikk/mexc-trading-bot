// ============================================================================
// SIGNAL DETECTION CORE
// Це той самий алгоритм агресивного потоку (aggTrade dominance/impulse-fade),
// що й у твоєму оригінальному index.js. Математику НЕ змінював навмисно —
// щоб тижневий тест був порівнюваний з попередніми результатами (~50-55%).
// Пропозиції щодо покращення алгоритму — окремим текстом, не тут.
// ============================================================================

const config = require('../config/settings');

class SymbolState {
  constructor(symbol, windowSeconds) {
    this.symbol = symbol;
    this.windowMs = windowSeconds * 1000;
    this.trades = [];
    this.firstPrice = null;
    this.lastPrice = null;
  }

  addTrade(timestamp, price, quantity, isBuyerMaker) {
    const volume = price * quantity;
    this.trades.push({
      timestamp,
      price,
      buyVol: isBuyerMaker ? 0 : volume,
      sellVol: isBuyerMaker ? volume : 0
    });
    this.lastPrice = price;
    if (this.firstPrice === null) this.firstPrice = price;
    this.cleanup(timestamp);
  }

  cleanup(currentTime) {
    const cutoff = currentTime - this.windowMs;
    this.trades = this.trades.filter(t => t.timestamp >= cutoff);
    this.firstPrice = this.trades.length > 0 ? this.trades[0].price : null;
  }

  getStats() {
    if (this.trades.length === 0) return null;

    let buyVolume = 0, sellVolume = 0;
    for (const t of this.trades) {
      buyVolume += t.buyVol;
      sellVolume += t.sellVol;
    }

    const totalVolume = buyVolume + sellVolume;
    if (totalVolume === 0) return null;

    const buyDominance = (buyVolume / totalVolume) * 100;
    const sellDominance = (sellVolume / totalVolume) * 100;
    const dominantSide = buyVolume > sellVolume ? 'buy' : 'sell';
    const dominance = Math.max(buyDominance, sellDominance);

    const priceChange = this.firstPrice
      ? ((this.lastPrice - this.firstPrice) / this.firstPrice) * 100
      : 0;

    const duration = (this.trades[this.trades.length - 1].timestamp - this.trades[0].timestamp) / 1000;

    return {
      buyVolume, sellVolume, totalVolume,
      dominantSide, dominance, priceChange, duration,
      tradeCount: this.trades.length,
      lastPrice: this.lastPrice
    };
  }

  reset() {
    this.trades = [];
    this.firstPrice = null;
    this.lastPrice = null;
  }
}

class TradeAggregator {
  constructor(windowSeconds) {
    this.windowSeconds = windowSeconds;
    this.states = new Map();
  }

  addTrade(symbol, timestamp, price, quantity, isBuyerMaker) {
    if (!this.states.has(symbol)) {
      this.states.set(symbol, new SymbolState(symbol, this.windowSeconds));
    }
    this.states.get(symbol).addTrade(timestamp, price, quantity, isBuyerMaker);
  }

  getStats(symbol) {
    const s = this.states.get(symbol);
    return s ? s.getStats() : null;
  }

  resetSymbol(symbol) {
    const s = this.states.get(symbol);
    if (s) s.reset();
  }

  getActiveCount() { return this.states.size; }

  getTotalTrades() {
    let total = 0;
    for (const s of this.states.values()) total += s.trades.length;
    return total;
  }
}

class SignalEngine {
  shouldAlert(symbol, stats) {
    if (!stats) return false;
    const cfg = config.getSymbolConfig(symbol);
    if (!cfg || !cfg.enabled) return false;

    if (stats.totalVolume < cfg.minVolumeUSD) return false;
    if (stats.dominance < cfg.minDominance) return false;
    if (Math.abs(stats.priceChange) < cfg.minPriceChange) return false;

    if (stats.dominantSide === 'buy' && stats.priceChange < 0) return false;
    if (stats.dominantSide === 'sell' && stats.priceChange > 0) return false;

    return true;
  }

  // Фейд імпульсу: агресивні покупки -> очікуємо відкат -> SHORT.
  // Агресивні продажі -> очікуємо відскок -> LONG.
  interpretSignal(stats) {
    if (stats.dominantSide === 'buy') {
      return { type: 'SHORT_SQUEEZE', label: 'SHORT SQUEEZE', emoji: '🟢', direction: 'SHORT' };
    }
    return { type: 'LONG_FLUSH', label: 'LONG FLUSH', emoji: '🔴', direction: 'LONG' };
  }
}

class CooldownManager {
  constructor() {
    this.cooldowns = new Map();
  }

  canAlert(symbol, stats) {
    const cfg = config.getSymbolConfig(symbol);
    if (!cfg) return false;
    if (!this.cooldowns.has(symbol)) return true;

    const last = this.cooldowns.get(symbol);
    const cooldownMs = cfg.cooldownMinutes * 60 * 1000;
    const elapsed = Date.now() - last.timestamp;

    if (elapsed < cooldownMs) {
      const oppositeSide = stats.dominantSide !== last.side;
      const biggerVolume = stats.totalVolume / last.volume >= 2.0;
      return oppositeSide || biggerVolume;
    }
    return true;
  }

  recordAlert(symbol, stats) {
    this.cooldowns.set(symbol, {
      timestamp: Date.now(),
      side: stats.dominantSide,
      volume: stats.totalVolume
    });
  }
}

module.exports = { TradeAggregator, SignalEngine, CooldownManager };
