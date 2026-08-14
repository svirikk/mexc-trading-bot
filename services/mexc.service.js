// ============================================================================
// MEXC FUTURES REST CLIENT
// Побудовано за офіційною документацією: https://www.mexc.com/api-docs/futures
// Підпис (Introduction -> Request Format):
//   GET/DELETE : paramString = "k1=v1&k2=v2" (ключі відсортовані за алфавітом)
//   POST       : paramString = JSON.stringify(body)  (без сортування)
//   target     = accessKey + requestTimeMs + paramString
//   Signature  = HMAC_SHA256(target, secretKey)
//   Headers    = ApiKey, Request-Time, Signature
// ============================================================================

const axios = require('axios');
const crypto = require('crypto');
const config = require('../config/settings');
const logger = require('../utils/logger');

class MexcService {
  constructor() {
    this.baseURL = config.mexc.baseURL;
    this.apiKey = config.mexc.apiKey;
    this.apiSecret = config.mexc.apiSecret;
    this.contractCache = new Map(); // symbol -> {tick, volUnit, minVol, maxVol, contractSize}
  }

  sign(paramString, timestamp) {
    const target = `${this.apiKey}${timestamp}${paramString}`;
    return crypto.createHmac('sha256', this.apiSecret).update(target).digest('hex');
  }

  async request(method, path, params = {}, isPrivate = true) {
    const url = `${this.baseURL}${path}`;
    const headers = { 'Content-Type': 'application/json' };
    let axiosConfig = { method, url, headers, timeout: 10000 };

    if (!isPrivate) {
      if (method === 'GET' && Object.keys(params).length) {
        axiosConfig.params = params;
      }
    } else {
      const timestamp = Date.now().toString();
      let paramString;

      if (method === 'GET' || method === 'DELETE') {
        const entries = Object.entries(params)
          .filter(([, v]) => v !== undefined && v !== null && v !== '')
          .sort(([a], [b]) => a.localeCompare(b));
        paramString = entries.map(([k, v]) => `${k}=${v}`).join('&');
        if (paramString) axiosConfig.url = `${url}?${paramString}`;
      } else {
        // POST — тіло сериалізуємо ОДИН раз і використовуємо той самий рядок
        // і для підпису, і для фактичного запиту (порядок ключів має збігатись)
        const bodyStr = JSON.stringify(params);
        paramString = bodyStr;
        axiosConfig.data = bodyStr;
      }

      const signature = this.sign(paramString, timestamp);
      headers['ApiKey'] = this.apiKey;
      headers['Request-Time'] = timestamp;
      headers['Signature'] = signature;
    }

    try {
      const response = await axios(axiosConfig);
      if (response.data && response.data.success === false) {
        throw new Error(`MEXC API error [${response.data.code}]: ${response.data.message || 'unknown error'}`);
      }
      return response.data;
    } catch (error) {
      if (error.response) {
        const d = error.response.data;
        throw new Error(`MEXC HTTP ${error.response.status}: ${d?.message || JSON.stringify(d)}`);
      }
      throw error;
    }
  }

  // ---------------------------------------------------------------------
  // MARKET (public)
  // ---------------------------------------------------------------------

  async getContractDetail(symbol) {
    if (this.contractCache.has(symbol)) return this.contractCache.get(symbol);

    const res = await this.request('GET', '/api/v1/contract/detail', { symbol }, false);
    const d = res.data;
    if (!d) throw new Error(`Contract ${symbol} not found on MEXC`);

    const info = {
      symbol: d.symbol,
      contractSize: parseFloat(d.contractSize),
      priceUnit: parseFloat(d.priceUnit),   // tick size
      volUnit: parseFloat(d.volUnit),       // lot step, contracts
      minVol: parseFloat(d.minVol),
      maxVol: parseFloat(d.maxVol),
      minLeverage: d.minLeverage,
      maxLeverage: d.maxLeverage,
      priceScale: d.priceScale,
      apiAllowed: d.apiAllowed
    };
    this.contractCache.set(symbol, info);
    return info;
  }

  async getTicker(symbol) {
    const res = await this.request('GET', '/api/v1/contract/ticker', { symbol }, false);
    if (!res.data) throw new Error(`Ticker for ${symbol} not found`);
    return {
      lastPrice: parseFloat(res.data.lastPrice),
      bid1: parseFloat(res.data.bid1),
      ask1: parseFloat(res.data.ask1),
      fairPrice: parseFloat(res.data.fairPrice),
      timestamp: res.data.timestamp
    };
  }

  // ---------------------------------------------------------------------
  // ACCOUNT (private)
  // ---------------------------------------------------------------------

  async getUSDTBalance() {
    const res = await this.request('GET', '/api/v1/private/account/assets', {}, true);
    const list = res.data || [];
    const usdt = list.find(c => c.currency === 'USDT');
    if (!usdt) return 0;
    return parseFloat(usdt.availableBalance || '0');
  }

  // ---------------------------------------------------------------------
  // LEVERAGE
  // ---------------------------------------------------------------------

  async setLeverage({ symbol, leverage, openType, positionType, positionId = null }) {
    const params = { leverage };
    if (positionId) {
      params.positionId = positionId;
    } else {
      params.symbol = symbol;
      params.openType = openType;
      params.positionType = positionType;
    }
    return this.request('POST', '/api/v1/private/position/change_leverage', params, true);
  }

  // ---------------------------------------------------------------------
  // ORDERS
  // ---------------------------------------------------------------------

  /**
   * Ринковий вхід у позицію.
   * side: 1 = open long, 3 = open short
   */
  async openMarketOrder({ symbol, side, vol, leverage, openType, price, positionMode }) {
    const params = {
      symbol,
      price,        // деякі маршрути вимагають ціну навіть для маркет-ордера як price-protection
      vol,
      leverage,
      side,
      type: 5,       // 5 = market
      openType,
      positionMode
    };
    const res = await this.request('POST', '/api/v1/private/order/create', params, true);
    return res.data; // { orderId, ts }
  }

  async getOrder(orderId) {
    const res = await this.request('GET', `/api/v1/private/order/get/${orderId}`, {}, true);
    return res.data;
  }

  /**
   * Ставить TP і SL, прив'язані до позиції, ОДНИМ запитом (OCO-подібна
   * поведінка: коли одне тригериться, інше знімається).
   *
   * ІСТОРІЯ ДВОХ ПОМИЛОК НА РЕАЛЬНИХ ОРДЕРАХ (виправлено остаточно
   * 2026-08-12, за актуальною документацією MEXC після її реструктуризації):
   *
   * 1) [600] "takeProfitPrice and takeProfitOrderPrice cannot be set at the
   *    same time" — коли слались ОБИДВІ ціни (stopLossPrice/takeProfitPrice
   *    ТА stopLossOrderPrice/takeProfitOrderPrice) одночасно.
   * 2) Після видалення *OrderPrice полів — [5001] "Stop profit price and
   *    stop loss price cannot both be empty", бо насправді якраз
   *    stopLossOrderPrice/takeProfitOrderPrice/stopLossType є ОБОВ'ЯЗКОВИМИ
   *    полями (required: true в актуальній документації), а
   *    stopLossPrice/takeProfitPrice — опціональні (required: false) і
   *    конфліктують з *OrderPrice, якщо вказані разом.
   *
   * Правильна комбінація: слати ЛИШЕ *OrderPrice поля (обов'язкові) +
   * stopLossType/takeProfitType=1 (limit), НЕ слати stopLossPrice/
   * takeProfitPrice взагалі.
   */
  async placeTpSl({ positionId, vol, stopLossPrice, takeProfitPrice }) {
    const params = {
      positionId,
      vol,
      lossTrend: 1,                        // 1 = latest price як джерело тригера
      profitTrend: 1,
      stopLossOrderPrice: stopLossPrice,   // обов'язкове поле — ціна ліміт-SL
      stopLossType: 1,                      // 0 market SL / 1 limit SL
      takeProfitOrderPrice: takeProfitPrice, // обов'язкове поле — ціна ліміт-TP
      takeProfitType: 1,                    // 0 market TP / 1 limit TP
      volType: 2                            // 2 = TP/SL по всій позиції
    };
    const res = await this.request('POST', '/api/v1/private/stoporder/place', params, true);
    return res.data;
  }

  async cancelOrders(orderIds) {
    if (!orderIds || !orderIds.length) return null;
    const res = await this.request('POST', '/api/v1/private/order/cancel', { orderIds }, true);
    return res.data;
  }

  /**
   * Аварійне закриття позиції по ринку. side: 2 = close short, 4 = close long.
   * Використовується як страховка, якщо не вдалось поставити TP/SL —
   * краще гарантовано закрити зараз, ніж лишити позицію без захисту.
   */
  async closePositionMarket({ symbol, direction, vol, price }) {
    const side = direction === 'LONG' ? 4 : 2;
    const params = {
      symbol,
      price,
      vol,
      side,
      type: 5, // market
      openType: config.trading.openType
    };
    const res = await this.request('POST', '/api/v1/private/order/create', params, true);
    return res.data;
  }

  // ---------------------------------------------------------------------
  // POSITIONS
  // ---------------------------------------------------------------------

  async getOpenPositions(symbol = null) {
    const params = {};
    if (symbol) params.symbol = symbol;
    const res = await this.request('GET', '/api/v1/private/position/open_positions', params, true);
    return res.data || [];
  }

  async getHistoricalPositions({ symbol = null, pageNum = 1, pageSize = 20 } = {}) {
    const params = { page_num: pageNum, page_size: pageSize };
    if (symbol) params.symbol = symbol;
    const res = await this.request('GET', '/api/v1/private/position/list/history_positions', params, true);
    return res.data?.resultList || res.data || [];
  }

  async connect() {
    try {
      // Ping через публічний ендпоінт ф'ючерсів (не потребує підпису)
      await this.request('GET', '/api/v1/contract/ticker', { symbol: 'BTC_USDT' }, false);
      logger.info('[MEXC] ✅ Public API reachable');
      if (!config.trading.dryRun) {
        const balance = await this.getUSDTBalance();
        logger.info(`[MEXC] ✅ Private API authenticated. USDT balance: ${balance}`);
      }
      return true;
    } catch (error) {
      logger.error(`[MEXC] Connection check failed: ${error.message}`);
      throw error;
    }
  }
}

module.exports = new MexcService();
