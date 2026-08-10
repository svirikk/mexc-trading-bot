const WebSocket = require('ws');
const crypto = require('crypto');
const config = require('../config/settings');
const logger = require('../utils/logger');

// ============================================================================
// MEXC ПРИВАТНИЙ USER-DATA WEBSOCKET
// wss://contract.mexc.com/edge — після успішного логіну біржа САМА штовхає
// оновлення по ордерах/позиціях/балансу (push.personal.position і т.д.),
// без потреби опитувати REST. Використовується ТІЛЬКИ в живій торгівлі —
// в DRY_RUN логінитись немає куди (немає реального акаунта, що торгує).
// Документація: https://www.mexc.com/api-docs/futures/websocket-api
// ============================================================================
class MexcUserStream {
  constructor() {
    this.ws = null;
    this.pingInterval = null;
    this.reconnectAttempts = 0;
    this.loggedIn = false;
    this.onPositionUpdate = null; // (data) => void, data = payload push.personal.position
    this.onAuthFailed = null;     // () => void — щоб index.js міг сповістити в Telegram
  }

  setOnPositionUpdate(fn) { this.onPositionUpdate = fn; }
  setOnAuthFailed(fn) { this.onAuthFailed = fn; }

  sign(reqTime) {
    // Той самий підпис, що й для REST: HMAC_SHA256(accessKey + reqTime, secretKey).
    // Для логіну paramString порожній — підписується лише apiKey+reqTime.
    const target = `${config.mexc.apiKey}${reqTime}`;
    return crypto.createHmac('sha256', config.mexc.apiSecret).update(target).digest('hex');
  }

  connect() {
    logger.info('[WS-USER] Підключення до приватного user-data стріму MEXC...');
    this.ws = new WebSocket('wss://contract.mexc.com/edge');

    this.ws.on('open', () => {
      logger.info('[WS-USER] З\'єднання відкрито, авторизуюсь...');
      this.login();
    });

    this.ws.on('message', (data) => this.handleMessage(data));
    this.ws.on('error', (error) => logger.error(`[WS-USER] error: ${error.message}`));
    this.ws.on('close', () => {
      logger.warn('[WS-USER] З\'єднання закрито');
      this.loggedIn = false;
      this.stopPing();
      this.reconnect();
    });
  }

  login() {
    const reqTime = Date.now().toString();
    const signature = this.sign(reqTime);
    this.ws.send(JSON.stringify({
      method: 'login',
      param: { apiKey: config.mexc.apiKey, reqTime, signature }
    }));
  }

  startPing() {
    this.stopPing();
    // За документацією: якщо сервер не отримає ping протягом 1 хв — закриє
    // з'єднання. Шлемо раз на 15с для запасу.
    this.pingInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ method: 'ping' }));
      }
    }, 15000);
  }

  stopPing() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  handleMessage(raw) {
    try {
      const msg = JSON.parse(raw);

      if (msg.channel === 'rs.login') {
        logger.info('[WS-USER] ✅ Авторизація успішна — позиції відстежуються через push, без REST-полінгу');
        this.loggedIn = true;
        this.reconnectAttempts = 0;
        this.startPing();
        return;
      }

      if (msg.channel === 'rs.error') {
        logger.error(`[WS-USER] Помилка авторизації/команди: ${JSON.stringify(msg.data)}`);
        if (!this.loggedIn && this.onAuthFailed) this.onAuthFailed(msg.data);
        return;
      }

      if (msg.channel === 'pong') return;

      if (msg.channel === 'push.personal.position' && this.onPositionUpdate) {
        this.onPositionUpdate(msg.data);
      }
    } catch (error) {
      logger.error(`[WS-USER] parse error: ${error.message}`);
    }
  }

  reconnect() {
    if (this.reconnectAttempts >= config.MAX_RECONNECTS) {
      logger.error('[WS-USER] Вичерпано спроби перепідключення до user-стріму — далі тільки на REST fallback');
      return;
    }
    this.reconnectAttempts++;
    const delay = 3000 * this.reconnectAttempts;
    logger.info(`[WS-USER] Перепідключення через ${delay}мс (${this.reconnectAttempts}/${config.MAX_RECONNECTS})...`);
    setTimeout(() => this.connect(), delay);
  }

  close() {
    this.stopPing();
    if (this.ws) this.ws.close();
  }
}

module.exports = new MexcUserStream();
