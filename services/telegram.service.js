const TelegramBot = require('node-telegram-bot-api');
const config = require('../config/settings');
const logger = require('../utils/logger');
const { fmtUsd } = require('../utils/helpers');

// ВАЖЛИВО: polling: false. Цей бот більше НІКОГО не слухає — детекція сигналу
// і виконання угоди тепер живуть в одному процесі й спілкуються прямим викликом
// функції (див. index.js), а не через читання Telegram-повідомлень іншим ботом.
// Це прибирає затримку, ризик неправильного парсингу та втрату сигналів.
class TelegramService {
  constructor() {
    this.bot = new TelegramBot(config.telegram.token, { polling: false });
    this.chatId = config.telegram.chatId;
  }

  async send(message, options = {}) {
    try {
      await this.bot.sendMessage(this.chatId, message, { parse_mode: 'HTML', ...options });
    } catch (error) {
      logger.error(`[TELEGRAM] Send error: ${error.message}`);
    }
  }

  formatSignalDetected(symbol, direction, stats, plannedTime) {
    const emoji = direction === 'LONG' ? '🟢' : '🔴';
    return `${emoji} <b>СИГНАЛ ВИЯВЛЕНО</b>

<b>Символ:</b> ${symbol}
<b>Напрямок:</b> ${direction}
<b>Обʼєм:</b> $${fmtUsd(stats.totalVolume)} за ${stats.duration.toFixed(0)}с
<b>Домінування:</b> ${stats.dominance.toFixed(1)}%
<b>Δ ціни:</b> ${stats.priceChange >= 0 ? '+' : ''}${stats.priceChange.toFixed(2)}%
<b>Останні ціна:</b> $${stats.lastPrice}

⏱ Вхід заплановано на: <b>${plannedTime}</b>`;
  }

  formatPositionOpened(p) {
    const emoji = p.direction === 'LONG' ? '📈' : '📉';
    const oiLine = (p.oiChangePercent !== null && p.oiChangePercent !== undefined)
      ? `\n📊 <b>OI Δ на вході:</b> ${p.oiChangePercent >= 0 ? '+' : ''}${p.oiChangePercent.toFixed(2)}%`
      : '';
    return `✅ <b>ПОЗИЦІЮ ВІДКРИТО</b>

<b>Символ:</b> ${p.symbol}
<b>Напрямок:</b> ${emoji} ${p.direction}
<b>Ціна входу:</b> $${p.entryPrice}
<b>Контрактів:</b> ${p.contracts}
<b>Плече:</b> ${p.leverage}x
<b>Маржа:</b> $${p.requiredMargin.toFixed(2)}

🎯 <b>TP:</b> $${p.takeProfitPrice}
🛑 <b>SL:</b> $${p.stopLossPrice}
💰 <b>Ризик:</b> $${p.riskAmount.toFixed(2)}${oiLine}`;
  }

  formatPositionClosed(p) {
    const isProfit = p.pnl >= 0;
    const emoji = isProfit ? '🟢' : '🔴';
    const hitLabel = p.hitType ? ` (${p.hitType})` : '';
    const simulatedNote = p.simulated ? '\n\n🧪 <i>Симуляція DRY RUN — без реальних комісій/сліпеджу</i>' : '';
    return `${emoji} <b>ПОЗИЦІЮ ЗАКРИТО${hitLabel} — ${isProfit ? 'ПРИБУТОК' : 'ЗБИТОК'}</b>

<b>Символ:</b> ${p.symbol}
<b>Напрямок:</b> ${p.direction}
<b>Вхід:</b> $${p.entryPrice}  →  <b>Вихід:</b> $${p.exitPrice ?? '—'}
<b>Результат:</b> ${p.pnl >= 0 ? '+' : ''}$${p.pnl.toFixed(2)}
<b>Тривалість:</b> ${p.duration || '—'}${simulatedNote}`;
  }

  formatDailyStats(stats) {
    const { total, wins, losses, pnl } = stats;
    const winrate = total > 0 ? ((wins / total) * 100).toFixed(1) : '—';
    const emoji = pnl >= 0 ? '📈' : '📉';
    const dryRunNote = config.trading.dryRun ? '\n\n🧪 <i>DRY RUN — симуляція, без реальних комісій/сліпеджу</i>' : '';

    if (total === 0) {
      return `${emoji} <b>Підсумок дня</b>\n\nСьогодні угод не було.`;
    }

    return `${emoji} <b>Підсумок дня</b>

<b>Угод:</b> ${total}
<b>Прибуткових:</b> ${wins}
<b>Збиткових:</b> ${losses}
<b>Winrate:</b> ${winrate}%
<b>Результат:</b> ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}${dryRunNote}`;
  }

  formatSignalSkipped(symbol, direction, reason) {
    return `⏭ <b>СИГНАЛ ПРОІГНОРОВАНО</b>\n\n<b>Символ:</b> ${symbol}\n<b>Напрямок:</b> ${direction}\n<b>Причина:</b> ${reason}`;
  }

  formatError(context, error) {
    return `❌ <b>ПОМИЛКА</b>\n\n<b>Де:</b> ${context}\n<b>Деталі:</b> ${error.message || error}`;
  }
}

module.exports = new TelegramService();
