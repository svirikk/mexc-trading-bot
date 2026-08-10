function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function isValidNumber(n) {
  return typeof n === 'number' && Number.isFinite(n);
}

// Округлення ВНИЗ до кратності step (для volUnit / lot size ф'ючерсу)
function floorToStep(value, step) {
  if (!step || step <= 0) return value;
  return Math.floor(value / step) * step;
}

// Округлення ціни до кратності priceUnit (tick size)
function roundToTick(value, tick) {
  if (!tick || tick <= 0) return value;
  const decimals = Math.max(0, (tick.toString().split('.')[1] || '').length);
  return parseFloat((Math.round(value / tick) * tick).toFixed(decimals));
}

function fmtUsd(num) {
  if (Math.abs(num) >= 1_000_000) return (num / 1_000_000).toFixed(2) + 'M';
  if (Math.abs(num) >= 1_000) return (num / 1_000).toFixed(0) + 'K';
  return num.toFixed(2);
}

function formatDuration(totalSeconds) {
  const h = Math.floor(totalSeconds / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = Math.floor(totalSeconds % 60);
  const parts = [];
  if (h) parts.push(`${h}г`);
  if (m) parts.push(`${m}хв`);
  parts.push(`${s}с`);
  return parts.join(' ');
}

function getCurrentDate() {
  return new Date().toISOString().split('T')[0];
}

// Мс до початку наступної хвилини (00 секунда) + додатковий буфер
function msUntilNextMinute(bufferMs = 0) {
  const now = new Date();
  const seconds = now.getSeconds();
  const ms = now.getMilliseconds();
  const msUntilBoundary = (60 - seconds) * 1000 - ms;
  return msUntilBoundary + bufferMs;
}

// Чи зараз в межах робочого вікна (за UTC годинами)
function isWithinTradingHours(tradingHoursCfg) {
  if (!tradingHoursCfg || !tradingHoursCfg.enabled) return true;
  const utcHour = new Date().getUTCHours();
  const { startHour, endHour } = tradingHoursCfg;
  if (startHour === endHour) return true; // вікно на всю добу
  if (startHour < endHour) {
    return utcHour >= startHour && utcHour < endHour;
  }
  // вікно, що переходить через північ UTC (напр. 22 -> 6)
  return utcHour >= startHour || utcHour < endHour;
}

// Мс до наступного разу, коли UTC-час досягне hour:minute (сьогодні або завтра)
function msUntilNextUtcTime(hour, minute = 0) {
  const now = new Date();
  const target = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),
    hour, minute, 0, 0
  ));
  if (target.getTime() <= now.getTime()) {
    target.setUTCDate(target.getUTCDate() + 1);
  }
  return target.getTime() - now.getTime();
}

module.exports = {
  sleep,
  isValidNumber,
  floorToStep,
  roundToTick,
  fmtUsd,
  formatDuration,
  getCurrentDate,
  msUntilNextMinute,
  isWithinTradingHours,
  msUntilNextUtcTime
};
