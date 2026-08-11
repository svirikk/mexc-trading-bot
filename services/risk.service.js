const config = require('../config/settings');
const { floorToStep, roundToTick, isValidNumber } = require('../utils/helpers');
const logger = require('../utils/logger');

/**
 * Рахує ціни TP/SL для заданої ціни входу. Винесено окремо, бо після
 * ринкового ордера треба перерахувати ці ціни під ФАКТИЧНУ (dealAvgPrice)
 * ціну заповнення, а не під ту, що була на момент розрахунку розміру
 * позиції кілька сотень мс тому.
 *
 * ПРИМІТКА: stopLossPrice/takeProfitPrice — це і є ЄДИНІ ціни, які реально
 * йдуть на MEXC (stoporder/place приймає лише одну ціну на сторону, див.
 * коментар у mexc.service.js::placeTpSl). stopLossOrderPrice/
 * takeProfitOrderPrice (з невеликим буфером) MEXC більше не отримує —
 * вони лишились лише для симуляції DRY_RUN (checkDryRunPosition
 * використовує їх як приблизну ціну заповнення після "спрацювання").
 */
function computeExitPrices(entryPrice, direction, contractInfo) {
  const { takeProfitPercent, stopLossPercent, slFillBufferPercent, tpFillBufferPercent } = config.risk;

  const stopLossPrice = direction === 'LONG'
    ? entryPrice * (1 - stopLossPercent / 100)
    : entryPrice * (1 + stopLossPercent / 100);

  const takeProfitPrice = direction === 'LONG'
    ? entryPrice * (1 + takeProfitPercent / 100)
    : entryPrice * (1 - takeProfitPercent / 100);

  const roundedStopLoss = roundToTick(stopLossPrice, contractInfo.priceUnit);
  const roundedTakeProfit = roundToTick(takeProfitPrice, contractInfo.priceUnit);

  const takeProfitOrderPrice = direction === 'LONG'
    ? roundToTick(roundedTakeProfit * (1 - tpFillBufferPercent / 100), contractInfo.priceUnit)
    : roundToTick(roundedTakeProfit * (1 + tpFillBufferPercent / 100), contractInfo.priceUnit);

  const stopLossOrderPrice = direction === 'LONG'
    ? roundToTick(roundedStopLoss * (1 - slFillBufferPercent / 100), contractInfo.priceUnit)
    : roundToTick(roundedStopLoss * (1 + slFillBufferPercent / 100), contractInfo.priceUnit);

  return {
    stopLossPrice: roundedStopLoss,
    stopLossOrderPrice,
    takeProfitPrice: roundedTakeProfit,
    takeProfitOrderPrice
  };
}

/**
 * Розраховує розмір позиції та ціни TP/SL.
 *
 * Логіка ризику (як просив):
 *  - SL відстань = STOP_LOSS_PERCENT% від ціни входу (рух ЦІНИ, не ROI)
 *  - Розмір позиції підбирається так, щоб у разі спрацювання SL
 *    втрата дорівнювала рівно RISK_PERCENT_OF_DEPOSIT% депозиту
 *  - Плече лише визначає необхідну маржу, на розмір позиції в USDT не впливає
 *
 * @param {number} balance - доступний USDT баланс на ф'ючерсному акаунті
 * @param {number} entryPrice - ціна на момент входу (свіжий тікер)
 * @param {'LONG'|'SHORT'} direction
 * @param {object} contractInfo - { contractSize, priceUnit, volUnit, minVol, maxVol }
 */
function calculatePositionParameters(balance, entryPrice, direction, contractInfo) {
  if (!isValidNumber(balance) || balance <= 0) {
    throw new Error(`Invalid balance: ${balance}`);
  }
  if (!isValidNumber(entryPrice) || entryPrice <= 0) {
    throw new Error(`Invalid entry price: ${entryPrice}`);
  }
  if (direction !== 'LONG' && direction !== 'SHORT') {
    throw new Error(`Invalid direction: ${direction}`);
  }

  const { percentOfDeposit, leverage, stopLossPercent } = config.risk;

  // 1. Скільки готові втратити в USDT, якщо SL спрацює
  const riskAmount = balance * (percentOfDeposit / 100);

  // 2. Розмір позиції в USDT: riskAmount = positionSizeUSDT * (stopLossPercent/100)
  let positionSizeUSDT = riskAmount / (stopLossPercent / 100);

  // 4. Необхідна маржа з урахуванням плеча
  let requiredMargin = positionSizeUSDT / leverage;

  if (requiredMargin > balance) {
    logger.warn(`[RISK] Необхідна маржа (${requiredMargin.toFixed(2)} USDT) перевищує баланс (${balance.toFixed(2)} USDT). Обмежую розмір позиції балансом.`);
    positionSizeUSDT = balance * leverage;
    requiredMargin = balance;
  }

  // 5. Переводимо USDT-розмір позиції у контракти MEXC
  //    quantityBase = скільки базового активу (напр. ADA) купуємо
  //    contracts    = quantityBase / contractSize, округлено ВНИЗ до volUnit
  const quantityBase = positionSizeUSDT / entryPrice;
  let contracts = floorToStep(quantityBase / contractInfo.contractSize, contractInfo.volUnit);

  if (contracts < contractInfo.minVol) {
    logger.warn(`[RISK] Розрахована кількість контрактів (${contracts}) менша за мінімум (${contractInfo.minVol}). Використовую мінімум.`);
    contracts = contractInfo.minVol;
  }
  if (contracts > contractInfo.maxVol) {
    logger.warn(`[RISK] Розрахована кількість контрактів (${contracts}) перевищує максимум (${contractInfo.maxVol}). Обмежую максимумом.`);
    contracts = contractInfo.maxVol;
  }

  // 6. Перерахунок фактичного розміру позиції та маржі після округлення
  const actualPositionSizeUSDT = contracts * contractInfo.contractSize * entryPrice;
  const finalRequiredMargin = actualPositionSizeUSDT / leverage;

  if (finalRequiredMargin > balance) {
    throw new Error(
      `Недостатньо балансу навіть для мінімального лоту. ` +
      `Потрібно: ${finalRequiredMargin.toFixed(2)} USDT, доступно: ${balance.toFixed(2)} USDT`
    );
  }

  // 7. Округлення ціни входу + розрахунок TP/SL
  const roundedEntryPrice = roundToTick(entryPrice, contractInfo.priceUnit);
  const exitPrices = computeExitPrices(roundedEntryPrice, direction, contractInfo);

  const result = {
    direction,
    entryPrice: roundedEntryPrice,
    contracts,
    positionSizeUSDT: actualPositionSizeUSDT,
    leverage,
    requiredMargin: finalRequiredMargin,
    riskAmount,
    ...exitPrices
  };

  logger.info(
    `[RISK] ${direction} | contracts=${contracts} | entry≈${roundedEntryPrice} | ` +
    `TP=${exitPrices.takeProfitPrice} | SL=${exitPrices.stopLossPrice} | ` +
    `margin=${finalRequiredMargin.toFixed(2)} USDT | risk=${riskAmount.toFixed(2)} USDT`
  );

  return result;
}

module.exports = { calculatePositionParameters, computeExitPrices };
