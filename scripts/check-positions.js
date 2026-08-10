const mexc = require('../services/mexc.service');

(async () => {
  try {
    const positions = await mexc.getOpenPositions();
    if (!positions.length) {
      console.log('Відкритих позицій немає.');
      return;
    }
    for (const p of positions) {
      console.log(
        `${p.symbol} | ${p.positionType === 1 ? 'LONG' : 'SHORT'} | vol=${p.holdVol} | ` +
        `avg=${p.holdAvgPrice} | uPnL=${p.unRealizedPnl} | leverage=${p.leverage}x`
      );
    }
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
})();
