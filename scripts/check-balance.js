const mexc = require('../services/mexc.service');

(async () => {
  try {
    const balance = await mexc.getUSDTBalance();
    console.log(`USDT (available): ${balance}`);
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
})();
