function ts() {
  return new Date().toISOString().replace('T', ' ').substring(0, 23);
}

const logger = {
  info: (msg) => console.log(`[${ts()}] [INFO] ${msg}`),
  warn: (msg) => console.warn(`[${ts()}] [WARN] ${msg}`),
  error: (msg) => console.error(`[${ts()}] [ERROR] ${msg}`),
  debug: (msg) => {
    if (process.env.DEBUG === 'true') console.log(`[${ts()}] [DEBUG] ${msg}`);
  }
};

module.exports = logger;
