// Logger sederhana supaya output di terminal gampang dibaca saat demo.
const COLORS = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
};

export function createLogger(name, color = 'cyan') {
  const tag = `${COLORS[color]}[${name}]${COLORS.reset}`;
  const stamp = () =>
    `${COLORS.dim}${new Date().toISOString().slice(11, 23)}${COLORS.reset}`;

  return {
    info: (...a) => console.log(stamp(), tag, ...a),
    ok: (...a) => console.log(stamp(), tag, `${COLORS.green}OK${COLORS.reset}`, ...a),
    warn: (...a) => console.log(stamp(), tag, `${COLORS.yellow}WARN${COLORS.reset}`, ...a),
    error: (...a) => console.log(stamp(), tag, `${COLORS.red}ERROR${COLORS.reset}`, ...a),
    dead: (...a) => console.log(stamp(), tag, `${COLORS.magenta}DLQ${COLORS.reset}`, ...a),
  };
}
