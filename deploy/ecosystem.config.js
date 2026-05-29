module.exports = {
  apps: [{
    name: 'rsi-scanner',
    script: 'server/index.js',
    cwd: '/root/rsi-scanner-pro',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '500M',
    env: {
      NODE_ENV: 'production',
      PORT: 3000
    },
    error_file: '/root/logs/rsi-error.log',
    out_file: '/root/logs/rsi-out.log',
    log_file: '/root/logs/rsi-combined.log',
    time: true
  }]
};
