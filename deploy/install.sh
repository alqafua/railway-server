#!/bin/bash
# ══════════════════════════════════════════════
#  RSI Scanner Pro v3 — Auto Install Script
#  يثبت Node.js + PM2 + المشروع + cloudflared
# ══════════════════════════════════════════════

set -e
RED='\033[0;31m' GREEN='\033[0;32m' YELLOW='\033[1;33m' NC='\033[0m'
log(){ echo -e "${GREEN}[✓]${NC} $1"; }
warn(){ echo -e "${YELLOW}[!]${NC} $1"; }
err(){ echo -e "${RED}[✗]${NC} $1"; exit 1; }

echo ""
echo "🚀 RSI Scanner Pro v3 — Auto Installer"
echo "════════════════════════════════════════"

# ── 1. تحديث النظام ──
log "تحديث النظام..."
apt-get update -qq && apt-get upgrade -y -qq

# ── 2. Node.js 20 ──
log "تثبيت Node.js 20..."
curl -fsSL https://deb.nodesource.com/setup_20.x | bash - > /dev/null 2>&1
apt-get install -y nodejs > /dev/null 2>&1
log "Node.js: $(node --version)"

# ── 3. PM2 ──
log "تثبيت PM2..."
npm install -g pm2 > /dev/null 2>&1
log "PM2: $(pm2 --version)"

# ── 4. إنشاء مجلد المشروع ──
log "إعداد المشروع..."
mkdir -p /root/rsi-scanner/server /root/rsi-scanner/client/public /root/logs

# ── 5. ملف .env ──
JWT_SECRET=$(openssl rand -hex 32)
cat > /root/rsi-scanner/.env << EOF
PORT=3000
JWT_SECRET=${JWT_SECRET}
NODE_ENV=production
EOF
log "ملف .env جاهز"

# ── 6. package.json ──
cat > /root/rsi-scanner/package.json << 'EOF'
{
  "name": "rsi-scanner-pro",
  "version": "3.0.0",
  "main": "server/index.js",
  "scripts": {
    "start": "node server/index.js"
  },
  "dependencies": {
    "bcryptjs": "^2.4.3",
    "dotenv": "^16.3.1",
    "express": "^4.18.2",
    "jsonwebtoken": "^9.0.2",
    "node-fetch": "^2.7.0",
    "ws": "^8.16.0"
  }
}
EOF

# ── 7. تثبيت المكتبات ──
log "تثبيت المكتبات..."
cd /root/rsi-scanner && npm install > /dev/null 2>&1
log "المكتبات جاهزة"

# ── 8. السيرفر (يُنزّل من رابط مباشر أو يُكتب) ──
log "تحميل ملفات المشروع..."

# نتحقق إذا الملفات موجودة مسبقاً
if [ ! -f /root/rsi-scanner/server/index.js ]; then
  warn "ملف السيرفر غير موجود!"
  warn "انسخ ملفات المشروع إلى /root/rsi-scanner/ ثم شغّل:"
  warn "pm2 start /root/rsi-scanner/server/index.js --name rsi-scanner"
  warn ""
  warn "أو شغّل هذا الأمر بعد نقل الملفات:"
  warn "cd /root/rsi-scanner && pm2 start server/index.js --name rsi-scanner && pm2 save"
else
  log "ملفات المشروع موجودة ✓"
fi

# ── 9. cloudflared (Cloudflare Tunnel) ──
log "تثبيت Cloudflare Tunnel..."
curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o /tmp/cloudflared.deb 2>/dev/null
dpkg -i /tmp/cloudflared.deb > /dev/null 2>&1 || true
rm /tmp/cloudflared.deb
log "cloudflared: $(cloudflared --version 2>/dev/null | head -1)"

# ── 10. PM2 startup ──
log "إعداد التشغيل التلقائي..."
pm2 startup systemd -u root --hp /root > /dev/null 2>&1 || true

# ── الانتهاء ──
echo ""
echo "════════════════════════════════════════"
echo -e "${GREEN}✅ التثبيت اكتمل!${NC}"
echo ""
echo "📋 الخطوات التالية:"
echo ""
echo "1) انقل ملفات المشروع:"
echo "   scp -r project/server/index.js root@$(curl -s ifconfig.me 2>/dev/null || echo 'IP'):/root/rsi-scanner/server/"
echo "   scp -r project/client/public/index.html root@$(curl -s ifconfig.me 2>/dev/null || echo 'IP'):/root/rsi-scanner/client/public/"
echo ""
echo "2) شغّل السيرفر:"
echo "   cd /root/rsi-scanner"
echo "   pm2 start server/index.js --name rsi-scanner"
echo "   pm2 save"
echo ""
echo "3) افتح نفق Cloudflare (للوصول من الإنترنت):"
echo "   cloudflared tunnel --url http://localhost:3000"
echo ""
echo "   أو افتح مباشرة:"
echo "   http://$(curl -s ifconfig.me 2>/dev/null || echo 'IP'):3000"
echo ""
echo "🔐 بيانات الدخول: Alqafua / 7007"
echo "════════════════════════════════════════"
