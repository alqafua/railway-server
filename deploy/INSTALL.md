# دليل تثبيت RSI Scanner Pro v3

## المتطلبات
- Ubuntu 22.04 VPS (Hetzner CAX11 - 4€/شهر)
- Node.js 20+
- اتصال بـ Binance (سيرفر خارج المنطقة المحجوبة)

---

## 1. تثبيت Node.js

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
node --version  # يجب أن يظهر v20+
```

---

## 2. رفع المشروع

```bash
# على جهازك المحلي
scp -r rsi-scanner-pro/ root@YOUR_VPS_IP:/root/

# أو استخدم git
git clone https://github.com/YOUR_USER/rsi-scanner-pro /root/rsi-scanner-pro
```

---

## 3. إعداد المشروع

```bash
cd /root/rsi-scanner-pro
npm install

# إنشاء ملف البيئة
cp .env.example .env
nano .env
```

### محتوى ملف .env:
```
PORT=3000
JWT_SECRET=اكتب_هنا_كلمة_سر_عشوائية_طويلة
NODE_ENV=production
```

---

## 4. تثبيت PM2 (تشغيل 24/7)

```bash
npm install -g pm2

# تشغيل التطبيق
pm2 start server/index.js --name rsi-scanner

# تشغيل تلقائي عند إعادة التشغيل
pm2 startup
pm2 save

# مراقبة
pm2 status
pm2 logs rsi-scanner
```

---

## 5. Cloudflare Tunnel (بدون دومين - مجاني)

```bash
# تثبيت cloudflared
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o cloudflared.deb
sudo dpkg -i cloudflared.deb

# تسجيل الدخول (يفتح رابط في المتصفح)
cloudflared tunnel login

# إنشاء نفق
cloudflared tunnel create rsi-scanner
cloudflared tunnel route dns rsi-scanner rsi.yourdomain.com

# تشغيل
cloudflared tunnel run rsi-scanner
```

### أو بدون دومين (مؤقت للاختبار):
```bash
cloudflared tunnel --url http://localhost:3000
# سيعطيك رابط مثل: https://random-name.trycloudflare.com
```

---

## 6. Nginx (اختياري - لو عندك IP مباشر)

```bash
sudo apt install nginx -y

sudo nano /etc/nginx/sites-available/rsi-scanner
```

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/rsi-scanner /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx

# SSL مجاني
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d your-domain.com
```

---

## 7. تحقق أن كل شيء يعمل

```bash
# تحقق من الاتصال بـ Binance
curl -s https://fapi.binance.com/fapi/v1/ping
# يجب أن يرجع: {}

# تحقق من السيرفر
curl http://localhost:3000/api/state
# يجب أن يرجع JSON
```

---

## بيانات الدخول
- **Username:** Alqafua
- **Password:** 7007

---

## أوامر مفيدة

```bash
# إعادة تشغيل
pm2 restart rsi-scanner

# مشاهدة اللوق
pm2 logs rsi-scanner --lines 100

# إيقاف
pm2 stop rsi-scanner

# حالة
pm2 status
```
