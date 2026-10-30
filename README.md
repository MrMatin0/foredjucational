# VLESS Node Runner (sing-box / Xray)

اجرای سریع نود **VLESS + WebSocket** روی هاست‌های مبتنی بر پنل Pterodactyl (مثل Bot-Hosting)، با امکان انتخاب هسته **sing-box** یا **Xray** و تونل اختیاری **Cloudflare**.

* بدون هیچ وابستگی npm (فقط Node.js 18 به بالا)
* دانلود خودکار هسته متناسب با معماری سرور (`amd64` / `arm64`)
* آپدیت خودکار هسته وقتی نسخه در تنظیمات عوض شود
* ری‌استارت خودکار هسته و تونل در صورت کرش

| جزء | نسخه پیش‌فرض |
| :--- | :--- |
| sing-box | `1.14.4` |
| Xray-core | `26.10.29` |
| cloudflared | `2026.10.6` |

---

## 🚀 راه‌اندازی

1. فایل‌های پروژه را در File Manager پنل آپلود کنید.
2. فایل `config.json` را ویرایش کنید:

```json
{
  "core": "sing-box",
  "uuid": "",
  "ws_path": "/vless-ws",
  "tunnel": true,
  "log_level": "warning"
}
```

| فیلد | توضیح |
| :--- | :--- |
| `core` | `sing-box` یا `xray` |
| `uuid` | UUID دلخواه. اگر خالی یا نامعتبر باشد، یک UUID تصادفی ساخته و در همین فایل ذخیره می‌شود |
| `ws_path` | مسیر WebSocket |
| `tunnel` | روشن/خاموش کردن تونل موقت Cloudflare (`trycloudflare.com`) |
| `log_level` | `debug` / `info` / `warning` / `error` |
| `versions` *(اختیاری)* | مثلا `{"xray": "latest"}` برای گرفتن آخرین نسخه پایدار از GitHub |

3. در کنسول پنل روی **Start** بزنید. لینک‌های اتصال (مستقیم با IP، مستقیم با دامنه PTR و تونل Cloudflare) در لاگ چاپ می‌شوند.

> فایل `config.json` قدیمی (فرمت sing-box) به‌صورت خودکار به فرمت جدید تبدیل می‌شود و UUID قبلی حفظ می‌شود.

### متغیرهای محیطی (اختیاری، اولویت بالاتر از config.json)

| متغیر | توضیح |
| :--- | :--- |
| `CORE` | `sing-box` یا `xray` |
| `UUID` | UUID کاربر |
| `WS_PATH` | مسیر WebSocket |
| `TUNNEL` | `false` برای خاموش کردن تونل |
| `LOG_LEVEL` | سطح لاگ |
| `SINGBOX_VERSION` / `XRAY_VERSION` / `CLOUDFLARED_VERSION` | نسخه مشخص یا `latest` |
| `SERVER_PORT` / `PORT` | پورت (پنل Pterodactyl خودش `SERVER_PORT` را ست می‌کند) |

---

## ⚙️ پارامترهای کلاینت

| پارامتر | نود مستقیم | نود CF Worker (پیشنهادی) |
| :--- | :--- | :--- |
| Protocol | VLESS | VLESS |
| Address | دامنه/IP سرور (مثلا `fi3.bot-hosting.net`) | IP تمیز Cloudflare (مثلا `104.16.160.1`) |
| Port | پورت اختصاصی پنل (مثلا `25150`) | `443` |
| UUID | UUID شما | UUID شما |
| Network | `ws` | `ws` |
| Path | `/vless-ws` | `/vless-ws` |
| TLS | `none` | `tls` |
| Host | *(خالی)* | دامنه Worker |
| SNI | *(خالی)* | دامنه Worker |

هر دو هسته (sing-box و Xray) دقیقا همین پارامترها را دارند، پس لینک‌ها با عوض کردن هسته تغییر نمی‌کنند.

### قالب لینک مستقیم
```text
vless://<UUID>@<SERVER_HOST>:<PORT>?encryption=none&security=none&type=ws&path=%2Fvless-ws#Direct
```

### قالب لینک CF Worker
```text
vless://<UUID>@104.16.160.1:443?encryption=none&security=tls&sni=<WORKER_DOMAIN>&type=ws&host=<WORKER_DOMAIN>&path=%2Fvless-ws#CF-Worker
```

---

## 🌐 Cloudflare Worker (ریورس پراکسی WebSocket)

دامنه‌های موقت `*.trycloudflare.com` زود فیلتر می‌شوند؛ Worker اختصاصی پایدارتر است.

1. در [داشبورد Cloudflare](https://dash.cloudflare.com/) وارد **Workers & Pages** شوید و یک Worker جدید بسازید.
2. کد زیر را جایگزین کنید و `ORIGIN_HOST` و `ORIGIN_PORT` را با اطلاعات نود خودتان عوض کنید:

```javascript
const ORIGIN_HOST = 'fi3.bot-hosting.net';
const ORIGIN_PORT = '25150';

export default {
  async fetch(request) {
    const url = new URL(request.url);
    url.protocol = 'http:';
    url.hostname = ORIGIN_HOST;
    url.port = ORIGIN_PORT;

    const headers = new Headers(request.headers);
    headers.set('Host', ORIGIN_HOST);

    return fetch(new Request(url.toString(), {
      method: request.method,
      headers,
      body: request.body,
      redirect: 'manual',
    }));
  },
};
```

3. **Save and deploy** بزنید، یک دامنه سفارشی به Worker وصل کنید و از قالب لینک CF Worker بالا استفاده کنید.

---

## 💡 نکات

* فعال کردن **Mux** در کلاینت، تاخیر ساخت اتصال‌های جدید را کم می‌کند.
* Address نود Worker را می‌توانید با سریع‌ترین IP تمیز Cloudflare در اپراتور خودتان عوض کنید.
* در Xray نسخه‌های جدید، ترنسپورت WebSocket یک هشدار «deprecated (بدون حذف)» در لاگ نشان می‌دهد؛ این هشدار عادی است و کارکرد را مختل نمی‌کند.
