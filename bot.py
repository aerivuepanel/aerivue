import io
import os
import json
import hmac
import hashlib
import time
import asyncio
import re
import random
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone

import requests
import qrcode
from dotenv import load_dotenv

load_dotenv()
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes,
)
from telegram.constants import ParseMode


# ==================== PREMIUM EMOJIS ====================
# NOTE: these ids must be custom emoji ids your bot actually has access to.
# If you see raw <tg-emoji> tags in chat, the id below is invalid for this bot —
# grab a fresh id by inserting the emoji in a real Telegram chat and copying it.
PREMIUM_EMOJIS = {
    "⭐️": "5181422544162391976",
    "❤️": "5260535596941582167",
    "💬": "5258330865674494479",
    "⚡️": "5938539885907415367",
    "🌐": "6041705726206808304",
    "🔥": "5420315771991497307",
    "📈": "5774022692642492953",
    "🪙": "5884428842780594914",
    "💰": "6039802097916974085",
    "🤑": "5893473283696759404",
    "📱": "6152069549442208798",
    "💤": "5895266423952904371",
    "✅": "5197474765387864959",
    "🆔": "5936017305585586269",
    "🛡": "5920052658743283381",
    "🛡️": "5920052658743283381",
    "📤": "6030822047150512346",
    "⭐": "5879785854284599288",
    "👤": "5258011929993026890",
    "📝": "5879841310902324730",
    "⏱️": "5936170807716745162",
    "📌": "5796440171364749940",
    "🚀": "5780773956030043338",
    "🏆": "6194737030165959506",
    "👑": "5807868868886009920",
    "📖": "5258328383183396223",
    "ℹ️": "5994473545650934240",
    "💳": "6039802097916974085",
    "📦": "6181745837673211124",
    "🔑": "5936017305585586269",
    "📊": "5774022692642492953",
}

_TAG_RE = re.compile(r"<[^>]+>")


def pe(emoji: str) -> str:
    emoji_id = PREMIUM_EMOJIS.get(emoji)
    return f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>' if emoji_id else emoji


def strip_html(text: str) -> str:
    """Fallback for when HTML parse fails on Telegram's side — never leak raw tags."""
    if not text:
        return text
    return _TAG_RE.sub("", text)


def btn(text: str, callback_data: str, emoji: str = None, style: str = None) -> InlineKeyboardButton:
    icon_id = PREMIUM_EMOJIS.get(emoji) if emoji else None
    kwargs = {"text": text, "callback_data": callback_data}
    if icon_id:
        kwargs["icon_custom_emoji_id"] = icon_id
    if style:
        kwargs["style"] = style
    return InlineKeyboardButton(**kwargs)


def grid(buttons, cols=2):
    return [buttons[i:i + cols] for i in range(0, len(buttons), cols)]


# ==================== CONFIGURATION ====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
API_URL = os.environ.get("SMM_API_URL", "https://easysmmpanel.com/api/v2")
API_KEY = os.environ.get("SMM_API_KEY", "")

# Razorpay credentials. Set these as environment variables; never hard-code secrets.
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET", "")
RAZORPAY_WEBHOOK_SECRET = os.environ.get("RAZORPAY_WEBHOOK_SECRET", "")

# <-- REPLACE WITH YOUR OWN TELEGRAM NUMERIC USER ID (get it from @userinfobot)
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "demon_smm_bot")

# Public channel for "new user" / "new order" announcements. Bot must be an
# admin of this channel, otherwise these posts fail silently (logged only).
CHANNEL_ID = os.environ.get("CHANNEL_ID", "@aerivue")

# Port the health-check server listens on — some hosts (Render/Railway free
# tiers etc.) need an open HTTP port to consider the process "alive".
PORT = int(os.environ.get("PORT", "8080"))

CATEGORIES_PER_PAGE = 10
SERVICES_PER_PAGE = 5

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


BOT_INSTANCE = None
BOT_LOOP = None

def verify_razorpay_webhook(raw_body: bytes, signature: str) -> bool:
    if not RAZORPAY_WEBHOOK_SECRET or not signature:
        return False
    expected = hmac.new(RAZORPAY_WEBHOOK_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def process_razorpay_webhook(payload: dict):
    """Schedule webhook processing without blocking Razorpay's HTTP request."""
    if not BOT_INSTANCE or not BOT_LOOP:
        logger.error("Razorpay webhook received before bot loop was ready")
        return
    future = asyncio.run_coroutine_threadsafe(
        BOT_INSTANCE.handle_razorpay_webhook(payload), BOT_LOOP
    )
    def _done(fut):
        try:
            fut.result()
        except Exception as exc:
            logger.exception("Razorpay webhook processing failed: %s", exc)
    future.add_done_callback(_done)


class _HealthHandler(BaseHTTPRequestHandler):
    """Health endpoint + Razorpay webhook endpoint on the same public port."""
    def do_GET(self):
        if self.path != "/":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running")

    def do_POST(self):
        if self.path != "/razorpay/webhook":
            self.send_response(404)
            self.end_headers()
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length)
            signature = self.headers.get("X-Razorpay-Signature", "")
            if not verify_razorpay_webhook(raw_body, signature):
                logger.warning("Rejected Razorpay webhook: invalid signature")
                self.send_response(401)
                self.end_headers()
                return
            payload = json.loads(raw_body.decode("utf-8"))
            if payload.get("event") == "payment_link.paid":
                process_razorpay_webhook(payload)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        except Exception as e:
            logger.exception(f"Razorpay webhook error: {e}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def start_health_server(port: int):
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"HTTP server listening on port {port} — GET / and POST /razorpay/webhook")


# ==================== RESELLER MARKUP PRICING ENGINE ====================
TIER1_MAX_QTY = 2000       # up to this qty -> full markup (your "list price")
TIER2_MIN_QTY = 10000      # from this qty -> discounted markup (reads as a bulk deal)
MARKUP_TIER1 = 0.50        # +50% below TIER1_MAX_QTY
MARKUP_MID = 0.40          # +40% between the two tiers
MARKUP_TIER2 = 0.30        # +30% at/above TIER2_MIN_QTY


def markup_for_quantity(quantity: int) -> float:
    if quantity <= TIER1_MAX_QTY:
        return MARKUP_TIER1
    if quantity >= TIER2_MIN_QTY:
        return MARKUP_TIER2
    return MARKUP_MID


def display_rate_per_1000(real_rate: float, quantity: int) -> float:
    """The rate/1000 a normal user sees, tiered so bulk orders look discounted."""
    return round(real_rate * (1 + markup_for_quantity(quantity)), 4)


def compute_cost(real_rate: float, quantity: int):
    """
    Returns (cost, base_rate, tiered_rate, discounted, savings_pct) for a NORMAL user.
    base_rate = the full-markup "list" rate (what tier-1 pricing would be).
    tiered_rate = the actual rate applied for this quantity.
    """
    base_rate = round(real_rate * (1 + MARKUP_TIER1), 4)
    tiered_rate = display_rate_per_1000(real_rate, quantity)
    cost = round(tiered_rate / 1000 * quantity, 2)
    discounted = quantity >= TIER2_MIN_QTY
    savings_pct = round((1 - tiered_rate / base_rate) * 100) if discounted and base_rate else 0
    return cost, base_rate, tiered_rate, discounted, savings_pct


# ==================== API CLASS ====================
class EasySMMAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.api_url = API_URL

    def _request(self, data):
        data['key'] = self.api_key
        try:
            response = requests.post(self.api_url, data=data, timeout=30)
            return response.json()
        except Exception as e:
            logger.error(f"API Error: {e}")
            return {"error": str(e)}

    def get_services(self):
        return self._request({'action': 'services'})

    def get_balance(self):
        return self._request({'action': 'balance'})

    def add_order(self, service_id, link, quantity):
        return self._request({'action': 'add', 'service': service_id, 'link': link, 'quantity': quantity})

    def get_order_status(self, order_id):
        return self._request({'action': 'status', 'order': order_id})

    def get_multiple_status(self, order_ids):
        return self._request({'action': 'status', 'orders': ','.join(map(str, order_ids))})

    def cancel_orders(self, order_ids):
        return self._request({'action': 'cancel', 'orders': ','.join(map(str, order_ids))})

    def create_refill(self, order_id):
        return self._request({'action': 'refill', 'order': order_id})

    def create_multiple_refill(self, order_ids):
        return self._request({'action': 'refill', 'orders': ','.join(map(str, order_ids))})

    def get_refill_status(self, refill_id):
        return self._request({'action': 'refill_status', 'refill': refill_id})

    def get_multiple_refill_status(self, refill_ids):
        return self._request({'action': 'refill_status', 'refills': ','.join(map(str, refill_ids))})


# ==================== MONGODB STORE ====================
class Store:
    """
    Collections:
      users          {_id: telegram_user_id, balance, name, username}
      pending        {_id: txn_id, user_id, name, username, amount, utr, screenshot_file_id, created_at}
    """
    def __init__(self, uri, db_name):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.users = self.db.users
        self.pending = self.db.pending_payments

    async def ensure_indexes(self):
        await self.users.create_index("_id")
        await self.pending.create_index("payment_link_id", unique=True, sparse=True)
        await self.pending.create_index("razorpay_payment_id", unique=True, sparse=True)
        await self.users.create_index("razorpay_payment_ids")

    # ---- wallets ----
    async def ensure_user(self, uid: int, name: str = None, username: str = None) -> bool:
        """Upserts the user; returns True if this call created a brand-new record."""
        result = await self.users.update_one(
            {"_id": uid},
            {"$setOnInsert": {"balance": 0}, "$set": {"name": name or "", "username": username or ""}},
            upsert=True,
        )
        return result.upserted_id is not None

    async def get_balance(self, uid: int) -> float:
        doc = await self.users.find_one({"_id": uid})
        return float(doc["balance"]) if doc else 0.0

    async def add_balance(self, uid: int, amount: float, name: str = None) -> float:
        amount = round(float(amount), 2)
        update = {"$inc": {"balance": amount}}
        if name:
            update["$set"] = {"name": name}
        doc = await self.users.find_one_and_update(
            {"_id": uid}, update, upsert=True, return_document=ReturnDocument.AFTER
        )
        return float(doc["balance"])

    async def deduct_balance(self, uid: int, amount: float) -> bool:
        """Atomic — only succeeds if balance >= amount at the moment of the update."""
        amount = round(float(amount), 2)
        doc = await self.users.find_one_and_update(
            {"_id": uid, "balance": {"$gte": amount}},
            {"$inc": {"balance": -amount}},
        )
        return doc is not None

    # ---- pending payment verification ----
    async def add_pending(self, txn_id: str, info: dict):
        doc = dict(info)
        doc["_id"] = txn_id
        doc["created_at"] = datetime.now(timezone.utc)
        await self.pending.replace_one({"_id": txn_id}, doc, upsert=True)

    async def pop_pending(self, txn_id: str):
        return await self.pending.find_one_and_delete({"_id": txn_id})


    async def add_razorpay_pending(self, txn_id: str, info: dict):
        doc = dict(info)
        doc["_id"] = txn_id
        doc["status"] = "pending"
        doc["created_at"] = datetime.now(timezone.utc)
        await self.pending.replace_one({"_id": txn_id}, doc, upsert=True)

    async def credit_razorpay_payment(self, payment_link_id: str, payment_id: str, amount_paise: int):
        """Validate a pending QR payment and credit the wallet idempotently.

        The user's wallet update is atomic and also stores the Razorpay payment id,
        so the same payment can never be credited twice even if Razorpay retries
        the webhook.
        """
        info = await self.pending.find_one({
            "payment_link_id": payment_link_id,
            "status": "pending",
        })
        if not info:
            # A duplicate webhook may arrive after the pending record was marked
            # credited. Treat it as already handled rather than crediting again.
            existing = await self.pending.find_one({"razorpay_payment_id": payment_id})
            if existing:
                return existing, "already_processed"
            return None, "not_found"

        expected_paise = int(round(float(info.get("amount", 0)) * 100))
        if amount_paise != expected_paise:
            await self.pending.update_one(
                {"_id": info["_id"], "status": "pending"},
                {"$set": {
                    "status": "amount_mismatch",
                    "razorpay_payment_id": payment_id,
                    "received_amount_paise": amount_paise,
                    "updated_at": datetime.now(timezone.utc),
                }}
            )
            return None, f"amount_mismatch:{amount_paise}!={expected_paise}"

        uid = int(info["user_id"])
        amount = float(info["amount"])

        # One MongoDB document update = atomic. The same payment_id cannot
        # increment the balance twice.
        wallet = await self.users.find_one_and_update(
            {"_id": uid, "razorpay_payment_ids": {"$ne": payment_id}},
            {
                "$inc": {"balance": amount},
                "$addToSet": {"razorpay_payment_ids": payment_id},
                "$set": {"name": info.get("name", ""), "username": info.get("username", "")},
            },
            upsert=False,
            return_document=ReturnDocument.AFTER,
        )

        # No match means this exact Razorpay payment id was already recorded.
        if wallet is None:
            existing_user = await self.users.find_one({"_id": uid})
            if existing_user and payment_id in existing_user.get("razorpay_payment_ids", []):
                await self.pending.update_one(
                    {"_id": info["_id"], "status": "pending"},
                    {"$set": {
                        "status": "credited",
                        "razorpay_payment_id": payment_id,
                        "credited_at": datetime.now(timezone.utc),
                    }}
                )
            return None, "already_processed"

        new_balance = float(wallet.get("balance", 0))
        claimed = await self.pending.find_one_and_update(
            {"_id": info["_id"], "status": "pending"},
            {"$set": {
                "status": "credited",
                "razorpay_payment_id": payment_id,
                "credited_at": datetime.now(timezone.utc),
            }},
            return_document=ReturnDocument.AFTER,
        )
        if not claimed:
            # Wallet already contains this payment id, so this is still safe.
            claimed = await self.pending.find_one({"_id": info["_id"]}) or info

        return {**claimed, "new_balance": new_balance}, "credited"

# ==================== MAIN BOT ====================
class SMMBot:
    def __init__(self, token, api_key):
        self.app = Application.builder().token(token).build()
        self.api = EasySMMAPI(api_key)
        self.store = Store(MONGO_URI, MONGO_DB_NAME)
        self.setup_handlers()

    def setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("balance", self.balance_command))
        self.app.add_handler(CommandHandler("services", self.services_command))
        self.app.add_handler(CommandHandler("order", self.order_command))
        self.app.add_handler(CommandHandler("status", self.status_command))
        self.app.add_handler(CommandHandler("cancel", self.cancel_command))
        self.app.add_handler(CommandHandler("refill", self.refill_command))
        self.app.add_handler(CommandHandler("refillstatus", self.refill_status_command))
        self.app.add_handler(CommandHandler("payment", self.payment_command))

        self.app.add_handler(CallbackQueryHandler(
            self.button_callback,
            pattern="^(bal|serv|ord|pay|stat|canc|refstat|ref|menu)$"))
        self.app.add_handler(CallbackQueryHandler(self.categories_page_callback, pattern="^catpg_"))
        self.app.add_handler(CallbackQueryHandler(self.category_callback, pattern="^cat_"))
        self.app.add_handler(CallbackQueryHandler(self.page_callback, pattern="^pg_"))
        self.app.add_handler(CallbackQueryHandler(self.payment_callback, pattern="^payamt_"))
        self.app.add_handler(CallbackQueryHandler(self.order_confirm_callback, pattern="^ordconfirm$"))
        self.app.add_handler(CallbackQueryHandler(self.order_cancel_callback, pattern="^ordcancel$"))

        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    # ==================== RENDER (plain send, nothing ever deleted) ====================
    async def render(self, update, context, text=None, reply_markup=None, photo=None, caption=None, chat_id=None):
        chat_id = chat_id or update.effective_chat.id
        try:
            if photo is not None:
                sent = await context.bot.send_photo(
                    chat_id=chat_id, photo=photo, caption=caption,
                    reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            else:
                sent = await context.bot.send_message(
                    chat_id=chat_id, text=text,
                    reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except Exception as e:
            # HTML (or a bad custom-emoji id) got rejected -> degrade to
            # plain text instead of leaking raw <tg-emoji> tags to the user.
            logger.error(f"Render send error, falling back to plain text: {e}")
            plain_text = strip_html(text) if text else None
            plain_caption = strip_html(caption) if caption else None
            if photo is not None:
                try:
                    photo.seek(0)
                except Exception:
                    pass
                sent = await context.bot.send_photo(chat_id=chat_id, photo=photo, caption=plain_caption, reply_markup=reply_markup)
            else:
                sent = await context.bot.send_message(chat_id=chat_id, text=plain_text, reply_markup=reply_markup)
        return sent

    async def post_to_channel(self, context, text):
        """Best-effort announcement post — never breaks the user-facing flow."""
        try:
            await context.bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Could not post to channel {CHANNEL_ID} (is the bot an admin there?): {e}")

    def menu_kb(self):
        return InlineKeyboardMarkup(grid([btn("Main Menu", "menu", emoji="🔑")], cols=1))

    # ==================== CATEGORIES ====================
    def categorize(self, service):
        name = service.get('name', '')
        category = service.get('category', 'Uncategorized')
        if not category or category == 'Uncategorized':
            for key in ('Instagram', 'Facebook', 'YouTube', 'Youtube', 'Twitter', 'TikTok', 'Telegram', 'Spotify'):
                if key in name:
                    category = 'YouTube' if key == 'Youtube' else key
                    break
            else:
                category = 'Other'
        return category

    def extract_categories(self, services):
        categories = {}
        for service in services:
            categories.setdefault(self.categorize(service), []).append(service)
        return categories

    async def get_all_services(self, context):
        """Cache the panel's full service list in bot_data (shared, refreshed on /services)."""
        cached = context.bot_data.get('all_services')
        if cached:
            return cached
        result = self.api.get_services()
        if isinstance(result, list):
            context.bot_data['all_services'] = result
            return result
        return []

    async def find_service(self, context, service_id):
        services = await self.get_all_services(context)
        for s in services:
            if str(s.get('service')) == str(service_id):
                return s
        return None

    def is_admin(self, update) -> bool:
        return bool(update.effective_user) and update.effective_user.id == ADMIN_ID

    # ==================== COMMANDS ====================

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Clear any in-progress flow so repeated /start calls are always clean
        # and independent of whatever the user was doing before.
        context.user_data.clear()

        keyboard = grid([
            btn("Balance", "bal", emoji="💰", style="primary"),
            btn("Services", "serv", emoji="📊", style="primary"),
            btn("New Order", "ord", emoji="📦", style="success"),
            btn("Add Balance", "pay", emoji="💳", style="success"),
            btn("Order Status", "stat", emoji="📈", style="primary"),
            btn("Cancel Order", "canc", emoji="🛡️", style="danger"),
            btn("Refill", "ref", emoji="📤", style="primary"),
            btn("Refill Status", "refstat", emoji="📝", style="primary"),
        ], cols=2)
        reply_markup = InlineKeyboardMarkup(keyboard)

        user = update.effective_user
        name = user.first_name if user else "User"
        is_new_user = False
        if user and user.id != ADMIN_ID:
            is_new_user = await self.store.ensure_user(user.id, name=name, username=user.username)

        text = f"""{pe('🔥')} <b>Welcome {name}!</b>
{pe('🚀')} <b>EasySMMPanel Bot</b>

{pe('📌')} Choose an option below:"""

        await self.render(update, context, text=text, reply_markup=reply_markup)

        if is_new_user:
            await self.post_to_channel(
                context,
                f"""{pe('🚀')} <b>New user joined the bot!</b>

{pe('👤')} {name} (@{user.username or 'no_username'})""")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = f"""{pe('📖')} <b>EasySMMPanel Bot Commands</b>

{pe('⭐')} /start - Show main menu
{pe('ℹ️')} /help - Show this help
{pe('💰')} /balance - Check your balance
{pe('📊')} /services - Browse services by category
{pe('📦')} /order - Place a new order
{pe('💳')} /payment - Add balance via UPI
{pe('📈')} /status - Check order status
{pe('🛡️')} /cancel - Cancel an order
{pe('📤')} /refill - Create a refill
{pe('📝')} /refillstatus - Check refill status"""
        await self.render(update, context, text=text)

    async def balance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if self.is_admin(update):
            await self.render(update, context, text=f"{pe('⏱️')} Fetching panel balance...")
            result = self.api.get_balance()
            if result and 'balance' in result:
                text = f"""{pe('💰')} <b>Panel Balance (Admin)</b>

Balance: <code>{result['balance']} {result.get('currency', 'USD')}</code>
{pe('⏱️')} Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
            else:
                text = f"{pe('ℹ️')} Failed to fetch panel balance."
        else:
            bal = await self.store.get_balance(user.id) if user else 0
            text = f"""{pe('💰')} <b>Your Balance</b>

Balance: <code>₹{bal:.2f}</code>"""

        await self.render(update, context, text=text, reply_markup=self.menu_kb())

    async def services_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.render(update, context, text=f"{pe('⏱️')} Loading services...")
        services = await self.get_all_services(context)
        if not services:
            await self.render(update, context, text=f"{pe('ℹ️')} Failed to fetch services.", reply_markup=self.menu_kb())
            return

        categories = self.extract_categories(services)
        sorted_cats = sorted(categories.keys())
        context.user_data['categories'] = categories
        context.user_data['sorted_categories'] = sorted_cats
        context.user_data['total_services'] = len(services)

        await self.render_categories_page(update, context, 0)

    async def render_categories_page(self, update, context, page):
        categories = context.user_data.get('categories', {})
        sorted_cats = context.user_data.get('sorted_categories', [])
        total_services = context.user_data.get('total_services', 0)

        per_page = CATEGORIES_PER_PAGE
        total_pages = max(1, (len(sorted_cats) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        start, end = page * per_page, min(page * per_page + per_page, len(sorted_cats))

        cat_buttons = [btn(f"{sorted_cats[i]} ({len(categories[sorted_cats[i]])})", f"cat_{i}", emoji="📦")
                       for i in range(start, end)]
        keyboard = grid(cat_buttons, cols=2)

        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("‹ Prev", callback_data=f"catpg_{page-1}"))
        nav_row.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="serv"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ›", callback_data=f"catpg_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        keyboard.append([btn("Main Menu", "menu", emoji="🔑")])

        text = f"""{pe('📌')} <b>Categories</b>

{pe('📦')} {len(sorted_cats)} categories · {total_services} services total

Select a category:"""
        await self.render(update, context, text=text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def show_category_services(self, update, context: ContextTypes.DEFAULT_TYPE, cat_index, page=0):
        categories = context.user_data.get('categories', {})
        sorted_cats = context.user_data.get('sorted_categories', [])

        if cat_index < 0 or cat_index >= len(sorted_cats):
            await self.render(update, context, text=f"{pe('ℹ️')} Category not found — try /services again.", reply_markup=self.menu_kb())
            return

        category = sorted_cats[cat_index]
        services = categories.get(category, [])
        per_page = SERVICES_PER_PAGE
        total_pages = max(1, (len(services) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        start, end = page * per_page, min(page * per_page + per_page, len(services))

        admin = self.is_admin(update)
        text = f"{pe('📌')} <b>{category}</b> ({len(services)} services)\n\n"
        for service in services[start:end]:
            real_rate = float(service.get('rate', 0) or 0)
            text += f"{pe('🆔')} <b>{service.get('service')}</b> - {service.get('name', 'Unknown')[:40]}\n"
            if admin:
                text += f"   {pe('💰')} Rate/1000: {service.get('rate', 'N/A')} | Min: {service.get('min', 'N/A')} | Max: {service.get('max', 'N/A')}\n\n"
            else:
                disp_rate = display_rate_per_1000(real_rate, TIER1_MAX_QTY)
                text += f"   {pe('💰')} Rate/1000: ₹{disp_rate:.2f} | Min: {service.get('min', 'N/A')} | Max: {service.get('max', 'N/A')}\n"
                text += f"   {pe('🔥')} Order {TIER2_MIN_QTY:,}+ for a bulk discount\n\n"

        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("‹ Prev", callback_data=f"pg_{cat_index}_{page-1}"))
        nav_row.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="serv"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ›", callback_data=f"pg_{cat_index}_{page+1}"))

        keyboard = [nav_row] if nav_row else []
        keyboard += grid([btn("Order Now", "ord", emoji="📦", style="success"),
                           btn("Categories", "serv", emoji="📊")], cols=2)
        keyboard.append([btn("Main Menu", "menu", emoji="🔑")])

        await self.render(update, context, text=text, reply_markup=InlineKeyboardMarkup(keyboard))

    # ==================== PAYMENT (Razorpay Payment Link + QR) ====================

    async def payment_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = grid([
            btn("₹100", "payamt_100", emoji="💰", style="primary"),
            btn("₹500", "payamt_500", emoji="💰", style="primary"),
            btn("₹1000", "payamt_1000", emoji="💰", style="primary"),
            btn("₹2000", "payamt_2000", emoji="💰", style="primary"),
        ], cols=2)
        keyboard.append([btn("Custom Amount", "payamt_custom", emoji="💳")])
        keyboard.append([btn("Main Menu", "menu", emoji="🔑")])
        text = f"""{pe('💳')} <b>Add Balance</b>

{pe('💰')} Select an amount
{pe('📱')} Pay securely via Razorpay UPI QR

{pe('⚡️')} Payment is automatically verified — no screenshot or UTR required."""
        await self.render(update, context, text=text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def payment_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if query.data == "payamt_custom":
            context.user_data['payment_step'] = 'custom'
            await self.render(update, context, text=f"{pe('💳')} Enter amount (in INR):\n\nExample: 250")
            return
        amount = int(query.data.split("_")[1])
        await self.generate_upi_payment(update, amount, context)

    def razorpay_ready(self):
        return bool(RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET and RAZORPAY_WEBHOOK_SECRET)

    def create_razorpay_payment_link(self, amount: int, txn_id: str, user_id: int):
        """Create a Razorpay UPI Payment Link. A local QR is generated from its short_url.

        This avoids the separate Razorpay Payment Link QRs API feature, which may be disabled
        on an account. Payment Links are available as a standard product and expose
        the payment_link.paid webhook event.
        """
        if not self.razorpay_ready():
            return {"error": "Razorpay is not configured. Set RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET and RAZORPAY_WEBHOOK_SECRET."}
        payload = {
            "upi_link": True,
            "amount": int(amount * 100),
            "currency": "INR",
            "accept_partial": False,
            "reference_id": txn_id[:40],
            "description": f"Wallet top-up {txn_id}",
            "notes": {
                "telegram_user_id": str(user_id),
                "txn_id": txn_id,
            },
            "expire_by": int(time.time()) + 30 * 60,
            "reminder_enable": False,
            "notify": {"sms": False, "email": False},
        }
        try:
            response = requests.post(
                "https://api.razorpay.com/v1/payment_links",
                auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
                json=payload, timeout=30
            )
            data = response.json()
            if response.status_code >= 400:
                logger.error("Razorpay Payment Link create failed: %s", data)
                return {"error": data.get("error", {}).get("description", str(data))}
            return data
        except Exception as e:
            logger.exception("Razorpay Payment Link API error: %s", e)
            return {"error": str(e)}

    def make_qr_image(self, text: str):
        qr = qrcode.QRCode(version=None, box_size=10, border=4)
        qr.add_data(text)
        qr.make(fit=True)
        image = qr.make_image()
        buf = io.BytesIO()
        image.save(buf, format="PNG")
        buf.seek(0)
        return buf

    async def generate_upi_payment(self, update, amount, context):
        user = update.effective_user
        if not user:
            return
        if amount < 1:
            await self.render(update, context, text=f"{pe('ℹ️')} Invalid amount.")
            return
        if not self.razorpay_ready():
            await self.render(update, context, text=f"{pe('ℹ️')} <b>Razorpay is not configured.</b>\n\nAdmin: set the Razorpay environment variables and restart the bot.", reply_markup=self.menu_kb())
            return

        txn_id = f"TXN{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}"
        qr = self.create_razorpay_payment_link(amount, txn_id, user.id)
        if not qr or "id" not in qr or not qr.get("short_url"):
            error = qr.get("error", "Unknown error") if isinstance(qr, dict) else "Unknown error"
            await self.render(update, context, text=f"{pe('ℹ️')} Could not create Razorpay QR.\n\n<code>{error}</code>", reply_markup=self.menu_kb())
            return

        await self.store.add_razorpay_pending(txn_id, {
            "user_id": user.id, "name": user.first_name, "username": user.username or "",
            "amount": float(amount), "payment_link_id": qr["id"], "razorpay_payment_link_url": qr["short_url"],
        })
        context.user_data['pending_payment'] = {'amount': amount, 'txn_id': txn_id, 'payment_link_id': qr['id']}

        try:
            img_bytes = self.make_qr_image(qr["short_url"])
            caption = f"""{pe('💳')} <b>Razorpay Payment</b>

{pe('💰')} Amount: ₹{amount}
{pe('🆔')} TXN ID: <code>{txn_id}</code>

{pe('📱')} Scan this QR with GPay / PhonePe / Paytm.
{pe('⚡️')} QR opens Razorpay's secure UPI payment page.
{pe('✅')} After successful payment, balance is credited automatically.
{pe('ℹ️')} No screenshot or UTR is required."""
            pay_button = InlineKeyboardButton("Pay / Open Link", url=qr["short_url"])
            await self.render(update, context, photo=img_bytes, caption=caption, reply_markup=InlineKeyboardMarkup([[pay_button], [btn("Main Menu", "menu", emoji="🔑")]]))
            # Also show the actual short URL as text so the user can open it if scanning is inconvenient.
            await self.render(update, context, text=f"{pe('🔗')} <b>Payment Link</b>\n\n{qr['short_url']}", reply_markup=self.menu_kb())
        except Exception as e:
            logger.exception("Could not generate/send Payment Link QR: %s", e)
            await self.render(update, context, text=f"{pe('💳')} <b>Razorpay Payment</b>\n\n{pe('💰')} Amount: ₹{amount}\n{pe('🆔')} TXN ID: <code>{txn_id}</code>\n\n{pe('🔗')} Payment Link: {qr['short_url']}", reply_markup=self.menu_kb())

    async def handle_razorpay_webhook(self, payload: dict):
        if payload.get("event") != "payment_link.paid":
            return

        pl = payload.get("payload", {}).get("payment_link", {}).get("entity", {})
        payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
        payment_id = payment.get("id") or payload.get("id") or f"plink:{payment_link_id}"
        payment_link_id = pl.get("id") or ""
        amount_paise = int(payment.get("amount", 0) or pl.get("amount_paid", 0) or 0)
        status = payment.get("status") or "captured"
        currency = payment.get("currency") or pl.get("currency") or "INR"

        if not payment_link_id:
            logger.warning("payment_link.paid received without payment_link id")
            return
        if currency not in ("INR", ""):
            logger.warning("Ignoring non-INR payment link: %s", payment_link_id)
            return
        if payment.get("id") and status not in ("captured", ""):
            logger.warning("Ignoring payment link with payment status %s: %s", status, payment_link_id)
            return

        info, credit_status = await self.store.credit_razorpay_payment(
            payment_link_id, payment_id, amount_paise
        )
        if credit_status == "already_processed":
            logger.info("Ignoring duplicate Razorpay payment webhook: %s", payment_id)
            return
        if credit_status != "credited":
            logger.warning("Razorpay payment %s not credited: %s", payment_id, credit_status)
            return

        uid, amount = int(info["user_id"]), float(info["amount"])
        new_balance, txn_id = float(info["new_balance"]), info.get("_id", "")

        try:
            await self.app.bot.send_message(
                chat_id=uid,
                text=(
                    f"{pe('✅')} <b>Payment Successful!</b>\n\n"
                    f"{pe('💰')} ₹{amount:.2f} added to your balance.\n"
                    f"{pe('🆔')} Payment ID: <code>{payment_id}</code>\n"
                    f"{pe('📌')} TXN: <code>{txn_id}</code>\n"
                    f"{pe('💰')} New Balance: ₹{new_balance:.2f}"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.error("Could not notify user after Razorpay payment %s: %s", payment_id, exc)

        try:
            await self.app.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"{pe('✅')} <b>Razorpay Payment Credited</b>\n\n"
                    f"{pe('👤')} User ID: <code>{uid}</code>\n"
                    f"{pe('💰')} Amount: ₹{amount:.2f}\n"
                    f"{pe('🆔')} Payment: <code>{payment_id}</code>\n"
                    f"{pe('📌')} TXN: <code>{txn_id}</code>\n"
                    f"{pe('🔗')} Payment Link: <code>{payment_link_id}</code>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.error("Could not notify admin after Razorpay payment %s: %s", payment_id, exc)

    async def paid_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """User claims they've paid -> ask for screenshot, then UTR, before it ever reaches admin."""
        query = update.callback_query
        await query.answer()
        txn_id = query.data.split("_", 1)[1]
        pending = context.user_data.get('pending_payment', {})

        if pending.get('txn_id') != txn_id:
            await self.render(update, context, text=f"{pe('ℹ️')} Payment session expired. Please start again with /payment.", reply_markup=self.menu_kb())
            return

        context.user_data['awaiting_screenshot_txn'] = txn_id
        await self.render(
            update, context,
            text=f"{pe('📱')} <b>Send Payment Screenshot</b>\n\n{pe('ℹ️')} Please send a screenshot photo of the payment for TXN <code>{txn_id}</code>.")

    async def handle_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        txn_id = context.user_data.get('awaiting_screenshot_txn')
        if not txn_id:
            return  # not in a payment flow, ignore stray photos

        file_id = update.message.photo[-1].file_id
        context.user_data['screenshot_file_id'] = file_id
        context.user_data.pop('awaiting_screenshot_txn', None)
        context.user_data['awaiting_utr_txn'] = txn_id

        await self.render(
            update, context,
            text=f"{pe('✅')} Screenshot received.\n\n{pe('ℹ️')} Now send the <b>UTR / Transaction Reference ID</b> from your payment app:")

    async def handle_utr_text(self, update, context, txn_id):
        pending = context.user_data.get('pending_payment', {})
        amount = pending.get('amount', 0)
        screenshot_file_id = context.user_data.pop('screenshot_file_id', None)
        utr = update.message.text.strip()
        context.user_data.pop('awaiting_utr_txn', None)

        user = update.effective_user
        await self.store.add_pending(txn_id, {
            "user_id": user.id, "name": user.first_name, "username": user.username or "",
            "amount": amount, "utr": utr, "screenshot_file_id": screenshot_file_id,
        })

        # Notify user
        await self.render(
            update, context,
            text=f"{pe('⏱️')} <b>Sent for Verification</b>\n\n{pe('ℹ️')} Your payment (₹{amount}, TXN <code>{txn_id}</code>) is awaiting admin approval. You'll be notified once it's confirmed.",
            reply_markup=self.menu_kb())

        # Notify admin (separate chat — admin must have started the bot at least once)
        admin_text = f"""{pe('📤')} <b>New Payment Verification</b>

{pe('👤')} User: {user.first_name} (@{user.username or 'no_username'})
{pe('🆔')} User ID: <code>{user.id}</code>
{pe('💰')} Amount: ₹{amount}
{pe('🆔')} TXN: <code>{txn_id}</code>
{pe('📝')} UTR: <code>{utr}</code>"""

        admin_kb = InlineKeyboardMarkup(grid([
            btn("Approve", f"apprv_{txn_id}", emoji="✅", style="success"),
            btn("Reject", f"rejct_{txn_id}", emoji="🛡️", style="danger"),
        ], cols=2))

        try:
            if screenshot_file_id:
                await context.bot.send_photo(chat_id=ADMIN_ID, photo=screenshot_file_id,
                                              caption=admin_text, reply_markup=admin_kb, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text,
                                                reply_markup=admin_kb, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Could not notify admin (has the admin started the bot?): {e}")

    async def admin_decision_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user = update.effective_user

        if not user or user.id != ADMIN_ID:
            await query.answer("Only the admin can do this.", show_alert=True)
            return
        await query.answer()

        action, txn_id = query.data.split("_", 1)
        info = await self.store.pop_pending(txn_id)

        if not info:
            if query.message.photo:
                await query.edit_message_caption(caption=f"{pe('ℹ️')} Already handled or expired.", parse_mode=ParseMode.HTML)
            else:
                await query.edit_message_text(f"{pe('ℹ️')} Already handled or expired.", parse_mode=ParseMode.HTML)
            return

        target_uid = info["user_id"]
        amount = info["amount"]

        if action == "apprv":
            new_balance = await self.store.add_balance(target_uid, amount, name=info.get("name"))
            result_text = f"{pe('✅')} <b>Approved</b> — ₹{amount} added.\nUser new balance: ₹{new_balance:.2f}"
            try:
                await context.bot.send_message(
                    chat_id=target_uid,
                    text=f"{pe('✅')} <b>Payment Approved!</b>\n\n{pe('💰')} ₹{amount} added to your balance.\n{pe('📊')} Use /balance to check.",
                    parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Could not notify user of approval: {e}")
        else:
            result_text = f"{pe('🛡️')} <b>Rejected</b> — no balance added."
            try:
                await context.bot.send_message(
                    chat_id=target_uid,
                    text=f"{pe('ℹ️')} <b>Payment Rejected</b>\n\n{pe('ℹ️')} TXN <code>{txn_id}</code> could not be verified. Contact support if this is a mistake.",
                    parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Could not notify user of rejection: {e}")

        if query.message.photo:
            await query.edit_message_caption(caption=(query.message.caption or "") + f"\n\n{result_text}", parse_mode=ParseMode.HTML, reply_markup=None)
        else:
            await query.edit_message_text((query.message.text or "") + f"\n\n{result_text}", parse_mode=ParseMode.HTML, reply_markup=None)

    # ==================== ORDER (wallet-gated, markup-aware) ====================

    async def order_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data['order_step'] = 'service_id'
        await self.render(update, context, text=f"{pe('📦')} <b>New Order</b>\n\nEnter Service ID (use /services to find):")

    async def order_confirm_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        pending = context.user_data.get('order_confirm')
        user = update.effective_user

        if not pending:
            await self.render(update, context, text=f"{pe('ℹ️')} Nothing to confirm — start with /order.", reply_markup=self.menu_kb())
            return

        cost = pending['cost']
        if not await self.store.deduct_balance(user.id, cost):
            await self.render(update, context, text=f"{pe('ℹ️')} Insufficient balance — please add balance first.", reply_markup=self.menu_kb())
            context.user_data.pop('order_confirm', None)
            return

        await self.render(update, context, text=f"{pe('⏱️')} Placing order...")
        result = self.api.add_order(pending['service_id'], pending['link'], pending['quantity'])

        if result and 'order' in result:
            new_balance = await self.store.get_balance(user.id)
            text = f"""{pe('✅')} <b>Order Placed!</b>

{pe('🆔')} Order ID: <code>{result['order']}</code>
{pe('📌')} Service: {pending['service_id']} ({pending['category']})
{pe('📊')} Quantity: {pending['quantity']}
{pe('💰')} Charged: ₹{cost:.2f}
{pe('💰')} Balance left: ₹{new_balance:.2f}"""

            await self.post_to_channel(
                context,
                f"""{pe('📦')} <b>New Order!</b>

{pe('👤')} {user.first_name} (@{user.username or 'no_username'}) just bought:
{pe('📌')} {pending['category']} — Service #{pending['service_id']}
{pe('📊')} Quantity: {pending['quantity']}""")
        else:
            # API failed after we deducted -> refund
            await self.store.add_balance(user.id, cost)
            error = result.get('error', 'Unknown error') if isinstance(result, dict) else 'Unknown error'
            text = f"{pe('ℹ️')} Order failed: {error}\n{pe('ℹ️')} Amount refunded to your balance."

            balance_issue = "balance" in str(error).lower()
            admin_alert = f"""{pe('ℹ️')} <b>Order Failed</b>{' — ⚠️ looks like the panel balance is low!' if balance_issue else ''}

{pe('👤')} User: {user.first_name} (@{user.username or 'no_username'}) — <code>{user.id}</code>
{pe('🆔')} Service: {pending['service_id']} ({pending['category']})
{pe('📊')} Quantity: {pending['quantity']}
{pe('💰')} Amount (refunded to user): ₹{cost:.2f}
{pe('ℹ️')} Panel error: {error}"""
            try:
                await context.bot.send_message(chat_id=ADMIN_ID, text=admin_alert, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Could not notify admin of order failure: {e}")

        context.user_data.pop('order_confirm', None)
        await self.render(update, context, text=text, reply_markup=self.menu_kb())

    async def order_cancel_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        context.user_data.pop('order_confirm', None)
        await self.render(update, context, text=f"{pe('ℹ️')} Order cancelled.", reply_markup=self.menu_kb())

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data['status_step'] = True
        await self.render(update, context, text=f"{pe('📈')} <b>Order Status</b>\n\nEnter Order ID(s) (comma separated):")

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data['cancel_step'] = True
        await self.render(update, context, text=f"{pe('🛡️')} <b>Cancel Orders</b>\n\nEnter Order IDs to cancel (comma separated):")

    async def refill_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data['refill_step'] = True
        await self.render(update, context, text=f"{pe('📤')} <b>Create Refill</b>\n\nEnter Order IDs for refill (comma separated):")

    async def refill_status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data['refill_status_step'] = True
        await self.render(update, context, text=f"{pe('📝')} <b>Refill Status</b>\n\nEnter Refill IDs (comma separated):")

    # ==================== MESSAGE HANDLER ====================

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_input = update.message.text.strip()

        # ---- Custom payment amount ----
        if context.user_data.get('payment_step') == 'custom':
            try:
                amount = int(user_input)
                if amount < 10:
                    await self.render(update, context, text=f"{pe('ℹ️')} Minimum amount is ₹10.")
                    return
                await self.generate_upi_payment(update, amount, context)
                context.user_data.pop('payment_step', None)
            except ValueError:
                await self.render(update, context, text=f"{pe('ℹ️')} Please enter a valid number.")
            return

        # ---- Order flow ----
        if context.user_data.get('order_step') == 'service_id':
            service = await self.find_service(context, user_input)
            if not service:
                await self.render(update, context, text=f"{pe('ℹ️')} Service ID not found. Enter a valid Service ID (use /services to browse):")
                return
            context.user_data['order_service'] = service
            context.user_data['order_step'] = 'link'
            admin = self.is_admin(update)
            rate_line = (f"Rate/1000: {service.get('rate','N/A')}" if admin
                         else f"Rate/1000: ₹{display_rate_per_1000(float(service.get('rate', 0) or 0), TIER1_MAX_QTY):.2f}")
            await self.render(
                update, context,
                text=f"{pe('📦')} <b>{service.get('name','Unknown')[:60]}</b>\n"
                     f"{pe('💰')} {rate_line} | Min: {service.get('min','N/A')} | Max: {service.get('max','N/A')}\n\n"
                     f"Enter the Link/URL:")
            return

        if context.user_data.get('order_step') == 'link':
            context.user_data['order_link'] = user_input
            context.user_data['order_step'] = 'quantity'
            await self.render(update, context, text=f"{pe('📦')} Enter the Quantity:")
            return

        if context.user_data.get('order_step') == 'quantity':
            try:
                quantity = int(user_input)
            except ValueError:
                await self.render(update, context, text=f"{pe('ℹ️')} Enter a valid quantity (number).")
                return

            service = context.user_data.get('order_service', {})
            link = context.user_data.get('order_link')
            real_rate = float(service.get('rate', 0) or 0)
            admin = self.is_admin(update)
            category = self.categorize(service)
            user_id = update.effective_user.id
            balance = await self.store.get_balance(user_id)

            discount_line = ""
            if admin:
                cost = round(real_rate / 1000 * quantity, 2)
            else:
                cost, base_rate, tiered_rate, discounted, savings_pct = compute_cost(real_rate, quantity)
                if discounted:
                    discount_line = f"\n{pe('🔥')} Bulk discount applied — {savings_pct}% cheaper than list rate!"

            for k in ('order_step', 'order_service', 'order_link'):
                context.user_data.pop(k, None)

            if balance < cost:
                text = f"""{pe('ℹ️')} <b>Insufficient Balance</b>

{pe('📌')} Category: {category}
{pe('🆔')} Service ID: {service.get('service')}
{pe('📊')} Quantity: {quantity}
{pe('💰')} Cost: ₹{cost:.2f}
{pe('💰')} Your Balance: ₹{balance:.2f}
{pe('ℹ️')} Short by: ₹{cost - balance:.2f}"""
                keyboard = InlineKeyboardMarkup(grid([
                    btn("Add Balance", "pay", emoji="💳", style="success"),
                    btn("Main Menu", "menu", emoji="🔑"),
                ], cols=2))
                await self.render(update, context, text=text, reply_markup=keyboard)
                return

            context.user_data['order_confirm'] = {
                'service_id': service.get('service'), 'link': link,
                'quantity': quantity, 'cost': cost, 'category': category,
            }

            text = f"""{pe('📌')} <b>Confirm Order</b>

{pe('📌')} Category: {category}
{pe('🆔')} Service ID: {service.get('service')}
{pe('📊')} Quantity: {quantity}
{pe('💰')} Cost: ₹{cost:.2f}
{pe('💰')} Your Balance: ₹{balance:.2f} → ₹{balance - cost:.2f}{discount_line}"""
            keyboard = InlineKeyboardMarkup(grid([
                btn("Confirm", "ordconfirm", emoji="✅", style="success"),
                btn("Cancel", "ordcancel", emoji="🛡️", style="danger"),
            ], cols=2))
            await self.render(update, context, text=text, reply_markup=keyboard)
            return

        # ---- Status flow ----
        if context.user_data.get('status_step'):
            context.user_data.pop('status_step', None)
            order_ids = [x.strip() for x in user_input.split(",") if x.strip()]
            await self.render(update, context, text=f"{pe('⏱️')} Checking status...")

            results = ({order_ids[0]: self.api.get_order_status(order_ids[0])} if len(order_ids) == 1
                       else self.api.get_multiple_status(order_ids)) or {}

            text = f"{pe('📈')} <b>Order Status</b>\n\n"
            for oid, info in results.items():
                if isinstance(info, dict) and 'error' not in info:
                    text += (f"{pe('🆔')} <b>{oid}</b>\n   Status: {info.get('status', 'N/A')}\n"
                             f"   Charge: {info.get('charge', 'N/A')} | Start: {info.get('start_count', 'N/A')} | Remains: {info.get('remains', 'N/A')}\n\n")
                else:
                    text += f"{pe('ℹ️')} <b>{oid}</b>: {info.get('error', 'Unknown error') if isinstance(info, dict) else 'Unknown error'}\n\n"

            await self.render(update, context, text=text, reply_markup=self.menu_kb())
            return

        # ---- Cancel flow ----
        if context.user_data.get('cancel_step'):
            context.user_data.pop('cancel_step', None)
            order_ids = [x.strip() for x in user_input.split(",") if x.strip()]
            await self.render(update, context, text=f"{pe('⏱️')} Cancelling...")
            result = self.api.cancel_orders(order_ids)

            if not result or (isinstance(result, dict) and 'error' in result):
                text = f"{pe('ℹ️')} Failed: {result.get('error', 'Unknown error') if isinstance(result, dict) else 'Unknown error'}"
            else:
                text = f"{pe('🛡️')} <b>Cancel Requested</b>\n\n"
                if isinstance(result, list):
                    for r in result:
                        text += f"{pe('🆔')} {r.get('order', 'N/A')}: {r.get('cancel', {}).get('status', 'N/A')}\n"
                else:
                    text += str(result)
            await self.render(update, context, text=text, reply_markup=self.menu_kb())
            return

        # ---- Refill flow ----
        if context.user_data.get('refill_step'):
            context.user_data.pop('refill_step', None)
            order_ids = [x.strip() for x in user_input.split(",") if x.strip()]
            await self.render(update, context, text=f"{pe('📤')} Creating refill...")
            result = self.api.create_refill(order_ids[0]) if len(order_ids) == 1 else self.api.create_multiple_refill(order_ids)
            text = (f"{pe('✅')} Refill request created:\n<code>{result}</code>"
                    if result and not (isinstance(result, dict) and 'error' in result)
                    else f"{pe('ℹ️')} Failed: {result.get('error', 'Unknown error') if isinstance(result, dict) else 'Unknown error'}")
            await self.render(update, context, text=text, reply_markup=self.menu_kb())
            return

        # ---- Refill status flow ----
        if context.user_data.get('refill_status_step'):
            context.user_data.pop('refill_status_step', None)
            refill_ids = [x.strip() for x in user_input.split(",") if x.strip()]
            await self.render(update, context, text=f"{pe('⏱️')} Checking refill status...")
            result = self.api.get_refill_status(refill_ids[0]) if len(refill_ids) == 1 else self.api.get_multiple_refill_status(refill_ids)
            text = (f"{pe('📝')} Refill status:\n<code>{result}</code>"
                    if result and not (isinstance(result, dict) and 'error' in result)
                    else f"{pe('ℹ️')} Failed: {result.get('error', 'Unknown error') if isinstance(result, dict) else 'Unknown error'}")
            await self.render(update, context, text=text, reply_markup=self.menu_kb())
            return

        # ---- No active flow ----
        await self.render(update, context, text=f"{pe('ℹ️')} Use /start to see the main menu", reply_markup=self.menu_kb())

    # ==================== BUTTON CALLBACKS ====================

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        handlers = {
            "menu": self.start_command, "bal": self.balance_command, "serv": self.services_command,
            "ord": self.order_command, "pay": self.payment_command, "stat": self.status_command,
            "canc": self.cancel_command, "ref": self.refill_command, "refstat": self.refill_status_command,
        }
        await handlers[data](update, context)

    async def categories_page_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        await self.render_categories_page(update, context, int(query.data.split("_", 1)[1]))

    async def category_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        await self.show_category_services(update, context, int(query.data.split("_", 1)[1]), 0)

    async def page_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        _, cat_index, page = query.data.split("_")
        await self.show_category_services(update, context, int(cat_index), int(page))

    # ==================== RUN ====================
    async def _post_init(self, app):
        global BOT_INSTANCE, BOT_LOOP
        BOT_INSTANCE = self
        BOT_LOOP = asyncio.get_running_loop()
        await self.store.ensure_indexes()

    def run(self):
        required = {
            "BOT_TOKEN": BOT_TOKEN,
            "SMM_API_KEY": API_KEY,
            "ADMIN_ID": str(ADMIN_ID) if ADMIN_ID else "",
            "MONGO_URI": MONGO_URI,
            "RAZORPAY_KEY_ID": RAZORPAY_KEY_ID,
            "RAZORPAY_KEY_SECRET": RAZORPAY_KEY_SECRET,
            "RAZORPAY_WEBHOOK_SECRET": RAZORPAY_WEBHOOK_SECRET,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError("Missing required environment variables: " + ", ".join(missing))
        if len(RAZORPAY_WEBHOOK_SECRET) < 32:
            raise RuntimeError("RAZORPAY_WEBHOOK_SECRET is too short; use a strong random secret of at least 32 characters.")

        self.app.post_init = self._post_init
        start_health_server(PORT)
        print(f"""
+===================================================+
|  DEMON SMM BOT v11.0 - NO-DELETE UX + LIVE ALERTS  |
|  Status: RUNNING (polling)                         |
|  Health check: http://0.0.0.0:{PORT}/  -> "Bot is running"
+===================================================+
        """)
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    bot = SMMBot(BOT_TOKEN, API_KEY)
    bot.run()
