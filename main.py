import asyncio
import os
import random
import contextlib
from typing import List, Tuple, Optional
import sqlite3
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram import Router
import aiohttp
from dotenv import load_dotenv
from aiogram.client.default import DefaultBotProperties


load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "bot.db"))


router = Router()

CHANNELS = [
    "😎СИСЕЧКИ.COM",
    "ФДЫЛВФОЫВЛДФВФВ",
    "ФЖЫДВЛФЖДВ",
]

CHANNEL_LINKS = [
    "https://t.me/+NuiRvBPgaswxZDRi",
    "https://t.me/your_channel2",
    "https://t.me/your_channel3",
]

CHANNEL_IDS = [
    -1003494454633,
    "@your_channel2",
    "@your_channel3",
]

PLANS = {
    "week": {"title": "Неделя", "amount": 0.15, "delta": timedelta(days=7)},
    "month": {"title": "Месяц", "amount": 5.0, "delta": timedelta(days=30)},
    "lifetime": {"title": "Навсегда", "amount": 15.0, "delta": None},
}

reply_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📈 Тарифы"), KeyboardButton(text="📊 Подписка")]],
    resize_keyboard=True,
)

PENDING: dict[int, dict] = {}

def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS invoices (
                invoice_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                channel_idx INTEGER NOT NULL,
                plan TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                payload TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_idx INTEGER NOT NULL,
                expires_at INTEGER NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_idx INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ui_messages (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            )
            """
        )
        con.commit()
    finally:
        con.close()


def upsert_user(user: Message):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              username=excluded.username,
              first_name=excluded.first_name,
              last_name=excluded.last_name
            """,
            (
                user.from_user.id if user.from_user else 0,
                user.from_user.username if user.from_user else None,
                user.from_user.first_name if user.from_user else None,
                user.from_user.last_name if user.from_user else None,
            ),
        )
        con.commit()
    finally:
        con.close()


def create_invoice_record(invoice_id: int, user_id: int, channel_idx: int, plan: str, amount: float, payload: str):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO invoices (invoice_id, user_id, channel_idx, plan, amount, status, payload, created_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (invoice_id, user_id, channel_idx, plan, amount, payload, int(datetime.utcnow().timestamp())),
        )
        con.commit()
    finally:
        con.close()


def mark_invoice_paid(invoice_id: int):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute("UPDATE invoices SET status='paid' WHERE invoice_id=?", (invoice_id,))
        con.commit()
    finally:
        con.close()


def grant_subscription(user_id: int, channel_idx: int, plan: str):
    now = datetime.utcnow()
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT id, expires_at FROM subscriptions WHERE user_id=? AND channel_idx=? ORDER BY id DESC LIMIT 1",
            (user_id, channel_idx),
        )
        row = cur.fetchone()
        if PLANS[plan]["delta"] is None:
            expires_at = None
        else:
            delta = PLANS[plan]["delta"]
            base = now
            if row and row[1]:
                existing = datetime.utcfromtimestamp(row[1])
                if existing > now:
                    base = existing
            expires_at_dt = base + delta
            expires_at = int(expires_at_dt.timestamp())
        cur.execute(
            "INSERT INTO subscriptions (user_id, channel_idx, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (user_id, channel_idx, expires_at, int(now.timestamp())),
        )
        con.commit()
        print(f"Granted subscription: user={user_id}, channel={channel_idx}, plan={plan}, expires_at={expires_at}")
    finally:
        con.close()


def get_active_subscriptions(user_id: int) -> List[int]:
    now_ts = int(datetime.utcnow().timestamp())
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT DISTINCT channel_idx FROM subscriptions
            WHERE user_id=? AND (expires_at IS NULL OR expires_at > ?)
            """,
            (user_id, now_ts),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        con.close()


def add_notice(user_id: int, channel_idx: int, chat_id: int, message_id: int):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO notices (user_id, channel_idx, chat_id, message_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, channel_idx, chat_id, message_id, int(datetime.utcnow().timestamp())),
        )
        con.commit()
    finally:
        con.close()


def get_notices_for_user_channel(user_id: int, channel_idx: int) -> list:
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT chat_id, message_id FROM notices WHERE user_id=? AND channel_idx=?",
            (user_id, channel_idx),
        )
        return cur.fetchall()
    finally:
        con.close()


def clear_notices_for_user_channel(user_id: int, channel_idx: int):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            "DELETE FROM notices WHERE user_id=? AND channel_idx=?",
            (user_id, channel_idx),
        )
        con.commit()
    finally:
        con.close()

def get_anchor(user_id: int, chat_id: int) -> Optional[int]:
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT message_id FROM ui_messages WHERE user_id=? AND chat_id=?",
            (user_id, chat_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        con.close()


def set_anchor(user_id: int, chat_id: int, message_id: int):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO ui_messages(user_id, chat_id, message_id)
            VALUES(?,?,?)
            ON CONFLICT(user_id, chat_id) DO UPDATE SET message_id=excluded.message_id
            """,
            (user_id, chat_id, message_id),
        )
        con.commit()
    finally:
        con.close()


def clear_anchor(user_id: int, chat_id: int):
    con = get_db()
    try:
        cur = con.cursor()
        cur.execute(
            "DELETE FROM ui_messages WHERE user_id=? AND chat_id=?",
            (user_id, chat_id),
        )
        con.commit()
    finally:
        con.close()


async def edit_or_send_anchor(bot: Bot, user_id: int, chat_id: int, text: str, kb: Optional[InlineKeyboardMarkup]):
    try:
        msg_id = get_anchor(user_id, chat_id)
        if msg_id is not None:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=text,
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
                return
            except Exception as e:
                error_text = str(e).lower()
                if "message is not modified" in error_text or "message content and reply markup are exactly the same" in error_text:
                    print("Message content is the same, ignoring")
                    return
                print(f"Edit failed: {e}, sending new message")
        sent = await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
        set_anchor(user_id, chat_id, sent.message_id)
    except Exception as e:
        print(f"Error in edit_or_send_anchor: {e}")
        raise


def build_channels_kb() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=f"📡 {title}", callback_data=f"channel:{idx}")]
        for idx, title in enumerate(CHANNELS)
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_plans_kb(channel_idx: int) -> InlineKeyboardMarkup:
    inline_rows = []
    for plan_key, meta in PLANS.items():
        title = meta["title"]
        amount = meta["amount"]
        cb = f"buy:{channel_idx}:{plan_key}"
        inline_rows.append([InlineKeyboardButton(text=f"🛒 {title} — {amount} USDT", callback_data=cb)])
    inline_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:channels")])
    return InlineKeyboardMarkup(inline_keyboard=inline_rows)


def build_tariffs_kb(section_idx: int, tariffs: List[Tuple[str, float]]) -> InlineKeyboardMarkup:
    inline_rows = []
    for i, (name, price) in enumerate(tariffs):
        cb = f"buy:{section_idx}:{i}:{price}"
        inline_rows.append([InlineKeyboardButton(text=f"{name} — {price} USDT", callback_data=cb)])
    inline_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:sections")])
    return InlineKeyboardMarkup(inline_keyboard=inline_rows)


async def create_crypto_invoice(session: aiohttp.ClientSession, amount: float, description: str, payload: str) -> Tuple[int, str]:
    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    json = {
        "asset": "USDT",
        "amount": str(amount),
        "description": description,
        "payload": payload,
        "allow_comments": False,
        "allow_anonymous": True,
    }
    async with session.post(url, headers=headers, json=json) as resp:
        data = await resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"CryptoBot API error: {data}")
        result = data["result"]
        invoice_id = result.get("invoice_id")
        pay_url = result.get("pay_url") or result.get("invoice_url")
        if not invoice_id or not pay_url:
            raise RuntimeError("Некорректный ответ CryptoBot: нет invoice_id или pay_url")
        return invoice_id, pay_url


async def get_invoice_status(session: aiohttp.ClientSession, invoice_id: int) -> dict:
    url = "https://pay.crypt.bot/api/getInvoices"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload = {"invoice_ids": [invoice_id]}
    async with session.post(url, headers=headers, json=payload) as resp:
        data = await resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"CryptoBot API error: {data}")
        items = data["result"].get("items", [])
        return items[0] if items else {}


@router.message(CommandStart())
async def cmd_start(message: Message):
    try:
        print(f"Start command from user {message.from_user.id}")
        try:
            await message.delete()
        except Exception:
            pass
        upsert_user(message)
        clear_anchor(message.from_user.id, message.chat.id)
        print("Anchor cleared")
        await message.answer("Добро пожаловать! Используйте меню ниже.", reply_markup=reply_kb)
        print("Welcome message sent")
        await edit_or_send_anchor(message.bot, message.from_user.id, message.chat.id, "Выберите канал:", build_channels_kb())
        print("Anchor message sent")
    except Exception as e:
        print(f"Error in cmd_start: {e}")
        import traceback
        traceback.print_exc()
        await message.answer(f"Ошибка: {e}")


@router.message(F.text == "📈 Тарифы")
async def on_tariffs_btn(message: Message):
    try:
        await message.delete()
    except Exception:
        pass
    await edit_or_send_anchor(message.bot, message.from_user.id, message.chat.id, "Каналы:", build_channels_kb())


@router.message(F.text == "📊 Подписка")
async def on_subscription_btn(message: Message):
    try:
        await message.delete()
    except Exception:
        pass
    user_id = message.from_user.id if message.from_user else 0
    channels = set(get_active_subscriptions(user_id))
    if channels:
        inline_rows = []
        for ch_idx in sorted(channels):
            title = CHANNELS[ch_idx]
            url = CHANNEL_LINKS[ch_idx] if ch_idx < len(CHANNEL_LINKS) else None
            if url:
                inline_rows.append([InlineKeyboardButton(text=title, url=url)])
        kb = InlineKeyboardMarkup(inline_keyboard=inline_rows) if inline_rows else None
        await edit_or_send_anchor(message.bot, user_id, message.chat.id, "Ваши активные подписки:", kb)
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Показать список тарифов", callback_data="show_tariffs")]]
        )
        await edit_or_send_anchor(message.bot, user_id, message.chat.id, "Активных подписок нет. Оформите любой тариф.", kb)


@router.callback_query(F.data == "show_tariffs")
async def on_show_tariffs(callback: CallbackQuery):
    await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, "Каналы:", build_channels_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("channel:"))
async def on_channel_choice(callback: CallbackQuery):
    channel_idx = int(callback.data.split(":")[1])
    await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, f"{CHANNELS[channel_idx]} — выберите тариф:", build_plans_kb(channel_idx))
    await callback.answer()


@router.callback_query(F.data == "back:channels")
async def on_back_channels(callback: CallbackQuery):
    await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, "Каналы:", build_channels_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("buy:"))
async def on_buy_tariff(callback: CallbackQuery):
    try:
        _, channel_idx_str, plan_key = callback.data.split(":")
        channel_idx = int(channel_idx_str)
        plan = PLANS[plan_key]
    except Exception:
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return

    amount = plan["amount"]
    title = CHANNELS[channel_idx]
    description = f"Оплата тарифа '{plan['title']}' для '{title}'"
    payload = f"user={callback.from_user.id};channel={channel_idx};plan={plan_key};amount={amount}" 

    if not CRYPTO_PAY_TOKEN:
        await callback.answer("CRYPTO_PAY_TOKEN не задан", show_alert=True)
        return

    await callback.answer()
    await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, "Генерирую ссылку на оплату…", None)
    try:
        async with aiohttp.ClientSession() as session:
            invoice_id, pay_url = await create_crypto_invoice(session, amount, description, payload)
            PENDING[invoice_id] = {
                "user_id": callback.from_user.id,
                "chat_id": callback.message.chat.id,
                "section_idx": channel_idx,
                "amount": amount,
                "plan": plan_key,
            }
            create_invoice_record(invoice_id, callback.from_user.id, channel_idx, plan_key, amount, payload)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Оплатить в CryptoBot", url=pay_url)],
                    [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check:{invoice_id}")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:channels")],
                ]
            )
            await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, f"Счёт: {amount} USDT", kb)
    except Exception as e:
        await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, f"Не удалось создать счет: {e}", None)


@router.callback_query(F.data.startswith("check:"))
async def on_check_invoice(callback: CallbackQuery):
    try:
        _, invoice_id_str = callback.data.split(":")
        invoice_id = int(invoice_id_str)
    except Exception:
        await callback.answer("Некорректный запрос", show_alert=True)
        return

    if not CRYPTO_PAY_TOKEN:
        await callback.answer("CRYPTO_PAY_TOKEN не задан", show_alert=True)
        return

    await callback.answer()

    try:
        async with aiohttp.ClientSession() as session:
            inv = await get_invoice_status(session, invoice_id)
        if not inv:
            await callback.message.answer("Счёт не найден. Попробуйте позже.")
            return
        status = inv.get("status")
        if status == "paid":
            meta = PENDING.pop(invoice_id, None)
            ch = None
            if meta:
                uid = meta["user_id"]
                ch = int(meta.get("section_idx", 0))
                plan_key = meta.get("plan")
                mark_invoice_paid(invoice_id)
                grant_subscription(uid, ch, plan_key)
            else:
                payload_str = inv.get("payload") or ""
                try:
                    for part in str(payload_str).split(";"):
                        k, v = part.split("=", 1)
                        if k == "channel":
                            ch = int(v)
                            break
                except Exception:
                    ch = None
            if isinstance(ch, int) and 0 <= ch < len(CHANNEL_LINKS):
                link = CHANNEL_LINKS[ch]
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Перейти в канал", url=link)]])
                try:
                    chat_ident = CHANNEL_IDS[ch] if ch < len(CHANNEL_IDS) else None
                    if chat_ident:
                        await callback.message.bot.unban_chat_member(chat_ident, uid)
                except Exception:
                    pass
                await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, "Оплачено ✅ Доступ выдан.", kb)
            else:
                await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, "Оплачено ✅ Доступ выдан.", None)
        elif status in {"active", "pending"}:
            await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, "Счёт пока не оплачен. Попробуйте позже.", None)
        else:
            await edit_or_send_anchor(callback.message.bot, callback.from_user.id, callback.message.chat.id, f"Статус счёта: {status}", None)
    except Exception as e:
        await callback.message.answer(f"Ошибка проверки: {e}")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Установите переменную окружения BOT_TOKEN.")

    init_db()
    dp = Dispatcher()
    dp.include_router(router)

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=True)

    async def poll_invoices_task():
        if not CRYPTO_PAY_TOKEN:
            return
        while True:
            try:
                if PENDING:
                    ids = list(PENDING.keys())
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            "https://pay.crypt.bot/api/getInvoices",
                            headers={"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN},
                            json={"invoice_ids": ids},
                        ) as resp:
                            data = await resp.json()
                            if data.get("ok"):
                                for it in data["result"].get("items", []):
                                    if it.get("status") == "paid":
                                        inv_id = it.get("invoice_id")
                                        meta = PENDING.pop(inv_id, None)
                                        if meta:
                                            uid = meta["user_id"]
                                            ch = int(meta.get("section_idx", 0))
                                            plan_key = meta.get("plan")
                                            mark_invoice_paid(inv_id)
                                            grant_subscription(uid, ch, plan_key)
                                            try:
                                                link = CHANNEL_LINKS[ch] if ch < len(CHANNEL_LINKS) else None
                                                if link:
                                                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Перейти в канал", url=link)]])
                                                    try:
                                                        chat_ident = CHANNEL_IDS[ch] if ch < len(CHANNEL_IDS) else None
                                                        if chat_ident:
                                                            await bot.unban_chat_member(chat_ident, uid)
                                                    except Exception:
                                                        pass
                                                    await edit_or_send_anchor(bot, uid, meta["chat_id"], "Оплачено ✅ Доступ выдан.", kb)
                                                else:
                                                    await edit_or_send_anchor(bot, uid, meta["chat_id"], "Оплачено ✅ Доступ выдан.", None)
                                            except Exception:
                                                pass
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(20)

    async def enforce_expirations_task():
        processed = set()
        while True:
            try:
                now_ts = int(datetime.utcnow().timestamp())
                con = get_db()
                try:
                    cur = con.cursor()
                    cur.execute(
                        """SELECT DISTINCT user_id, channel_idx, expires_at 
                           FROM subscriptions 
                           WHERE expires_at IS NOT NULL AND expires_at <= ? AND expires_at > ?""",
                        (now_ts, now_ts - 86400),
                    )
                    to_revoke = cur.fetchall()
                finally:
                    con.close()

                for user_id, ch_idx, exp_ts in to_revoke:
                    key = (user_id, ch_idx, exp_ts)
                    if key in processed:
                        continue
                    processed.add(key)
                    
                    try:
                        chat_id = CHANNEL_IDS[ch_idx] if 0 <= ch_idx < len(CHANNEL_IDS) else None
                        if chat_id:
                            try:
                                await bot.ban_chat_member(chat_id, user_id)
                                print(f"Banned user {user_id} from channel {ch_idx} ({CHANNELS[ch_idx]})")
                            except Exception as e:
                                print(f"Failed to ban user {user_id} from channel {ch_idx}: {e}")
                        
                        try:
                            await edit_or_send_anchor(
                                bot, user_id, user_id, 
                                f"Подписка на '{CHANNELS[ch_idx]}' истекла. Доступ ограничен.", 
                                InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Показать список тарифов", callback_data="show_tariffs")]])
                            )
                        except Exception as e:
                            print(f"Failed to notify user {user_id}: {e}")
                    except Exception as e:
                        print(f"Error processing expiration for user {user_id}, channel {ch_idx}: {e}")
                        continue

                if len(processed) > 1000:
                    processed.clear()
                    print("Cleared processed expirations cache")

                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in enforce_expirations_task: {e}")
                await asyncio.sleep(30)

    poll_task = asyncio.create_task(poll_invoices_task())
    enforce_task = asyncio.create_task(enforce_expirations_task())
    try:
        await dp.start_polling(bot)
    finally:
        poll_task.cancel()
        with contextlib.suppress(Exception):
            await poll_task
        enforce_task.cancel()
        with contextlib.suppress(Exception):
            await enforce_task


if __name__ == "__main__":
    asyncio.run(main())

