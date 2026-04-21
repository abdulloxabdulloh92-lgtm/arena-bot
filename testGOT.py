"""
⚔️ Fight Arena Bot — v7.0
==========================
Yangiliklar v7:
  • Jins tizimi (👦 O'g'il / 👧 Qiz)
  • Fraksiya tizimi (Stark/Targaryen/Lannister/Baratheon)
  • Qahramon rasmlari (8 ta — har fraksiya 2 ta)
  • Raqib topilganda raqibning rasmi ko'rsatiladi
  • Profildan o'z fraksiya rasmi
  • Guruh reytingi (/guruh_top)
  • Admin: /admin_reset /admin_reset_stats /admin_delete /admin_reset_all /admin_transfer

pip install aiogram sqlalchemy aiosqlite python-dotenv
.env: BOT_TOKEN=...  DATABASE_URL=...  ADMIN_IDS=...
"""

import asyncio, datetime, logging, os, random, re
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardRemove,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile,
)
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey,
    Integer, String, func as sqlfunc, select, text as sa_text
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, relationship

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///fight_arena.db")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]

MAX_HP           = 20
TURN_TIMEOUT     = 60
WARN_AT          = 10
BASE_DMG_MIN     = 5
BASE_DMG_MAX     = 10
POISON_DAMAGE    = 2
COINS_WIN        = 50
COINS_LOSE       = 10
COINS_DRAW       = 25
COINS_DAILY      = 100
RATING_WIN       = 25
RATING_LOSE      = -10
RATING_DRAW      = 5
TOUR_PRIZE       = [500, 250, 100]
BOSS_DAILY_LIMIT = 3
SPLINTER_HP           = 25
SPLINTER_MAX_ENERGY   = 5
SPLINTER_KATANA_HOURS = 24
SPLINTER_COINS        = 300
SPLINTER_CRYSTAL      = 50

# ═══════════════════════════════════════════════════════════
#  FRAKSIYALAR
# ═══════════════════════════════════════════════════════════
FACTIONS = {
    "stark": {
        "name":   "Stark",
        "emoji":  "🐺",
        "motto1": "Biz muzlaymiz — sinmaymiz",
        "motto2": "Shimol hech kimni kechirmaydi!",
        "desc":   "Sharaf, sadoqat, chidamlilik",
    },
    "targaryen": {
        "name":   "Targaryen",
        "emoji":  "🐉",
        "motto1": "Biz kuymaymiz — biz yondiramiz",
        "motto2": "Olov hamma narsani tozalaydi!",
        "desc":   "Qudrat, ajdaho, g'alaba yoki halokat",
    },
    "lannister": {
        "name":   "Lannister",
        "emoji":  "🦁",
        "motto1": "Boylik — bu ham qurol",
        "motto2": "Lannisterlar har doim qarzini to'laydi!",
        "desc":   "Boylik, ayyorlik, nufuz",
    },
    "baratheon": {
        "name":   "Baratheon",
        "emoji":  "🦌",
        "motto1": "Taxt kuchliniki — kuchli esa bizmiz",
        "motto2": "Urush bizning tilimiz",
        "desc":   "Sobitlik, urush, taxt",
    },
}

# ── Qahramon rasmlari (File ID) ──────────────────────────
# Admin /get_file_id buyrug'i orqali oladi
# None bo'lsa — rasm yuborilmaydi, faqat matn chiqadi
CHARACTER_IMAGES = {
    "stark":      {"male": "AgACAgIAAxkBAAIrpmnnKh_WhtUG4_Jw4ZRuuHuukBPBAAK1Emsbd4U5SyXFjIxmWucBAQADAgADeAADOwQ",  "female": "AgACAgIAAxkBAAIrqGnnKnuhwH3Dkm3-9IpPXsvI05giAAK2Emsbd4U5S4mb7VqNpabQAQADAgADeAADOwQ"},
    "targaryen":  {"male": "AgACAgIAAxkBAAIrsGnnK5iyqtzubYtAxhRO6AYxLG8VAAK9Emsbd4U5S8lc8yq3pfbaAQADAgADeQADOwQ",  "female": "AgACAgIAAxkBAAIrrmnnK17sAauue9lRf-CI0w8KmFOeAAK8Emsbd4U5S2l0cfha3PRrAQADAgADeQADOwQ"},
    "lannister":  {"male": "AgACAgIAAxkBAAIrqmnnKsXyBFQWSX7d6OC0UQEUS6B1AAK5Emsbd4U5S9zV-EptM9NzAQADAgADeQADOwQ",  "female": "AgACAgIAAxkBAAIrrGnnKxi3Hy4ud_-LOh7uZW75ZpwHAAK7Emsbd4U5S6VD2Q0he3JmAQADAgADeQADOwQ"},
    "baratheon":  {"male": "AgACAgIAAxkBAAIrsmnnK9E6L2eA6W6msmoTrHVwst2fAAK-Emsbd4U5S9pHmWL5kHnHAQADAgADeQADOwQ",  "female": "AgACAgIAAxkBAAIrtGnnLAO3dX_JwLb2DEu9RcVfslyHAAK_Emsbd4U5S_UsbhZQrlNIAQADAgADeQADOwQ"},
}

# Splinter rasmlari
SPLINTER_FILE_ID  = "AgACAgIAAxkBAAIrKGnlIbqtJ_B4glQv9bbLPtWK8-6EAAITFmsbaB4oSw82KpMR4LmvAQADAgADeQADOwQ"
SPLINTER_WIN_IMG  = "AgACAgIAAxkBAAIrg2nlJQI4swmtnWVQdBo8dRVW-x1FAAImFmsbaB4oS5rdjnFEB58LAQADAgADeQADOwQ"
SPLINTER_LOSE_IMG = "AgACAgIAAxkBAAIrh2nlJYM4fSkoFQG2JZVrRRa-C8RpAAIqFmsbaB4oSx2JoalFREhMAQADAgADeQADOwQ"

def get_character_image(faction: str, gender: str) -> Optional[str]:
    """Fraksiya va jins bo'yicha rasm file_id ni qaytaradi"""
    f = CHARACTER_IMAGES.get(faction, {})
    return f.get("female" if gender == "female" else "male")

def faction_label(faction: str) -> str:
    """🐺 Stark"""
    f = FACTIONS.get(faction, {})
    return f"{f.get('emoji','')} {f.get('name','')}"

def faction_flag(faction: str) -> str:
    """Jang xabarida bayroq sifatida ishlatiladi"""
    return FACTIONS.get(faction, {}).get("emoji", "⚔️")

# ═══════════════════════════════════════════════════════════
#  BOSHQA KONSTANTALAR
# ═══════════════════════════════════════════════════════════
LEAGUES = [
    {"name": "🥉 Bronza",  "min": 0,    "color": "bronze"},
    {"name": "🥈 Kumush",  "min": 100,  "color": "silver"},
    {"name": "🥇 Oltin",   "min": 300,  "color": "gold"},
    {"name": "💎 Diamond", "min": 600,  "color": "diamond"},
    {"name": "👑 Legenda", "min": 1000, "color": "legend"},
]

SHOP_ITEMS = {
    "sword_iron":   {"name": "⚔️ Temir qilich",   "price": 200,  "type": "weapon",
                     "bonus_dmg": 2, "uses": 5,  "uses_label": "5 jang"},
    "sword_steel":  {"name": "🗡 Po'lat qilich",   "price": 450,  "type": "weapon",
                     "bonus_dmg": 4, "uses": 20, "uses_label": "20 jang"},
    "sword_magic":  {"name": "✨ Sehrli qilich",   "price": 900,  "type": "weapon",
                     "bonus_dmg": 7, "uses": 50, "uses_label": "50 jang"},
    "shield_wood":  {"name": "🪵 Yog'och qalqon",  "price": 150,  "type": "armor",  "bonus_def": 1},
    "shield_iron":  {"name": "🛡 Temir qalqon",    "price": 400,  "type": "armor",  "bonus_def": 2},
    "shield_magic": {"name": "💫 Sehrli qalqon",   "price": 900,  "type": "armor",  "bonus_def": 4},
    "potion_small": {"name": "🧪 Kichik iksir",    "price": 50,   "type": "potion", "heal": 5},
    "potion_large": {"name": "⚗️ Katta iksir",     "price": 120,  "type": "potion", "heal": 10},
    "amulet_luck":  {"name": "🍀 Omad talisman",   "price": 30,   "type": "amulet",
                     "currency": "crystal", "crit_chance": 15},
    "amulet_power": {"name": "💠 Kuch talisman",   "price": 50,   "type": "amulet",
                     "currency": "crystal", "bonus_dmg": 3},
}

DAILY_QUESTS = [
    {"id": "win_1",      "desc": "1 ta jang g'olib bo'l",         "target": 1,  "type": "wins",          "reward": 50},
    {"id": "win_3",      "desc": "3 ta jang g'olib bo'l",         "target": 3,  "type": "wins",          "reward": 150},
    {"id": "play_5",     "desc": "5 ta jang o'yna",               "target": 5,  "type": "games_played",  "reward": 80},
    {"id": "dmg_50",     "desc": "Jami 50 zarar ber",             "target": 50, "type": "total_damage",  "reward": 100},
    {"id": "spell_10",   "desc": "10 ta afsun ishlat",            "target": 10, "type": "spells_used",   "reward": 120},
    {"id": "poison_3",   "desc": "3 marta zahar afsunini ishlat", "target": 3,  "type": "poison_used",   "reward": 90},
    {"id": "boss_1",     "desc": "1 ta boss yengib kel",          "target": 1,  "type": "boss_wins",     "reward": 200},
    {"id": "splinter_1", "desc": "Splinterni yengib kel",         "target": 1,  "type": "splinter_wins", "reward": 500},
]

ATTACK_ZONES  = ["Bosh", "Qo'l", "Tana", "Oyoq"]
DEFENSE_ZONES = ["Boshni himoyalash", "Qo'lni himoyalash", "Tana himoyasi", "Oyoqni himoyalash"]
BLOCK_MAP     = {
    "Bosh": "Boshni himoyalash", "Qo'l": "Qo'lni himoyalash",
    "Tana": "Tana himoyasi",      "Oyoq": "Oyoqni himoyalash",
}

BOSSES = {
    "wolf":   {"name": "🐺 Bo'ri",  "hp": 30, "dmg_min": 3,  "dmg_max": 7,
               "def_chance": 40, "coins": 80,  "crystal": 5,
               "desc": "Yirtqich bo'ri! Tez va xavfli.", "ai": "random"},
    "dragon": {"name": "🐉 Ajdaho", "hp": 45, "dmg_min": 5,  "dmg_max": 10,
               "def_chance": 55, "coins": 150, "crystal": 15,
               "desc": "Qadimiy ajdaho! O'rta qiyinlik.", "ai": "adaptive"},
    "demon":  {"name": "👹 Iblis",  "hp": 60, "dmg_min": 7,  "dmg_max": 14,
               "def_chance": 70, "coins": 250, "crystal": 30,
               "desc": "Qorong'u kuchlar timsoli! Juda qiyin.", "ai": "smart"},
}

SPLINTER_WEAPONS = {
    "bolta":  {"name": "🪓 Bolta",  "range": "near", "dmg": (6,10), "hit": 90, "desc": "Og'ir, yaqinda kuchli"},
    "kastet": {"name": "👊 Kastet", "range": "near", "dmg": (4, 7), "hit": 95, "desc": "Tez, ko'p zarba"},
    "nayza":  {"name": "🗡 Nayza",  "range": "mid",  "dmg": (5, 8), "hit": 85, "desc": "O'rta masofa"},
    "pichoq": {"name": "🔪 Pichoq", "range": "near", "dmg": (3, 6), "hit": 92, "desc": "Zahar effekti"},
    "gurzi":  {"name": "🔨 Gurzi",  "range": "near", "dmg": (7,11), "hit": 75, "desc": "Stun berishi mumkin"},
    "yoy":    {"name": "🏹 Yoy",    "range": "far",  "dmg": (3, 6), "hit": 70, "desc": "Har masofada ishlaydi"},
}

POSITION_RANGE_MAP = {"near": ["near"], "mid": ["near","mid"], "far": ["mid","far"]}
POSITIONS          = ["near", "mid", "far"]
POSITION_NAMES     = {"near": "🔴 Yaqin", "mid": "🟡 O'rta", "far": "🟢 Uzoq"}

SPLINTER_STYLES = ["normal","normal","normal","parry","jump","rest"]

SPLINTER_ATK_STORIES = [
    "🐀 Splinter katanasini chaqiriqday tortib <b>{pname}</b>ga <b>{dmg}</b> zarb berdi!",
    "🐀 Splinter sakrab <b>{pname}</b>ning yelkasiga katana bilan <b>{dmg}</b> zarar yetkazdi!",
    "🐀 Splinter aylanib <b>{pname}</b>ga ketma-ket <b>{dmg}</b> zarb qildi!",
    "🐀 Splinter tutun ichidan chiqib <b>{pname}</b>ga <b>{dmg}</b> dahshatli zarba berdi!",
]

SPLINTER_INTRO_TEXTS = [
    "Arena qorong'ulashdi. Tutun orasidan qizil libosli soya paydo bo'ldi.\n"
    "Splinter sizga qarab asta yurdi va katanasini tortib oldi...\n"
    "«Siz hali tayyor emassiz» — dedi u past ovozda.",
    "Devorlar orqasidan g'alati shovqin eshitildi.\n"
    "Splinter bir sakrab arenaga tushdi. Ko'zlari yondi.\n"
    "«Keling, ko'raylik qanday jangchisiz» — dedi jimgina.",
    "Shamolsiz arenada shamol esdi.\n"
    "Splinter katanasini yerga qo'yib ta'zim qildi.\n"
    "«Bu jangda men sizga rahm qilmayman» — dedi u.",
]

ALL_SPELLS = {
    "🔥 Olovli shar":        {"type": "damage",  "value": 3,  "weight": 10,
        "story": "{atk} qo'lini ko'tarib osmonga olovli shar otdi. Raqib kuyib <b>{val}</b> zarar ko'rdi."},
    "💥 Portlovchi toshlar": {"type": "damage",  "value": 5,  "weight": 6,
        "story": "{atk} yer ostidan portlovchi toshlarni chaqirdi. Raqib <b>{val}</b> zarba oldi."},
    "⚔️ O'tkir nayzalar":   {"type": "damage",  "value": 4,  "weight": 9,
        "story": "{atk} havoda bir necha o'tkir nayzalar yaratdi. Ular raqibga <b>{val}</b> zarar yetkazdi."},
    "❄️ Muzli nayzalar":    {"type": "damage",  "value": 4,  "weight": 8,
        "story": "{atk} sovuq shamolni to'plab muzli nayzalar otdi. Raqib <b>{val}</b> zarar oldi."},
    "🌪 Bo'ron":             {"type": "damage",  "value": 6,  "weight": 3,
        "story": "{atk} kuchli bo'ron chaqirib raqibni o'rab oldi. Shiddatli shamol <b>{val}</b> zarar yetkazdi."},
    "☄️ Meteor":             {"type": "damage",  "value": 8,  "weight": 1,
        "story": "{atk} chaqirig'iga javoban koinotdan kameta uchib kelib yer yuzasiga urindi. Raqib <b>{val}</b> zarar ko'rdi."},
    "❤️ Jon tiklash":        {"type": "heal",    "value": 5,  "weight": 8,
        "story": "{atk} hayot energiyasini o'ziga tortib <b>+{val}</b> HP tikladi."},
    "💊 Kichik shifo":       {"type": "heal",    "value": 3,  "weight": 10,
        "story": "{atk} tez yordam iksirini ichib <b>+{val}</b> HP tikladi."},
    "💉 Katta shifo":        {"type": "heal",    "value": 8,  "weight": 3,
        "story": "{atk} yashil nurga cho'milib <b>+{val}</b> HP tikladi."},
    "🦂 Zaharlash":          {"type": "poison",  "value": 3,  "weight": 7,
        "story": "{atk} zaharli nayza otdi. Raqib <b>{val}</b> yurish davomida zahar ta'sirida qoladi."},
    "🛡 Sehrli qalqon":      {"type": "reflect", "value": 1,  "weight": 6,
        "story": "{atk} sehrli qalqon hosil qildi. Keyingi jismoniy zarba o'ziga qaytadi."},
    "⏳ Sekinlashtirish":    {"type": "slow",    "value": 1,  "weight": 5,
        "story": "{atk} raqib atrofida vaqt maydonini sekinlashtirdi. Raqib keyingi afsunni o'ta olmaydi."},
    "💨 Zaiflashtirish":     {"type": "weaken",  "value": 1,  "weight": 6,
        "story": "{atk} raqibga zaiflantiruvchi sehrni urdi. Raqib zarba kuchi 50% kamayadi."},
    "🎯 Aniq zarba":         {"type": "pierce",  "value": 1,  "weight": 5,
        "story": "{atk} raqibi orqasidan hujumga o'tib himoyasini bekor qildi."},
    "🧊 Muzlatish":          {"type": "freeze",  "value": 1,  "weight": 4,
        "story": "{atk} raqibga muzlatuvchi sehrni urdi. Raqib keyingi yurish hujum qila olmaydi."},
    "⚡ Chaqmoq":            {"type": "stun",    "value": 1,  "weight": 4,
        "story": "{atk} chaqmoq bilan raqibni urdiki, u karaxt bo'ldi. Raqib keyingi yurish himoya qila olmaydi."},
    "🔱 Uch zarba":          {"type": "triple",  "value": 6,  "weight": 3,
        "story": "{atk} ketma-ket 3 ta sehrli zarba berdi. Jami <b>{val}</b> HP zarar."},
    "🦖 Tiranozavr":         {"type": "damage",  "value": 6,  "weight": 2,
        "story": "{atk} katta portal ochib u yerdan Tiranozavr chiqardi. Raqib maxluq bilan kurashib <b>{val}</b> zarba oldi."},
    "🌀 Portal":             {"type": "pierce",  "value": 1,  "weight": 3,
        "story": "{atk} raqib orqasiga portal ochib himoyasini chetlab o'tdi."},
    "🕳 Qora tuynuk":        {"type": "damage",  "value": 7,  "weight": 2,
        "story": "{atk} qo'li bilan qora tuynuk yaratdi. Raqib ichiga tortilayotganda <b>{val}</b> zarar ko'rdi."},
}

ATTACK_STORIES = {
    "Bosh": ["{atk} aylanib {dfn}ning iyagiga {dmg} zarb berdi!",
             "{atk} sakrab {dfn}ning boshiga tizza bilan {dmg} zarb urdi!",
             "{atk} bor kuchini yig'ib {dfn}ning jag'iga {dmg} zarb berdi!"],
    "Qo'l": ["{atk} {dfn}ning bilagiga keskin zarba berib suyagini qisirlatdi! {dmg} zarar.",
             "{atk} {dfn}ning yelkasiga {dmg} zarb berdi!",
             "{atk} burilib {dfn}ning tirsagiga {dmg} zarb urdi!"],
    "Tana": ["{atk} {dfn}ning qovurg'asiga {dmg} zarb berdi!",
             "{atk} sakrab {dfn}ning ko'kragiga tiz bilan {dmg} zarb urdi!",
             "{atk} ketma ket hujum bilan {dfn}ning yelkasiga {dmg} zarb berdi!"],
    "Oyoq": ["{atk} aylanib {dfn}ning soniga {dmg} zarb berdi!",
             "{atk} past holatdan {dfn}ning tizzasiga {dmg} zarb urdi!",
             "{atk} oyog'ini baland ko'tarib {dfn}ning boldiri bo'ylab {dmg} zarb berdi!"],
}

BLOCK_STORIES = [
    "{dfn} professional himoyachi kabi zarba yo'lini to'sdi!",
    "{dfn} qo'lini ko'tarib zarbani to'liq blok qildi!",
    "{dfn} bir qadam chekinib zarbani shamday o'tkazib yubordi!",
]

CRIT_STORIES = [
    "{atk} bor kuchini yig'ib {dfn}ning {zone}iga {dmg} KRITIK zarb berdi! 💥",
    "{atk} mo'ljal olgan {zone} joyida {dfn}ga {dmg} dahshatli zarb tushdi! 💥",
]

COMMENTARIES = [
    "Aytishlaricha hayvonlar egalariga o'xsharkan, menimcha bu safar teskarisi!",
    "Nima bo'ldi endi ikkisi ham yerda yotibdi!",
    "Bo'ldi jang tugadi. Zambillarni olib kelinglar!",
    "Bunday jangni hech qachon ko'rmagan edim!",
    "Arena larzaga keldi! Bu zarba afsonaga aylanadi!",
    "Tomoshabinlar nafasini ichiga yutdi!",
    "Bu jang tarix kitoblariga kiradi!",
    "Jasorat va kuch — bugungi jangning ikki qahramoni!",
    "Qon qizigan! Hech kim chekinmaydi!",
]

NICK_RE = re.compile(r'^[a-zA-Z0-9а-яА-ЯёЁ]{3,10}$')

# ═══════════════════════════════════════════════════════════
#  FSM STATES
# ═══════════════════════════════════════════════════════════
class RegState(StatesGroup):
    waiting_gender  = State()
    waiting_nick    = State()
    waiting_faction = State()

class AdminState(StatesGroup):
    confirm_reset_all = State()
    confirm_transfer  = State()   # transfer ma'lumotini FSM da saqlaymiz

# ═══════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════
engine            = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

class Base(DeclarativeBase):
    pass

class Player(Base):
    __tablename__ = "players"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id      = Column(BigInteger, unique=True, nullable=False, index=True)
    username         = Column(String(64))
    full_name        = Column(String(128), nullable=False, default="Noma'lum")
    nickname         = Column(String(10), unique=True, nullable=True)
    gender           = Column(String(8), nullable=True)     # "male" | "female"
    faction          = Column(String(16), nullable=True)    # "stark" | "targaryen" | ...
    wins             = Column(Integer, default=0)
    losses           = Column(Integer, default=0)
    draws            = Column(Integer, default=0)
    total_damage     = Column(Integer, default=0)
    total_healed     = Column(Integer, default=0)
    spells_used      = Column(Integer, default=0)
    poison_used      = Column(Integer, default=0)
    boss_wins        = Column(Integer, default=0)
    splinter_wins    = Column(Integer, default=0)
    rating           = Column(Integer, default=0)
    league           = Column(String(32), default="bronze")
    coins            = Column(Integer, default=100)
    crystal          = Column(Integer, default=0)
    weapon_slot      = Column(String(32))
    weapon_uses_left = Column(Integer, default=0)
    armor_slot       = Column(String(32))
    amulet_slot      = Column(String(32))
    katana_expires   = Column(DateTime, nullable=True)
    created_at       = Column(DateTime, default=sqlfunc.now())
    last_daily       = Column(DateTime)
    boss_today       = Column(Integer, default=0)
    boss_date        = Column(String(10))
    inventory      = relationship("Inventory",    back_populates="player", cascade="all, delete-orphan")
    quest_progress = relationship("QuestProgress",back_populates="player", cascade="all, delete-orphan")
    match_history  = relationship("MatchHistory", back_populates="player", cascade="all, delete-orphan",
                                  foreign_keys="MatchHistory.player_id")
    tourn_parts    = relationship("TournamentParticipant", back_populates="player", cascade="all, delete-orphan")

    @property
    def has_katana(self) -> bool:
        return self.katana_expires is not None and self.katana_expires > datetime.datetime.utcnow()

    @property
    def katana_hours_left(self) -> int:
        if not self.has_katana: return 0
        return max(0, int((self.katana_expires - datetime.datetime.utcnow()).total_seconds() // 3600))

    @property
    def faction_flag(self) -> str:
        return faction_flag(self.faction) if self.faction else "⚔️"

    @property
    def display_name(self) -> str:
        """Jang xabarida ko'rinadigan nom: 🐺 Jony"""
        return f"{self.faction_flag}{self.nickname or self.full_name}"

class Inventory(Base):
    __tablename__ = "inventory"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False)
    item_key  = Column(String(64), nullable=False)
    quantity  = Column(Integer, default=1)
    bought_at = Column(DateTime, default=sqlfunc.now())
    player    = relationship("Player", back_populates="inventory")

class MatchHistory(Base):
    __tablename__ = "match_history"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    player_id      = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False)
    opponent_id    = Column(BigInteger, nullable=False)
    opponent_name  = Column(String(128), nullable=False)
    result         = Column(String(8), nullable=False)
    rounds         = Column(Integer, default=0)
    damage_dealt   = Column(Integer, default=0)
    coins_earned   = Column(Integer, default=0)
    crystal_earned = Column(Integer, default=0)
    rating_change  = Column(Integer, default=0)
    played_at      = Column(DateTime, default=sqlfunc.now())
    player         = relationship("Player", back_populates="match_history", foreign_keys=[player_id])

class QuestProgress(Base):
    __tablename__ = "quest_progress"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    player_id  = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False)
    quest_id   = Column(String(32), nullable=False)
    progress   = Column(Integer, default=0)
    completed  = Column(Boolean, default=False)
    claimed    = Column(Boolean, default=False)
    reset_date = Column(String(10), nullable=False)
    player     = relationship("Player", back_populates="quest_progress")

class Tournament(Base):
    __tablename__ = "tournaments"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    status       = Column(String(16), default="waiting")
    max_players  = Column(Integer, default=8)
    created_at   = Column(DateTime, default=sqlfunc.now())
    started_at   = Column(DateTime)
    finished_at  = Column(DateTime)
    participants = relationship("TournamentParticipant", back_populates="tournament", cascade="all, delete-orphan")
    matches      = relationship("TournamentMatch",       back_populates="tournament", cascade="all, delete-orphan")

class TournamentParticipant(Base):
    __tablename__ = "tournament_participants"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False)
    player_id     = Column(Integer, ForeignKey("players.id",     ondelete="CASCADE"), nullable=False)
    eliminated    = Column(Boolean, default=False)
    final_place   = Column(Integer)
    tournament    = relationship("Tournament", back_populates="participants")
    player        = relationship("Player",     back_populates="tourn_parts")

class TournamentMatch(Base):
    __tablename__ = "tournament_matches"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False)
    player1_tg    = Column(BigInteger, nullable=False)
    player2_tg    = Column(BigInteger, nullable=False)
    winner_tg     = Column(BigInteger)
    round_number  = Column(Integer, default=1)
    played_at     = Column(DateTime)
    tournament    = relationship("Tournament", back_populates="matches")

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        migrations = [
            ("players", "nickname",         "TEXT"),
            ("players", "gender",           "TEXT"),
            ("players", "faction",          "TEXT"),
            ("players", "crystal",          "INTEGER DEFAULT 0"),
            ("players", "weapon_uses_left", "INTEGER DEFAULT 0"),
            ("players", "boss_wins",        "INTEGER DEFAULT 0"),
            ("players", "splinter_wins",    "INTEGER DEFAULT 0"),
            ("players", "boss_today",       "INTEGER DEFAULT 0"),
            ("players", "boss_date",        "TEXT"),
            ("players", "katana_expires",   "DATETIME"),
            ("match_history", "crystal_earned", "INTEGER DEFAULT 0"),
        ]
        for table, col, col_def in migrations:
            try:
                await conn.execute(sa_text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
                logger.info(f"✅ Migration: {table}.{col}")
            except Exception:
                pass

async def db_get(session: AsyncSession, tid: int) -> Optional[Player]:
    r = await session.execute(select(Player).where(Player.telegram_id == tid))
    return r.scalar_one_or_none()

async def db_upsert(session: AsyncSession, tid: int, name: str, username=None) -> Player:
    p = await db_get(session, tid)
    if not p:
        p = Player(telegram_id=tid, full_name=name, username=username)
        session.add(p); await session.commit(); await session.refresh(p)
    elif p.full_name != name:
        p.full_name = name; p.username = username; await session.commit()
    return p

async def db_nick_exists(session: AsyncSession, nick: str) -> bool:
    r = await session.execute(select(Player).where(Player.nickname == nick))
    return r.scalar_one_or_none() is not None

async def db_top(session: AsyncSession, limit=10) -> list[Player]:
    r = await session.execute(select(Player).order_by(Player.rating.desc()).limit(limit))
    return list(r.scalars().all())

async def db_faction_top(session: AsyncSession) -> dict:
    """Har fraksiyaning umumiy rating summasi"""
    result = {}
    for fkey in FACTIONS:
        r = await session.execute(
            select(sqlfunc.sum(Player.rating), sqlfunc.count(Player.id))
            .where(Player.faction == fkey)
        )
        row = r.one()
        result[fkey] = {"total": row[0] or 0, "count": row[1] or 0}
    return result

# ═══════════════════════════════════════════════════════════
#  GLOBAL HOLAT
# ═══════════════════════════════════════════════════════════
games:          dict[int, "GameState"]     = {}
waiting_queue:  list[int]                  = []
timeout_tasks:  dict[int, asyncio.Task]    = {}
warn_tasks:     dict[int, asyncio.Task]    = {}
name_cache:     dict[int, str]             = {}
faction_cache:  dict[int, str]             = {}   # uid → faction key
gender_cache:   dict[int, str]             = {}   # uid → "male"|"female"
boss_games:     dict[int, "BossState"]     = {}
splinter_games: dict[int, "SplinterState"]= {}

# ═══════════════════════════════════════════════════════════
#  GAMESTATES
# ═══════════════════════════════════════════════════════════
@dataclass
class GameState:
    tid:     int
    eid:     Optional[int] = None
    hp:      int  = MAX_HP
    rnd:     int  = 0
    attack:  Optional[str]   = None
    defense: Optional[str]   = None
    spell:   Optional[tuple] = None
    poison:  int  = 0
    slow:    bool = False
    reflect: bool = False
    weaken:  bool = False
    pierce:  bool = False
    frozen:  bool = False
    stunned: bool = False
    bonus_dmg:   int = 0
    bonus_def:   int = 0
    crit_chance: int = 0
    s_dmg:    int = 0
    s_heal:   int = 0
    s_spells: int = 0
    s_poison: int = 0
    tournament_id: Optional[int] = None

    def reset_turn(self):
        self.attack = self.defense = self.spell = None
        self.reflect = self.weaken = self.pierce = self.stunned = False

    @property
    def hp_bar(self) -> str:
        f = max(0, round(self.hp / MAX_HP * 10))
        return "❤️" * f + "🖤" * (10 - f)

    def load_gear(self, w, a, am):
        if w  and w  in SHOP_ITEMS: self.bonus_dmg   = SHOP_ITEMS[w].get("bonus_dmg",   0)
        if a  and a  in SHOP_ITEMS: self.bonus_def   = SHOP_ITEMS[a].get("bonus_def",   0)
        if am and am in SHOP_ITEMS: self.crit_chance = SHOP_ITEMS[am].get("crit_chance", 0)

def _gs_ready(self): return (self.attack is not None and
                              self.defense is not None and self.spell is not None)
GameState.is_ready = property(_gs_ready)

@dataclass
class BossState:
    uid:        int
    boss_key:   str
    boss_hp:    int
    player_hp:  int = MAX_HP
    rnd:        int = 0
    hit_zones:  dict = None
    last_player_zone: Optional[str] = None

    def __post_init__(self):
        if self.hit_zones is None: self.hit_zones = {z: 0 for z in ATTACK_ZONES}

    @property
    def boss(self): return BOSSES[self.boss_key]
    @property
    def boss_name(self): return self.boss["name"]

    def player_bar(self):
        f = max(0, round(self.player_hp / MAX_HP * 10))
        return "❤️" * f + "🖤" * (10 - f)

    def boss_bar(self):
        f = max(0, round(self.boss_hp / self.boss["hp"] * 10))
        return "🔴" * f + "⬛" * (10 - f)

    def ai_defense(self):
        ai = self.boss["ai"]
        if ai == "adaptive" and self.last_player_zone:
            return BLOCK_MAP.get(self.last_player_zone, random.choice(DEFENSE_ZONES))
        elif ai == "smart":
            mx = max(self.hit_zones, key=lambda z: self.hit_zones[z])
            if self.hit_zones[mx] > 0: return BLOCK_MAP[mx]
        return random.choice(DEFENSE_ZONES)

    def ai_attack(self):
        if random.random() < 0.3 and self.last_player_zone:
            return self.last_player_zone
        return random.choice(ATTACK_ZONES)

@dataclass
class SplinterState:
    uid:          int
    weapon_key:   str
    player_hp:    int  = MAX_HP
    player_energy:int  = SPLINTER_MAX_ENERGY
    splinter_hp:  int  = SPLINTER_HP
    rnd:          int  = 0
    position:     str  = "mid"
    splinter_resting: bool = False
    splinter_double:  bool = False
    splinter_last_style: str = "normal"
    player_last_action:  str = ""
    total_dmg_dealt: int = 0
    total_dmg_taken: int = 0
    rounds_played:   int = 0

    @property
    def weapon(self): return SPLINTER_WEAPONS[self.weapon_key]

    def energy_bar(self):
        return "⚡" * self.player_energy + "▪️" * (SPLINTER_MAX_ENERGY - self.player_energy)

    def hp_bar(self):
        f = max(0, round(self.player_hp / MAX_HP * 10))
        return "❤️" * f + "🖤" * (10 - f)

    def splinter_hp_bar(self):
        f = max(0, round(self.splinter_hp / SPLINTER_HP * 10))
        return "🔴" * f + "⬛" * (10 - f)

    def hit_chance(self):
        w_range = self.weapon["range"]
        pos_ok  = POSITION_RANGE_MAP.get(self.position, ["mid"])
        base    = self.weapon["hit"]
        return base if w_range in pos_ok else max(20, base - 40)

# ═══════════════════════════════════════════════════════════
#  YORDAMCHI FUNKSIYALAR
# ═══════════════════════════════════════════════════════════
def get_league(rating: int) -> dict:
    res = LEAGUES[0]
    for lg in LEAGUES:
        if rating >= lg["min"]: res = lg
    return res

def cancel_timeout(uid: int):
    for d in (timeout_tasks, warn_tasks):
        t = d.pop(uid, None)
        if t and not t.done(): t.cancel()

def cancel_both(u1: int, u2: int):
    cancel_timeout(u1); cancel_timeout(u2)

async def start_timeout(uid: int, on_expire, on_warn=None):
    cancel_timeout(uid)

    async def _warn():
        await asyncio.sleep(TURN_TIMEOUT - WARN_AT)
        if uid in games or uid in boss_games or uid in splinter_games:
            try:
                if on_warn: await on_warn()
            except Exception as e: logger.error(f"warn err {uid}: {e}")

    async def _expire():
        await asyncio.sleep(TURN_TIMEOUT)
        if uid in games or uid in boss_games or uid in splinter_games:
            try: await on_expire()
            except Exception as e: logger.error(f"expire err {uid}: {e}")

    warn_tasks[uid]    = asyncio.create_task(_warn())
    timeout_tasks[uid] = asyncio.create_task(_expire())

async def get_pname(uid: int, bot: Bot) -> str:
    if uid not in name_cache:
        async with AsyncSessionLocal() as s:
            p = await db_get(s, uid)
            if p:
                name_cache[uid]    = p.nickname or p.full_name
                faction_cache[uid] = p.faction or ""
                gender_cache[uid]  = p.gender or "male"
                return name_cache[uid]
        try:
            c = await bot.get_chat(uid)
            name_cache[uid] = c.full_name or f"#{uid}"
        except: name_cache[uid] = f"#{uid}"
    return name_cache[uid]

def get_display_name(uid: int, name: str) -> str:
    """Jang xabarida: 🐺Jony"""
    flag = faction_flag(faction_cache.get(uid, ""))
    return f"{flag}{name}"

def pick_spells(n=3) -> list[str]:
    pool = list(ALL_SPELLS.keys())
    w    = [ALL_SPELLS[k]["weight"] for k in pool]
    out: list[str] = []
    tmp = list(zip(pool, w))
    while len(out) < n and tmp:
        ns, ws = zip(*tmp)
        pick = random.choices(list(ns), weights=list(ws), k=1)[0]
        out.append(pick); tmp = [(a, b) for a, b in tmp if a != pick]
    return out

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⚔️ Qayta jang", callback_data="go:game"),
        InlineKeyboardButton(text="🏟 Bosh menyu", callback_data="go:menu"),
    ]])

async def send_image(bot: Bot, uid: int, file_id: Optional[str],
                     filepath: Optional[str], caption: str):
    try:
        if file_id:
            await bot.send_photo(uid, file_id, caption=caption, parse_mode=ParseMode.HTML)
        elif filepath and os.path.exists(filepath):
            msg = await bot.send_photo(uid, FSInputFile(filepath), caption=caption, parse_mode=ParseMode.HTML)
            logger.info(f"Rasm file_id: {msg.photo[-1].file_id}")
        else:
            await bot.send_message(uid, caption)
    except Exception as e:
        logger.error(f"send_image err: {e}")
        await bot.send_message(uid, caption)

# ═══════════════════════════════════════════════════════════
#  ZARAR HISOBLASH
# ═══════════════════════════════════════════════════════════
def calc_hit(atk: GameState, dfn: GameState) -> tuple[int, bool, str, str]:
    if atk.attack == "FROZEN": return 0, False, "", "frozen"
    zone = atk.attack
    if dfn.reflect:
        raw = max(1, random.randint(BASE_DMG_MIN, BASE_DMG_MAX) + atk.bonus_dmg - dfn.bonus_def)
        return raw, True, "", zone
    eff_def = None if dfn.stunned else dfn.defense
    blocked = (not atk.pierce) and (BLOCK_MAP.get(zone) == eff_def)
    if blocked: return 0, False, random.choice(BLOCK_STORIES), zone
    base = random.randint(BASE_DMG_MIN, BASE_DMG_MAX)
    dmg  = max(1, base + atk.bonus_dmg - max(0, dfn.bonus_def))
    if dfn.weaken: dmg = max(1, dmg // 2)
    is_crit = atk.crit_chance > 0 and random.randint(1, 100) <= atk.crit_chance
    if is_crit:
        dmg = int(dmg * 1.5)
        tpl = random.choice(CRIT_STORIES)
    else:
        tpl = random.choice(ATTACK_STORIES.get(zone, ["{atk} {dfn}ga {dmg} zarb berdi!"]))
    return max(1, dmg), False, tpl, zone

def build_story(tpl: str, atk: str, dfn: str, dmg: int, zone: str) -> str:
    return tpl.format(atk=f"<b>{atk}</b>", dfn=f"<b>{dfn}</b>",
                      dmg=f"<b>{dmg}</b>", zone=zone.lower())

def apply_spell(sname: str, sdata: dict, atk: GameState, dfn: GameState, aname: str) -> str:
    st = sdata["type"]; val = sdata["value"]
    atk.s_spells += 1
    story = sdata["story"].format(atk=f"<b>{aname}</b>", val=val)
    if   st == "damage":  dfn.hp = max(0, dfn.hp - val); atk.s_dmg += val
    elif st == "heal":
        bef = atk.hp; atk.hp = min(MAX_HP, atk.hp + val)
        healed = atk.hp - bef; atk.s_heal += healed
        story = sdata["story"].format(atk=f"<b>{aname}</b>", val=healed)
    elif st == "poison":  dfn.poison = val; atk.s_poison += 1
    elif st == "reflect": atk.reflect = True
    elif st == "slow":    dfn.slow = True
    elif st == "weaken":  dfn.weaken = True
    elif st == "pierce":  atk.pierce = True
    elif st == "freeze":  dfn.frozen = True
    elif st == "stun":    dfn.stunned = True
    elif st == "triple":  dfn.hp = max(0, dfn.hp - val); atk.s_dmg += val
    return story

# ═══════════════════════════════════════════════════════════
#  KLAVIATURALAR
# ═══════════════════════════════════════════════════════════
attack_kb = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="Bosh"), KeyboardButton(text="Qo'l")],
    [KeyboardButton(text="Tana"), KeyboardButton(text="Oyoq")],
], resize_keyboard=True)

defense_kb = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="Boshni himoyalash"), KeyboardButton(text="Qo'lni himoyalash")],
    [KeyboardButton(text="Tana himoyasi"),     KeyboardButton(text="Oyoqni himoyalash")],
], resize_keyboard=True)

main_kb = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="⚔️ Jang"),    KeyboardButton(text="👤 Profil")],
    [KeyboardButton(text="🏪 Do'kon"),  KeyboardButton(text="📋 Vazifalar")],
    [KeyboardButton(text="👑 Reyting"), KeyboardButton(text="🏆 Turnir")],
    [KeyboardButton(text="🐉 Boss"),    KeyboardButton(text="🐀 Splinter")],
    [KeyboardButton(text="🏰 Guruhlar"),KeyboardButton(text="ℹ️ Yordam")],
], resize_keyboard=True)

def gender_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="👦 O'g'il", callback_data="reg:gender:male"),
        InlineKeyboardButton(text="👧 Qiz",    callback_data="reg:gender:female"),
    ]])

def faction_kb() -> InlineKeyboardMarkup:
    rows = []
    for fkey, fdata in FACTIONS.items():
        rows.append([InlineKeyboardButton(
            text=f"{fdata['emoji']} {fdata['name']}  —  {fdata['desc']}",
            callback_data=f"reg:faction:{fkey}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def spell_kb(names: list[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=f"{n}  ·  {ALL_SPELLS[n]['story'][:28].split('.')[0]}",
        callback_data=f"spell:{n}"
    )] for n in names]
    rows.append([InlineKeyboardButton(text="⏭ Afsun ishlatmaslik", callback_data="spell:SKIP")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def shop_kb(cat=None) -> InlineKeyboardMarkup:
    if not cat:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚔️ Qurollar (vaqtli)",  callback_data="shop:cat:weapon")],
            [InlineKeyboardButton(text="🛡 Zirh (doimiy)",       callback_data="shop:cat:armor")],
            [InlineKeyboardButton(text="🍀 Talismanlar (💎)",    callback_data="shop:cat:amulet")],
            [InlineKeyboardButton(text="🧪 Iksirlar",            callback_data="shop:cat:potion")],
        ])
    rows = []
    for k, v in SHOP_ITEMS.items():
        if v["type"] != cat: continue
        cur   = "💎" if v.get("currency") == "crystal" else "🪙"
        label = f"{v['name']}  —  {v['price']} {cur}"
        if v.get("uses_label"): label += f"  ({v['uses_label']})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"shop:buy:{k}")])
    rows.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="shop:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def equip_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Kiyish / Ishlatish", callback_data=f"equip:{key}")],
        [InlineKeyboardButton(text="◀️ Do'konga qaytish",   callback_data="shop:back")],
    ])

def quest_kb(qlist: list) -> InlineKeyboardMarkup:
    rows = []
    for q in qlist:
        if q["completed"] and not q["claimed"]:
            rows.append([InlineKeyboardButton(text=f"🎁 Mukofot  +{q['reward']} 🪙",
                                              callback_data=f"quest:claim:{q['id']}")])
        elif q["completed"]:
            rows.append([InlineKeyboardButton(text=f"✅ {q['desc']}", callback_data="noop")])
        else:
            rows.append([InlineKeyboardButton(
                text=f"⏳ {q['desc']}  ({q['progress']}/{q['target']})",
                callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def hist_kb(page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="◀️", callback_data=f"hist:{max(0,page-1)}"),
        InlineKeyboardButton(text=f"{page+1}-bet", callback_data="noop"),
        InlineKeyboardButton(text="▶️", callback_data=f"hist:{page+1}"),
    ]])

def tour_kb(tid: int, can: bool) -> InlineKeyboardMarkup:
    rows = []
    if can: rows.append([InlineKeyboardButton(text="⚔️ Qo'shilish", callback_data=f"tour:join:{tid}")])
    rows.append([InlineKeyboardButton(text="👥 Ishtirokchilar", callback_data=f"tour:list:{tid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def boss_select_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text=f"{v['name']}  ·  {v['desc']}", callback_data=f"boss:start:{k}"
    )] for k, v in BOSSES.items()])

def boss_attack_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🗡 {z}", callback_data=f"boss:atk:{z}")
    ] for z in ATTACK_ZONES])

def splinter_weapon_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text=f"{v['name']}  ·  {v['desc']}", callback_data=f"spl:weapon:{k}"
    )] for k, v in SPLINTER_WEAPONS.items()])

def splinter_action_kb(sp: SplinterState) -> InlineKeyboardMarkup:
    pos = POSITION_NAMES[sp.position]; hit = sp.hit_chance(); w = sp.weapon
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👊 Zarb  ({w['name']}, {hit}%)", callback_data="spl:act:zarb")],
        [InlineKeyboardButton(text="👣 Yaqinlashish (-1⚡)", callback_data="spl:act:yaqin"),
         InlineKeyboardButton(text="🏃 Chetlashish (-1⚡)",  callback_data="spl:act:chet")],
        [InlineKeyboardButton(text="🧘 Dam olish (+5⚡)",    callback_data="spl:act:dam"),
         InlineKeyboardButton(text="ℹ️ Holat",               callback_data="spl:act:info")],
        [InlineKeyboardButton(text=f"📍 {pos}  |  {sp.energy_bar()}", callback_data="noop")],
    ])

def confirm_reset_all_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, rostanam reset qilaman", callback_data="admin:confirm_reset_all")],
        [InlineKeyboardButton(text="❌ Yo'q, bekor qilish",         callback_data="admin:cancel")],
    ])

def confirm_transfer_kb(from_id: int, to_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, o'tkazaman",    callback_data=f"admin:confirm_transfer:{from_id}:{to_id}")],
        [InlineKeyboardButton(text="❌ Yo'q, bekor qilish", callback_data="admin:cancel")],
    ])

# ═══════════════════════════════════════════════════════════
#  PvP BOSQICHLARI
# ═══════════════════════════════════════════════════════════
async def phase_attack(uid: int, bot: Bot):
    g = games.get(uid)
    if not g: return
    if g.frozen:
        g.frozen = False; g.attack = "FROZEN"
        await bot.send_message(uid, "🧊 <b>Muzlatilgansiz!</b> Bu yurish hujum qila olmaysiz.",
                               reply_markup=defense_kb)
        await _pvp_timeout(uid, "himoya", bot)
    else:
        await bot.send_message(uid,
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🗡 <b>{g.rnd + 1}-RAUNT</b> — Hujumni tanlang!\n"
            f"⏰ Vaqt: {TURN_TIMEOUT} soniya\n"
            f"━━━━━━━━━━━━━━━━━━━━━",
            reply_markup=attack_kb
        )
        await _pvp_timeout(uid, "hujum", bot)

async def phase_defense(uid: int, bot: Bot):
    g = games.get(uid)
    if not g: return
    cancel_timeout(uid)
    if g.stunned:
        g.defense = "STUNNED"
        await bot.send_message(uid, "⚡ <b>Karaxt!</b> Bu yurish himoya qila olmaysiz.")
        await phase_spell(uid, bot)
    else:
        await bot.send_message(uid, f"🛡 <b>Himoyani tanlang!</b>\n⏰ Vaqt: {TURN_TIMEOUT} soniya",
                               reply_markup=defense_kb)
        await _pvp_timeout(uid, "himoya", bot)

async def phase_spell(uid: int, bot: Bot):
    g = games.get(uid)
    if not g: return
    cancel_timeout(uid)
    if g.slow:
        g.slow = False; g.spell = ("SKIP", None)
        await bot.send_message(uid, "⏳ <b>Sekinlashtirish!</b> Bu yurish afsun ishlatamolmaysiz.")
        await try_resolve(uid, bot); return
    sp = pick_spells(3)
    await bot.send_message(uid, f"🔮 <b>Afsun tanlang!</b>\n⏰ Vaqt: {TURN_TIMEOUT} soniya",
                           reply_markup=spell_kb(sp))
    await _pvp_timeout(uid, "afsun", bot)

async def _pvp_timeout(uid: int, step: str, bot: Bot):
    async def _warn():
        if uid in games:
            await bot.send_message(uid, f"⚠️ <b>{WARN_AT} soniya qoldi!</b> Tez <b>{step}</b> tanlang!")
    await start_timeout(uid, lambda: force_loss(uid, step, bot), _warn)

async def try_resolve(uid: int, bot: Bot):
    g = games.get(uid)
    if not g or not g.eid: return
    eg = games.get(g.eid)
    if not eg: return
    if g.is_ready and eg.is_ready:
        await resolve_turn(uid, g.eid, bot)

async def force_loss(loser: int, step: str, bot: Bot):
    if loser not in games: return
    g = games[loser]; eid = g.eid
    ln = name_cache.get(loser, f"#{loser}")
    await bot.send_message(loser,
        f"⏰ <b>Vaqt tugadi!</b> Siz <b>{step}</b> tanlamadingiz.\n❌ <b>Mag'lubiyat!</b>",
        reply_markup=ReplyKeyboardRemove())
    await bot.send_message(loser, "Keyingi jang uchun:", reply_markup=back_kb())
    if eid and eid in games:
        await bot.send_message(eid,
            f"⏰ <b>{ln}</b> vaqtni o'tkazib yubordi.\n🏆 <b>G'alaba sizniki!</b>",
            reply_markup=ReplyKeyboardRemove())
        await bot.send_message(eid, "Keyingi jang uchun:", reply_markup=back_kb())
        async with AsyncSessionLocal() as s:
            ep = await db_get(s, eid)
            if ep:
                ep.wins += 1; ep.coins += COINS_WIN
                ep.crystal = (ep.crystal or 0) + RATING_WIN
                ep.rating = max(0, ep.rating + RATING_WIN)
                ep.league = get_league(ep.rating)["color"]; await s.commit()
        cancel_timeout(eid); games.pop(eid, None)
    cancel_timeout(loser); games.pop(loser, None)

async def resolve_turn(uid1: int, uid2: int, bot: Bot):
    cancel_both(uid1, uid2)
    g1 = games.get(uid1); g2 = games.get(uid2)
    if not g1 or not g2: return

    g1.rnd += 1; g2.rnd = g1.rnd; rnd = g1.rnd
    n1 = await get_pname(uid1, bot); n2 = await get_pname(uid2, bot)
    dn1 = get_display_name(uid1, n1); dn2 = get_display_name(uid2, n2)
    lines: list[str] = []

    for ag, an, dg in [(g1, n1, g2), (g2, n2, g1)]:
        sname, sdata = ag.spell or ("SKIP", None)
        if sname != "SKIP" and sdata:
            lines.append(f"✨{apply_spell(sname, sdata, ag, dg, an)}")

    for g, gn in [(g1, n1), (g2, n2)]:
        if g.poison > 0:
            g.hp = max(0, g.hp - POISON_DAMAGE); g.poison -= 1
            left = f" ({g.poison} yurish qoldi)" if g.poison > 0 else " (zahar tugadi)"
            lines.append(f"☠️ <b>{gn}</b> zahar ta'sirida <b>-{POISON_DAMAGE} HP</b>{left}")

    d1, ref1, s1, z1 = calc_hit(g1, g2)
    d2, ref2, s2, z2 = calc_hit(g2, g1)

    def apply_hit(d, ref, s, z, ag, an, dn, dg, dfn):
        if z == "frozen":
            lines.append(f"🧊 <b>{an}</b> muzlatilgan — hujum qila olmadi")
        elif ref:
            ag.hp = max(0, ag.hp - d)
            lines.append(f"⚔ <b>{dn}</b> {z.lower()} joyiga zarba berdi, lekin 🪞 qalqon qaytardi! (<b>-{d} HP</b> o'ziga)")
        elif s:
            lines.append(f"⚔ {build_story(s, dn, dfn, d, z)}")
            dg.hp = max(0, dg.hp - d); ag.s_dmg += d
        else:
            tpl = random.choice(ATTACK_STORIES.get(z, ["{atk} {dfn}ga {dmg} zarb berdi!"]))
            lines.append(f"⚔ {build_story(tpl, dn, dfn, d, z)}")
            dg.hp = max(0, dg.hp - d); ag.s_dmg += d

    apply_hit(d1, ref1, s1, z1, g1, n1, dn1, g2, dn2)
    apply_hit(d2, ref2, s2, z2, g2, n2, dn2, g1, dn1)
    g1.reset_turn(); g2.reset_turn()

    msg = (
        f"📊 <b>{rnd} - yurish yakunlandi!</b>\n"
        + "\n".join(lines) + "\n"
        f"Boshlovchi: {random.choice(COMMENTARIES)}\n"
        f"{dn1}: {g1.hp}/{MAX_HP}💧 {g1.s_dmg}🔰 "
        f"{dn2}: {g2.hp}/{MAX_HP}💧 {g2.s_dmg}🔰\n"
        f"🗡 Qayerga zarb bermoqchisiz?"
    )

    if g1.hp <= 0 or g2.hp <= 0:
        result = "uid1_wins" if g1.hp > g2.hp else ("uid2_wins" if g2.hp > g1.hp else "draw")
        final  = msg.replace("🗡 Qayerga zarb bermoqchisiz?", "")
        await bot.send_message(uid1, final, reply_markup=ReplyKeyboardRemove())
        await bot.send_message(uid2, final, reply_markup=ReplyKeyboardRemove())
        await finish_game(uid1, uid2, result, bot, n1, n2, dn1, dn2, g1, g2, rnd); return

    await bot.send_message(uid1, msg, reply_markup=attack_kb)
    await bot.send_message(uid2, msg, reply_markup=attack_kb)
    await phase_attack(uid1, bot); await phase_attack(uid2, bot)

async def finish_game(uid1, uid2, result, bot: Bot,
                      n1, n2, dn1, dn2, g1: GameState, g2: GameState, rnd: int):
    cancel_both(uid1, uid2)
    games.pop(uid1, None); games.pop(uid2, None)

    if   result == "uid1_wins": r1, r2 = "win",  "loss"
    elif result == "uid2_wins": r1, r2 = "loss", "win"
    else:                       r1, r2 = "draw", "draw"

    def calc_r(res):
        if res == "win":  return COINS_WIN,  RATING_WIN
        if res == "loss": return COINS_LOSE, RATING_LOSE
        return COINS_DRAW, RATING_DRAW

    c1, rc1 = calc_r(r1); c2, rc2 = calc_r(r2)
    cr1 = abs(rc1); cr2 = abs(rc2)

    async with AsyncSessionLocal() as s:
        p1 = await db_get(s, uid1); p2 = await db_get(s, uid2)

        def upd(p, g, res, c, rc, cr):
            if not p or not g: return
            if res == "win": p.wins += 1
            elif res == "loss": p.losses += 1
            else: p.draws += 1
            p.coins += c; p.crystal = (p.crystal or 0) + cr
            p.rating = max(0, p.rating + rc)
            p.total_damage += g.s_dmg; p.total_healed += g.s_heal
            p.spells_used  += g.s_spells; p.poison_used += g.s_poison
            p.league = get_league(p.rating)["color"]
            if p.weapon_slot and (p.weapon_uses_left or 0) > 0:
                p.weapon_uses_left -= 1
                if p.weapon_uses_left <= 0: p.weapon_slot = None; p.weapon_uses_left = 0

        upd(p1, g1, r1, c1, rc1, cr1); upd(p2, g2, r2, c2, rc2, cr2)
        for p, g, oid, on, res, c, rc, cr in [
            (p1,g1,uid2,dn2,r1,c1,rc1,cr1), (p2,g2,uid1,dn1,r2,c2,rc2,cr2)]:
            if p and g:
                s.add(MatchHistory(player_id=p.id, opponent_id=oid, opponent_name=on,
                    result=res, rounds=rnd, damage_dealt=g.s_dmg,
                    coins_earned=c, crystal_earned=cr, rating_change=rc))
        await s.commit()
        rat1 = p1.rating if p1 else 0; rat2 = p2.rating if p2 else 0
        for p, g, res in [(p1,g1,r1),(p2,g2,r2)]:
            if p and g: await upd_quests(s, p, res, g)

    lg1 = get_league(rat1); lg2 = get_league(rat2); bk = back_kb()

    if result == "draw":
        dm = (f"🤝 <b>DURANG!</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
              f"{dn1}: +{c1}🪙 +{cr1}💎\n{dn2}: +{c2}🪙 +{cr2}💎")
        for uid in (uid1, uid2):
            await bot.send_message(uid, dm, reply_markup=ReplyKeyboardRemove())
            await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=bk)
        return

    wu, lu   = (uid1, uid2) if result=="uid1_wins" else (uid2, uid1)
    wdn,ldn  = (dn1, dn2)   if result=="uid1_wins" else (dn2, dn1)
    wc,wcr   = (c1, cr1)    if result=="uid1_wins" else (c2, cr2)
    lc,lcr   = (c2, cr2)    if result=="uid1_wins" else (c1, cr1)
    wlg,wrat = (lg1,rat1)   if result=="uid1_wins" else (lg2,rat2)
    llg,lrat = (lg2,rat2)   if result=="uid1_wins" else (lg1,rat1)

    await bot.send_message(wu,
        f"🎗 <b>Siz g'alaba qozondingiz!</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"{wdn}: +{wc}🪙 +{wcr}💎\n🏅 {wlg['name']}  ({wrat} ball)",
        reply_markup=ReplyKeyboardRemove())
    await bot.send_message(wu, "Keyingi jang uchun:", reply_markup=bk)
    await bot.send_message(lu,
        f"🎗 <b>Siz mag'lubiyatga uchradingiz!</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"{ldn}: +{lc}🪙 +{lcr}💎\n🏅 {llg['name']}  ({lrat} ball)",
        reply_markup=ReplyKeyboardRemove())
    await bot.send_message(lu, "Keyingi jang uchun:", reply_markup=bk)

# ═══════════════════════════════════════════════════════════
#  ODDIY BOSS
# ═══════════════════════════════════════════════════════════
async def boss_send_turn(uid: int, bot: Bot):
    bs = boss_games.get(uid)
    if not bs: return
    await bot.send_message(uid,
        f"━━━━━━━━━━━━━━━━━━━━━\n⚔️ <b>{bs.rnd + 1}-RAUNT</b>\n"
        f"👤 Siz: {bs.player_bar()}  <b>{bs.player_hp}/{MAX_HP}</b>\n"
        f"{bs.boss_name}: {bs.boss_bar()}  <b>{bs.boss_hp}/{bs.boss['hp']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n🗡 Hujum zonasini tanlang:\n⏰ {TURN_TIMEOUT} soniya",
        reply_markup=boss_attack_kb())
    async def _w():
        if uid in boss_games: await bot.send_message(uid, f"⚠️ <b>{WARN_AT} soniya qoldi!</b>")
    async def _e():
        if uid in boss_games:
            await bot.send_message(uid, "⏰ <b>Vaqt tugadi!</b>", reply_markup=ReplyKeyboardRemove())
            await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=back_kb())
            boss_games.pop(uid, None)
    await start_timeout(uid, _e, _w)

async def boss_process_attack(uid: int, zone: str, bot: Bot):
    bs = boss_games.get(uid)
    if not bs: return
    cancel_timeout(uid); bs.rnd += 1
    boss = bs.boss; lines = []
    bs.hit_zones[zone] = bs.hit_zones.get(zone, 0) + 1
    boss_def = bs.ai_defense()
    if BLOCK_MAP.get(zone) == boss_def:
        lines.append(f"🛡 <b>{boss['name']}</b> zarbani blokladi!")
        p_dmg = 0
    else:
        p_dmg = random.randint(BASE_DMG_MIN, BASE_DMG_MAX)
        tpl   = random.choice(ATTACK_STORIES.get(zone, ["{atk} {dfn}ga {dmg} zarb berdi!"]))
        pname = name_cache.get(uid, f"#{uid}")
        lines.append(f"⚔ {build_story(tpl, pname, boss['name'], p_dmg, zone)}")
        bs.boss_hp = max(0, bs.boss_hp - p_dmg)

    boss_zone = bs.ai_attack(); bs.last_player_zone = zone
    b_blocked = BLOCK_MAP.get(boss_zone) == random.choice(DEFENSE_ZONES) and random.random() < 0.3
    if b_blocked:
        lines.append(f"🛡 Siz bossning {boss_zone.lower()} zarbini blokladingiz!")
        b_dmg = 0
    else:
        b_dmg = random.randint(boss["dmg_min"], boss["dmg_max"])
        lines.append(f"💢 <b>{boss['name']}</b> sizning {boss_zone.lower()} joyingizga <b>{b_dmg}</b> zarba berdi!")
        bs.player_hp = max(0, bs.player_hp - b_dmg)

    msg = (f"📊 <b>{bs.rnd}-raund</b>\n" + "\n".join(lines) + "\n\n"
           f"👤 Siz: {bs.player_bar()}  <b>{bs.player_hp}/{MAX_HP} HP</b>\n"
           f"{boss['name']}: {bs.boss_bar()}  <b>{bs.boss_hp}/{boss['hp']} HP</b>")

    if bs.boss_hp <= 0:
        await bot.send_message(uid, msg, reply_markup=ReplyKeyboardRemove())
        await _boss_win(uid, bot); return
    if bs.player_hp <= 0:
        await bot.send_message(uid, msg, reply_markup=ReplyKeyboardRemove())
        await _boss_lose(uid, bot); return
    await bot.send_message(uid, msg)
    await boss_send_turn(uid, bot)

async def _boss_win(uid: int, bot: Bot):
    bs = boss_games.pop(uid, None)
    if not bs: return
    coins = bs.boss["coins"]; crystal = bs.boss["crystal"]
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if p:
            p.coins += coins; p.crystal = (p.crystal or 0) + crystal
            p.boss_wins = (p.boss_wins or 0) + 1
            p.boss_today = (p.boss_today or 0) + 1
            p.boss_date = datetime.date.today().isoformat(); await s.commit()
            await upd_quests(s, p, "boss_win", GameState(tid=uid))
    await bot.send_message(uid,
        f"🏆 <b>{bs.boss_name} yengildi!</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 +{coins}  💎 +{crystal}", reply_markup=ReplyKeyboardRemove())
    await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=back_kb())

async def _boss_lose(uid: int, bot: Bot):
    bs = boss_games.pop(uid, None)
    if not bs: return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if p:
            p.boss_today = (p.boss_today or 0) + 1
            p.boss_date = datetime.date.today().isoformat(); await s.commit()
    await bot.send_message(uid, f"💀 <b>{bs.boss_name} sizi yengdi!</b>",
                           reply_markup=ReplyKeyboardRemove())
    await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=back_kb())

# ═══════════════════════════════════════════════════════════
#  SPLINTER BOSS
# ═══════════════════════════════════════════════════════════
async def splinter_send_turn(uid: int, bot: Bot):
    sp = splinter_games.get(uid)
    if not sp: return
    msg = (f"Raunt {sp.rnd + 1}\n"
           f"{sp.hp_bar()}|{sp.player_hp} jon. Maksimum: {MAX_HP}\n"
           f"{sp.energy_bar()} |{sp.player_energy} energiya. Maksimum: {SPLINTER_MAX_ENERGY}\n"
           f"🎯 | Nishonga tegish ehtimolligi - {sp.hit_chance()}%\n")
    async def _w():
        if uid in splinter_games: await bot.send_message(uid, f"⚠️ <b>{WARN_AT} soniya qoldi!</b>")
    async def _e():
        if uid in splinter_games:
            splinter_games.pop(uid, None)
            await bot.send_message(uid, "⏰ <b>Vaqt tugadi!</b> Splinter g'olib!",
                                   reply_markup=ReplyKeyboardRemove())
            await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=back_kb())
    await bot.send_message(uid, msg, reply_markup=splinter_action_kb(sp))
    await start_timeout(uid, _e, _w)

async def splinter_process_action(uid: int, action: str, bot: Bot):
    sp = splinter_games.get(uid)
    if not sp: return
    cancel_timeout(uid)
    pname  = name_cache.get(uid, f"#{uid}")
    sp.rnd += 1
    lines  = [f"Raunt {sp.rnd}:", f"1-Guruh - {pname}", f"2-Guruh - 🐀|Splinter"]
    player_dmg = 0; splinter_dmg = 0

    # O'yinchi harakati
    if action == "zarb":
        if sp.player_energy < 2:
            lines.append(f"❌ Energiya yetarli emas! (kerak: 2⚡)")
            sp.player_last_action = "dam"
        else:
            sp.player_energy -= 2; sp.player_last_action = "zarb"
            hit_roll = random.randint(1, 100); hit_ch = sp.hit_chance(); w = sp.weapon
            if hit_roll <= hit_ch:
                dmg_min, dmg_max = w["dmg"]; player_dmg = random.randint(dmg_min, dmg_max)
                if sp.weapon_key == "pichoq" and random.random() < 0.4:
                    lines.append(f"🔪 <b>{pname}</b> Splinterni zaharli pichoq bilan urdi! <b>+{player_dmg}</b> zarar, zahar!")
                    sp.splinter_hp = max(0, sp.splinter_hp - player_dmg - 2)
                    lines.append(f"☠️ Zahar ta'sir qildi! Splinter yana -2 HP!")
                elif sp.weapon_key == "gurzi" and random.random() < 0.25:
                    lines.append(f"🔨 <b>{pname}</b> Splinterni gurzi bilan urdi! <b>+{player_dmg}</b> zarar + STUN!")
                    sp.splinter_hp = max(0, sp.splinter_hp - player_dmg)
                    lines.append(f"😵 Splinter karaxt bo'ldi! Bu raund hujum qilolmaydi.")
                    sp.total_dmg_dealt += player_dmg
                    lines.append(f"\nRaunt natijalari {sp.rnd}:")
                    lines.append(f"❗ Guruh <b>{pname}</b> ko'proq zarar yetkazdi!")
                    if player_dmg > 0:
                        lines.append(f"{sp.splinter_hp_bar()} |🐀|Splinter yo'qotayabdi {player_dmg} jon. Qoldi {sp.splinter_hp} jon.")
                    await bot.send_message(uid, "\n".join(lines))
                    if sp.splinter_hp <= 0:
                        await splinter_win(uid, bot)
                    else:
                        await splinter_send_turn(uid, bot)
                    return
                else:
                    tpl = random.choice(ATTACK_STORIES.get("Tana", ["{atk} {dfn}ga {dmg} zarb berdi!"]))
                    lines.append(f"👊 {build_story(tpl, pname, 'Splinter', player_dmg, 'Tana')}")
                    sp.splinter_hp = max(0, sp.splinter_hp - player_dmg)
                sp.total_dmg_dealt += player_dmg
            else:
                lines.append(f"💨 <b>{pname}</b> {sp.weapon['name']} bilan urdi — Splinter qochib ketdi! (Miss)")
    elif action == "yaqin":
        if sp.player_energy < 1:
            lines.append(f"❌ Energiya yetarli emas!"); sp.player_last_action = "dam"
        else:
            sp.player_energy -= 1; sp.player_last_action = "yaqin"
            idx = POSITIONS.index(sp.position)
            sp.position = POSITIONS[max(0, idx - 1)]
            lines.append(f"👣 <b>{pname}</b> Splintarga yaqinlashmoqda. ({POSITION_NAMES[sp.position]})")
    elif action == "chet":
        if sp.player_energy < 1:
            lines.append(f"❌ Energiya yetarli emas!"); sp.player_last_action = "dam"
        else:
            sp.player_energy -= 1; sp.player_last_action = "chet"
            idx = POSITIONS.index(sp.position)
            sp.position = POSITIONS[min(len(POSITIONS)-1, idx + 1)]
            lines.append(f"🏃 <b>{pname}</b> Splinterdan chetlashmoqda. ({POSITION_NAMES[sp.position]})")
    elif action == "dam":
        sp.player_energy = SPLINTER_MAX_ENERGY; sp.player_last_action = "dam"
        lines.append(f"🧘 <b>{pname}</b> damolib energiyasini tiklamoqda! ({SPLINTER_MAX_ENERGY})")

    # Splinter AI
    style = random.choice(SPLINTER_STYLES)
    if sp.splinter_double:   style = "double"; sp.splinter_double = False
    if sp.splinter_resting:  sp.splinter_resting = False; sp.splinter_double = True; style = "rest_active"

    if style == "parry" and sp.player_last_action == "zarb" and player_dmg > 0:
        lines.append(f"⚔|🐀|Splinter kontrxujumga tayorlanayabdi.")
        lines.append(f"🪞 Splinter <b>{pname}</b>ning zarbini parry qildi! Zarar qaytdi! (-{player_dmg} HP <b>{pname}</b>ga)")
        sp.player_hp = max(0, sp.player_hp - player_dmg)
        sp.total_dmg_taken += player_dmg; player_dmg = 0
    elif style == "jump":
        idx = POSITIONS.index(sp.position)
        new_idx = random.choice([max(0,idx-2), min(len(POSITIONS)-1, idx+2)])
        sp.position = POSITIONS[new_idx]
        lines.append(f"💨 Splinter sakrab pozitsiyasini o'zgartirdi! ({POSITION_NAMES[sp.position]})")
        s_dmg = random.randint(3, 6)
        lines.append(f"⚔|🐀|Splinter sakrash bilan <b>{pname}</b>ga <b>{s_dmg}</b> zarb berdi!")
        splinter_dmg = s_dmg; sp.player_hp = max(0, sp.player_hp - s_dmg); sp.total_dmg_taken += s_dmg
    elif style in ("rest", "rest_active"):
        lines.append(f"😤|🐀|Splinter nafasini roslayabdi. Energiya maksimalgacha tiklandi! ({SPLINTER_MAX_ENERGY})")
        sp.splinter_resting = True
    elif style == "double":
        s1 = random.randint(2, 5); s2 = random.randint(2, 5); splinter_dmg = s1 + s2
        lines.append(f"⚔|🐀|Splinter ikki marta ketma-ket zarba berdi: <b>{s1}</b> + <b>{s2}</b> = <b>{splinter_dmg}</b> zarar!")
        sp.player_hp = max(0, sp.player_hp - splinter_dmg); sp.total_dmg_taken += splinter_dmg
    else:
        s_dmg_base = random.randint(2, 6)
        if action == "dam": s_dmg_base += 2; lines.append(f"⚔|🐀|Splinter <b>{pname}</b> dam olayotganida hujum qildi!")
        story = random.choice(SPLINTER_ATK_STORIES).format(pname=f"<b>{pname}</b>", dmg=f"<b>{s_dmg_base}</b>")
        lines.append(f"⚔|{story}")
        splinter_dmg = s_dmg_base; sp.player_hp = max(0, sp.player_hp - s_dmg_base); sp.total_dmg_taken += s_dmg_base

    sp.rounds_played += 1

    # Raund natijasi
    lines.append(f"\nRaunt natijalari {sp.rnd}:")
    if player_dmg > 0 and splinter_dmg > 0:
        winner_name = pname if player_dmg > splinter_dmg else "🐀|Splinter"
        lines.append(f"❗|Guruh {winner_name} ko'proq zarar yetkazdi!")
    elif player_dmg == 0 and splinter_dmg == 0:
        lines.append(f"❗|Raundda zarar yetkazilmadi!")
    elif player_dmg == 0:
        lines.append(f"❗|Guruh 🐀|Splinter ko'proq zarar yetkazdi!")
    else:
        lines.append(f"❗|Guruh <b>{pname}</b> ko'proq zarar yetkazdi!")

    if splinter_dmg > 0:
        lines.append(f"{sp.hp_bar()} |<b>{pname}</b> yo'qotayabdi {splinter_dmg} jon. Qoldi {sp.player_hp} jon.")
    if player_dmg > 0:
        lines.append(f"{sp.splinter_hp_bar()} |🐀|Splinter yo'qotayabdi {player_dmg} jon. Qoldi {sp.splinter_hp} jon.")

    if sp.splinter_hp <= 0:
        lines.append(f"\n☠️|🐀|Splinter o'lmoqda.\n💀|🐀|Splinter mag'lubiyatga uchradi!")
        await bot.send_message(uid, "\n".join(lines))
        await splinter_win(uid, bot); return
    if sp.player_hp <= 0:
        lines.append(f"\n☠️|<b>{pname}</b> qulab tushdi.\n💀|🐀|Splinter g'alaba qozondi!")
        await bot.send_message(uid, "\n".join(lines))
        await splinter_lose(uid, bot); return

    await bot.send_message(uid, "\n".join(lines))
    await splinter_send_turn(uid, bot)

async def splinter_win(uid: int, bot: Bot):
    sp = splinter_games.pop(uid, None)
    if not sp: return
    pname = name_cache.get(uid, f"#{uid}")
    katana_exp = datetime.datetime.utcnow() + datetime.timedelta(hours=SPLINTER_KATANA_HOURS)
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if p:
            p.coins += SPLINTER_COINS; p.crystal = (p.crystal or 0) + SPLINTER_CRYSTAL
            p.splinter_wins = (p.splinter_wins or 0) + 1
            p.boss_wins     = (p.boss_wins or 0) + 1
            p.boss_today    = (p.boss_today or 0) + 1
            p.boss_date     = datetime.date.today().isoformat()
            p.katana_expires = katana_exp; await s.commit()
            await upd_quests(s, p, "splinter_win", GameState(tid=uid))
    await send_image(bot, uid, SPLINTER_WIN_IMG, None,
        f"🏆 <b>{pname} g'alaba qozondi!</b>\n\n"
        f"🐀 Splinter mag'lubiyatga uchradi!\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 +{SPLINTER_COINS}  💎 +{SPLINTER_CRYSTAL}\n"
        f"⚔️ <b>Katana</b> {SPLINTER_KATANA_HOURS} soatga sizga tegishli!\n"
        f"💡 Katana PvP jangda +5 zarar beradi")
    await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=back_kb())

async def splinter_lose(uid: int, bot: Bot):
    sp = splinter_games.pop(uid, None)
    if not sp: return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if p:
            p.boss_today = (p.boss_today or 0) + 1
            p.boss_date = datetime.date.today().isoformat(); await s.commit()
    await send_image(bot, uid, SPLINTER_LOSE_IMG, None,
        f"💀 <b>Splinter g'alaba qozondi!</b>\n\n"
        f"«Yana keling, yoshlar» — dedi u jimgina.\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 {sp.rounds_played} raund  •  {sp.total_dmg_dealt} zarar")
    await bot.send_message(uid, "Keyingi jang uchun:", reply_markup=back_kb())

# ═══════════════════════════════════════════════════════════
#  VAZIFALAR
# ═══════════════════════════════════════════════════════════
async def upd_quests(session: AsyncSession, p: Player, result: str, g: GameState):
    today = datetime.date.today().isoformat()
    for q in DAILY_QUESTS:
        r = await session.execute(select(QuestProgress).where(
            QuestProgress.player_id==p.id, QuestProgress.quest_id==q["id"],
            QuestProgress.reset_date==today))
        qp = r.scalar_one_or_none()
        if not qp:
            qp = QuestProgress(player_id=p.id, quest_id=q["id"],
                               reset_date=today, progress=0, completed=False, claimed=False)
            session.add(qp); await session.flush()
        if qp.completed: continue
        if qp.progress is None: qp.progress = 0
        t = q["type"]
        if   t == "wins"          and result in ("win","boss_win","splinter_win"): qp.progress += 1
        elif t == "games_played":                                                   qp.progress += 1
        elif t == "total_damage":                                                   qp.progress += (g.s_dmg if g else 0)
        elif t == "spells_used":                                                    qp.progress += (g.s_spells if g else 0)
        elif t == "poison_used":                                                    qp.progress += (g.s_poison if g else 0)
        elif t == "boss_wins"     and result in ("boss_win","splinter_win"):        qp.progress += 1
        elif t == "splinter_wins" and result == "splinter_win":                     qp.progress += 1
        if (qp.progress or 0) >= q["target"]: qp.completed = True
    await session.commit()

# ═══════════════════════════════════════════════════════════
#  ROUTER & HANDLERLAR
# ═══════════════════════════════════════════════════════════
router = Router()

# ── /start + Ro'yxatdan o'tish ────────────────────────────
@router.message(Command("start"))
async def h_start(msg: Message, state: FSMContext):
    uid = msg.from_user.id; name = msg.from_user.full_name
    async with AsyncSessionLocal() as s:
        p = await db_upsert(s, uid, name, msg.from_user.username)

    # Ro'yxatdan o'tganmi?
    if p.nickname and p.faction and p.gender:
        name_cache[uid]    = p.nickname
        faction_cache[uid] = p.faction
        gender_cache[uid]  = p.gender
        f = FACTIONS[p.faction]
        await msg.answer(
            f"{f['emoji']} <b>Xush kelibsiz, {p.nickname}!</b>\n\n"
            f"<i>{f['motto1']}</i>\n"
            f"<i>{f['motto2']}</i>\n\n"
            f"Menyudan tanlang 👇",
            reply_markup=main_kb
        )
    else:
        # Jins tanlash — birinchi qadam
        await state.set_state(RegState.waiting_gender)
        await msg.answer(
            f"⚔️ <b>FIGHT ARENA</b> ga xush kelibsiz!\n\n"
            f"Avval o'zingiz haqida bir nechta savol.\n\n"
            f"👤 <b>Jinsingizni tanlang:</b>",
            reply_markup=gender_kb()
        )

@router.callback_query(F.data.startswith("reg:gender:"))
async def h_reg_gender(cb: CallbackQuery, state: FSMContext):
    gender = cb.data.split(":")[-1]
    await state.update_data(gender=gender)
    await state.set_state(RegState.waiting_nick)
    await cb.message.edit_text(
        f"✅ {'👦 O\'g\'il' if gender=='male' else '👧 Qiz'} tanlandi!\n\n"
        f"✍️ <b>Nik (taxallus) tanlang:</b>\n\n"
        f"📌 Shartlar:\n"
        f"• 3–10 belgi\n"
        f"• Faqat harf va raqamlar\n"
        f"• Ishoralar va bo'shliq bo'lmasin\n"
        f"• Boshqa o'yinchilarda yo'q bo'lsin\n\n"
        f"Nikingizni yozing:"
    )
    await cb.answer()

@router.message(RegState.waiting_nick)
async def h_reg_nick(msg: Message, state: FSMContext):
    nick = (msg.text or "").strip()
    if not NICK_RE.match(nick):
        await msg.answer(
            f"❌ <b>Noto'g'ri nik!</b>\n• 3–10 belgi\n• Faqat harf+raqam\n"
            f"• Ishoralar yo'q\n\n✍️ Qaytadan yozing:"
        ); return
    async with AsyncSessionLocal() as s:
        if await db_nick_exists(s, nick):
            await msg.answer(f"❌ <b>'{nick}'</b> niki band!\nBoshqa nik tanlang:"); return

    await state.update_data(nick=nick)
    await state.set_state(RegState.waiting_faction)

    # Fraksiya tanlash
    ftext = ""
    for fkey, fdata in FACTIONS.items():
        ftext += f"\n{fdata['emoji']} <b>{fdata['name']}</b> — {fdata['desc']}\n<i>{fdata['motto1']}</i>\n"

    await msg.answer(
        f"✅ Nik: <b>{nick}</b>\n\n"
        f"🏰 <b>Qirolligingizni tanlang:</b>\n"
        f"{ftext}",
        reply_markup=faction_kb()
    )

@router.callback_query(F.data.startswith("reg:faction:"))
async def h_reg_faction(cb: CallbackQuery, state: FSMContext):
    fkey = cb.data.split(":")[-1]
    if fkey not in FACTIONS:
        await cb.answer("❌ Noto'g'ri.", show_alert=True); return

    data   = await state.get_data()
    nick   = data.get("nick")
    gender = data.get("gender", "male")
    uid    = cb.from_user.id

    if not nick:
        await cb.answer("❌ Nik topilmadi. /start qaytadan bosing.", show_alert=True); return

    async with AsyncSessionLocal() as s:
        if await db_nick_exists(s, nick):
            await cb.message.edit_text(f"❌ <b>'{nick}'</b> niki band! /start bosib qaytadan o'ting.")
            await state.clear(); await cb.answer(); return
        p = await db_get(s, uid)
        if p:
            p.nickname = nick; p.gender = gender; p.faction = fkey
            await s.commit()

    name_cache[uid]    = nick
    faction_cache[uid] = fkey
    gender_cache[uid]  = gender
    await state.clear()

    f = FACTIONS[fkey]
    await cb.message.edit_text(
        f"{f['emoji']} <b>Xush kelibsiz, {nick}!</b>\n\n"
        f"Siz <b>{f['name']}</b> qirolligiga qo'shildingiz!\n\n"
        f"<i>{f['motto1']}</i>\n"
        f"<i>{f['motto2']}</i>"
    )
    await cb.answer()
    await cb.message.answer("Menyudan tanlang 👇", reply_markup=main_kb)

# ── /help ────────────────────────────────────────────────
@router.message(Command("help"))
@router.message(F.text == "ℹ️ Yordam")
async def h_help(msg: Message):
    await msg.answer(
        "📖 <b>QO'LLANMA</b>\n\n"
        "<b>⚔️ PvP Jang:</b>\n"
        f"• Hujum, himoya, afsun mustaqil tanlaysiz\n"
        f"• Har bosqich {TURN_TIMEOUT}s  •  {WARN_AT}s ogohlantirish\n\n"
        "<b>🐀 Splinter:</b>\n"
        "• Qurol: bolta/kastet/nayza/pichoq/gurzi/yoy\n"
        "• Pozitsiya: yaqin/o'rta/uzoq\n"
        "• Energiya tizimi (5 max)\n"
        "• G'alaba: 24 soatlik ⚔️ Katana!\n\n"
        "<b>🐉 Oddiy Bosslar:</b>\n"
        f"• 3 ta boss, kuniga {BOSS_DAILY_LIMIT} urinish\n\n"
        "<b>🏰 Fraksiyalar:</b>\n"
        "• 🐺 Stark  •  🐉 Targaryen\n"
        "• 🦁 Lannister  •  🦌 Baratheon\n"
        "• /guruh_top — fraksiya reytingi\n\n"
        "<b>Buyruqlar:</b>\n"
        "/game /cancel /profile /shop\n"
        "/quests /daily /top /league\n"
        "/history /tournament /boss /splinter\n"
        "/guruh_top",
        reply_markup=main_kb
    )

# ── /game ────────────────────────────────────────────────
async def _start_game(uid: int, name: str, uname: str, bot: Bot, ans):
    if uid in games:
        g = games[uid]
        await ans("⏳ Navbatda." if not g.eid else "⚔️ Jangdasiz! /cancel"); return
    if uid in waiting_queue: await ans("⏳ Navbatda. /cancel"); return
    if uid in boss_games or uid in splinter_games: await ans("🐉 Boss jangida turibsiz!"); return

    async with AsyncSessionLocal() as s:
        p = await db_upsert(s, uid, name, uname)
        if not p.nickname or not p.faction:
            await ans("❌ Avval /start orqali ro'yxatdan o'ting!"); return
        name_cache[uid]    = p.nickname
        faction_cache[uid] = p.faction
        gender_cache[uid]  = p.gender or "male"
        ws, as_, ams       = p.weapon_slot, p.armor_slot, p.amulet_slot
        katana_dmg         = 5 if p.has_katana else 0
        my_img             = get_character_image(p.faction, p.gender or "male")

    if waiting_queue:
        eid = waiting_queue.pop(0)
        if eid not in games:
            games[uid] = GameState(tid=uid); waiting_queue.append(uid)
            await ans("✅ O'yinga qo'shildingiz! ⏳\n/cancel"); return

        async with AsyncSessionLocal() as s:
            edb    = await db_get(s, eid)
            ew, ea, eam = (edb.weapon_slot, edb.armor_slot, edb.amulet_slot) if edb else (None,None,None)
            en     = edb.nickname or edb.full_name if edb else f"#{eid}"
            e_fac  = edb.faction  if edb else ""
            e_gen  = edb.gender   if edb else "male"
            e_katana = 5 if edb and edb.has_katana else 0
            e_img  = get_character_image(e_fac, e_gen) if edb else None

        name_cache[eid]    = en
        faction_cache[eid] = e_fac
        gender_cache[eid]  = e_gen

        gs1 = GameState(tid=uid, eid=eid); gs2 = GameState(tid=eid, eid=uid)
        gs1.load_gear(ws, as_, ams); gs1.bonus_dmg += katana_dmg
        gs2.load_gear(ew, ea, eam); gs2.bonus_dmg += e_katana
        games[uid]=gs1; games[eid]=gs2

        udn = get_display_name(uid, name_cache[uid])
        edn = get_display_name(eid, en)
        katana_note = "\n⚔️ Sizda Katana bonusi aktiv!" if katana_dmg else ""

        # Raqibga o'yinchining rasmi
        base_msg = (f"🔥 <b>JANG BOSHLANDI!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Har bosqich mustaqil — raqibni kutmaysiz!\n"
                    f"⏰ {TURN_TIMEOUT}s  •  {WARN_AT}s ogohlantirish\n")

        await send_image(bot, uid, e_img, None,
            f"🎭 <b>RAQIB TOPILDI!</b>\n👤 {edn}  ❤️ {MAX_HP}/{MAX_HP}\n\n{base_msg}{katana_note}")
        await send_image(bot, eid, my_img, None,
            f"🎭 <b>RAQIB TOPILDI!</b>\n👤 {udn}  ❤️ {MAX_HP}/{MAX_HP}\n\n{base_msg}")

        await phase_attack(uid, bot); await phase_attack(eid, bot)
    else:
        games[uid] = GameState(tid=uid); waiting_queue.append(uid)
        await ans("✅ O'yinga qo'shildingiz!\n⏳ Raqib kutilmoqda...\n\n/cancel")

@router.message(Command("game"))
@router.message(F.text == "⚔️ Jang")
async def h_game(msg: Message, bot: Bot):
    await _start_game(msg.from_user.id, msg.from_user.full_name,
                      msg.from_user.username, bot, msg.answer)

# ── /cancel ──────────────────────────────────────────────
@router.message(Command("cancel"))
async def h_cancel(msg: Message, bot: Bot):
    uid = msg.from_user.id; bk = back_kb()
    if uid in waiting_queue:
        waiting_queue.remove(uid); games.pop(uid, None); cancel_timeout(uid)
        await msg.answer("✅ Navbatdan chiqdingiz.", reply_markup=ReplyKeyboardRemove())
        await msg.answer("Keyingi jang uchun:", reply_markup=bk); return
    if uid in splinter_games:
        splinter_games.pop(uid, None); cancel_timeout(uid)
        await msg.answer("🏃 Splinter jangidan chiqdingiz.", reply_markup=ReplyKeyboardRemove())
        await msg.answer("Keyingi jang uchun:", reply_markup=bk); return
    if uid in boss_games:
        boss_games.pop(uid, None); cancel_timeout(uid)
        await msg.answer("🏃 Boss jangidan chiqdingiz.", reply_markup=ReplyKeyboardRemove())
        await msg.answer("Keyingi jang uchun:", reply_markup=bk); return
    if uid not in games:
        await msg.answer("ℹ️ Jangda emassiz.", reply_markup=main_kb); return
    eid = games[uid].eid; cancel_timeout(uid); games.pop(uid, None)
    await msg.answer("🏃 Jangdan chiqdingiz.", reply_markup=ReplyKeyboardRemove())
    await msg.answer("Keyingi jang uchun:", reply_markup=bk)
    if eid and eid in games:
        cancel_timeout(eid); games.pop(eid, None)
        await bot.send_message(eid, "🏃 Raqib chiqdi. 🏆 <b>G'alaba sizniki!</b>",
                               reply_markup=ReplyKeyboardRemove())
        await bot.send_message(eid, "Keyingi jang uchun:", reply_markup=bk)
        async with AsyncSessionLocal() as s:
            ep = await db_get(s, eid)
            if ep:
                ep.wins += 1; ep.coins += COINS_WIN
                ep.crystal = (ep.crystal or 0) + RATING_WIN
                ep.rating = max(0, ep.rating + RATING_WIN)
                ep.league = get_league(ep.rating)["color"]; await s.commit()

# ── Hujum / Himoya / Afsun ───────────────────────────────
@router.message(F.text.in_(ATTACK_ZONES))
async def h_attack(msg: Message, bot: Bot):
    uid = msg.from_user.id
    if uid not in games: return
    g = games[uid]
    if g.attack is not None: await msg.answer("✅ Hujum tanlangan."); return
    if g.defense is not None or g.spell is not None: return
    g.attack = msg.text; cancel_timeout(uid)
    await msg.answer(f"✅ Hujum: <b>{msg.text}</b> ✔\n⏳ Endi himoyani tanlang 👇")
    await phase_defense(uid, bot)

@router.message(F.text.in_(DEFENSE_ZONES))
async def h_defense(msg: Message, bot: Bot):
    uid = msg.from_user.id
    if uid not in games: return
    g = games[uid]
    if g.attack is None: await msg.answer("⚠️ Avval hujumni tanlang!"); return
    if g.defense is not None: await msg.answer("✅ Himoya tanlangan."); return
    if g.spell is not None: return
    g.defense = msg.text; cancel_timeout(uid)
    await msg.answer(f"✅ Himoya: <b>{msg.text}</b> ✔\n⏳ Endi afsun tanlang 👇")
    await phase_spell(uid, bot)

@router.callback_query(F.data.startswith("spell:"))
async def h_spell(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id
    if uid not in games: await cb.answer("❌ Jangda emassiz.", show_alert=True); return
    g = games[uid]
    if g.defense is None: await cb.answer("⚠️ Avval himoya tanlang!", show_alert=True); return
    if g.spell is not None: await cb.answer("✅ Allaqachon tanlangan."); return
    sname = cb.data.split(":", 1)[1]
    if sname == "SKIP":
        g.spell = ("SKIP", None); await cb.message.edit_text("⏭ <b>Afsun o'tkazildi.</b>")
    else:
        sd = ALL_SPELLS.get(sname)
        if not sd: await cb.answer("❌ Noto'g'ri!", show_alert=True); return
        g.spell = (sname, sd); await cb.message.edit_text(f"✅ Afsun: <b>{sname}</b> ✔")
    await cb.answer(); cancel_timeout(uid)
    await try_resolve(uid, bot)

# ── Splinter ─────────────────────────────────────────────
@router.message(Command("splinter"))
@router.message(F.text == "🐀 Splinter")
async def h_splinter_menu(msg: Message):
    uid = msg.from_user.id; today = datetime.date.today().isoformat()
    if uid in games: await msg.answer("⚔️ Avval PvP jangni tugatting!"); return
    if uid in boss_games or uid in splinter_games: await msg.answer("🐉 Allaqachon jangda!"); return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p or not p.nickname: await msg.answer("❌ Avval /start orqali ro'yxatdan o'ting!"); return
        cnt = (p.boss_today or 0) if p.boss_date == today else 0
        katana_txt = f"\n⚔️ Sizda Katana bor! ({p.katana_hours_left} soat)" if p.has_katana else ""
    if BOSS_DAILY_LIMIT - cnt <= 0:
        await msg.answer(f"🐀 ❌ Bugungi {BOSS_DAILY_LIMIT} ta urinish tugadi!{katana_txt}"); return
    await send_image(msg.bot, uid, SPLINTER_FILE_ID, None,
        f"🐀 <b>SPLINTER</b>\n\n{random.choice(SPLINTER_INTRO_TEXTS)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"❤️ HP: {SPLINTER_HP}  •  ⚔️ Katana\n"
        f"Bugun qolgan: <b>{BOSS_DAILY_LIMIT - cnt}/{BOSS_DAILY_LIMIT}</b>\n\n"
        f"🏆 G'alaba: 🪙 +{SPLINTER_COINS}  💎 +{SPLINTER_CRYSTAL}\n"
        f"⚔️ Katana — 24 soatlik bonus (+5 zarar PvP da){katana_txt}\n\n"
        f"Qurolingizni tanlang 👇")
    await msg.answer("Qurol tanlang:", reply_markup=splinter_weapon_kb())

@router.callback_query(F.data.startswith("spl:weapon:"))
async def h_splinter_weapon(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id; wkey = cb.data.split(":")[-1]; today = datetime.date.today().isoformat()
    if wkey not in SPLINTER_WEAPONS: await cb.answer("❌ Noto'g'ri qurol.", show_alert=True); return
    if uid in splinter_games: await cb.answer("🐀 Allaqachon jangda!", show_alert=True); return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p: await cb.answer("❌ Profil topilmadi.", show_alert=True); return
        cnt = (p.boss_today or 0) if p.boss_date == today else 0
        if cnt >= BOSS_DAILY_LIMIT: await cb.answer("❌ Limit tugadi!", show_alert=True); return
    w = SPLINTER_WEAPONS[wkey]; sp = SplinterState(uid=uid, weapon_key=wkey)
    splinter_games[uid] = sp
    await cb.message.edit_text(
        f"✅ Qurol: <b>{w['name']}</b>\n"
        f"📍 Boshlang'ich pozitsiya: {POSITION_NAMES[sp.position]}\n"
        f"⚡ Energiya: {sp.player_energy}/{SPLINTER_MAX_ENERGY}\n\n"
        f"⚔️ Splinter bilan jang boshlandi!")
    await cb.answer()
    await splinter_send_turn(uid, bot)

@router.callback_query(F.data.startswith("spl:act:"))
async def h_splinter_action(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id; action = cb.data.split(":")[-1]
    if uid not in splinter_games: await cb.answer("❌ Splinter jangida emassiz.", show_alert=True); return
    if action == "info":
        sp = splinter_games[uid]; w = sp.weapon
        await cb.answer(
            f"📍 {POSITION_NAMES[sp.position]}\n🎯 {sp.hit_chance()}%\n"
            f"⚡ {sp.player_energy}/{SPLINTER_MAX_ENERGY}\nQurol: {w['name']}",
            show_alert=True); return
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer()
    await splinter_process_action(uid, action, bot)

# ── Boss ─────────────────────────────────────────────────
@router.message(Command("boss"))
@router.message(F.text == "🐉 Boss")
async def h_boss_menu(msg: Message):
    uid = msg.from_user.id; today = datetime.date.today().isoformat()
    if uid in games: await msg.answer("⚔️ Avval PvP jangni tugatting!"); return
    if uid in boss_games or uid in splinter_games: await msg.answer("🐉 Allaqachon jangda!"); return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p or not p.nickname: await msg.answer("❌ Avval /start orqali ro'yxatdan o'ting!"); return
        cnt = (p.boss_today or 0) if p.boss_date == today else 0
    if BOSS_DAILY_LIMIT - cnt <= 0:
        await msg.answer(f"🐉 Bugungi {BOSS_DAILY_LIMIT} ta urinish tugadi! Ertaga keling."); return
    await msg.answer(
        f"🐉 <b>BOSS REJIMI</b>\n\nBugun qolgan: <b>{BOSS_DAILY_LIMIT-cnt}/{BOSS_DAILY_LIMIT}</b>\n\nBoss tanlang:",
        reply_markup=boss_select_kb())

@router.callback_query(F.data.startswith("boss:start:"))
async def h_boss_start(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id; bkey = cb.data.split(":")[-1]; today = datetime.date.today().isoformat()
    if bkey not in BOSSES: await cb.answer("❌ Noto'g'ri boss.", show_alert=True); return
    if uid in games or uid in boss_games or uid in splinter_games:
        await cb.answer("⚔️ Avval joriy jangni tugatting!", show_alert=True); return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        cnt = (p.boss_today or 0) if p and p.boss_date == today else 0
        if cnt >= BOSS_DAILY_LIMIT: await cb.answer("❌ Limit tugadi!", show_alert=True); return
    boss = BOSSES[bkey]; bs = BossState(uid=uid, boss_key=bkey, boss_hp=boss["hp"])
    boss_games[uid] = bs
    await cb.message.edit_text(
        f"🐉 <b>{boss['name']} bilan jang boshlandi!</b>\n{boss['desc']}\n\n"
        f"❤️ Boss HP: <b>{boss['hp']}</b>  •  Zarar: {boss['dmg_min']}–{boss['dmg_max']}")
    await cb.answer(); await boss_send_turn(uid, bot)

@router.callback_query(F.data.startswith("boss:atk:"))
async def h_boss_attack(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id; zone = cb.data.split(":")[-1]
    if uid not in boss_games: await cb.answer("❌ Boss jangida emassiz.", show_alert=True); return
    await cb.answer(); await cb.message.edit_reply_markup(reply_markup=None)
    await boss_process_attack(uid, zone, bot)

# ── Profil ───────────────────────────────────────────────
@router.message(Command("profile"))
@router.message(F.text == "👤 Profil")
async def h_profile(msg: Message):
    uid = msg.from_user.id
    async with AsyncSessionLocal() as s: p = await db_upsert(s, uid, msg.from_user.full_name)
    if not p.nickname: await msg.answer("❌ Avval /start orqali ro'yxatdan o'ting!"); return
    lg   = get_league(p.rating); tot = p.wins + p.losses + p.draws
    wr   = round(p.wins/tot*100) if tot else 0
    f    = FACTIONS.get(p.faction, {}) if p.faction else {}
    wn   = SHOP_ITEMS[p.weapon_slot]["name"] if p.weapon_slot else "Qurolsiz"
    wu   = f" ({p.weapon_uses_left} jang)" if p.weapon_slot and p.weapon_uses_left else ""
    an   = SHOP_ITEMS[p.armor_slot]["name"]  if p.armor_slot  else "Zirh yo'q"
    amn  = SHOP_ITEMS[p.amulet_slot]["name"] if p.amulet_slot else "Talisman yo'q"
    today= datetime.date.today().isoformat()
    bl   = BOSS_DAILY_LIMIT - ((p.boss_today or 0) if p.boss_date == today else 0)
    gender_txt = "👦 O'g'il" if p.gender == "male" else "👧 Qiz"
    katana_line= f"\n⚔️ <b>Katana</b>: {p.katana_hours_left} soat qoldi!" if p.has_katana else ""

    caption = (
        f"{'👦' if p.gender=='male' else '👧'} <b>{p.nickname}</b>  "
        f"{f.get('emoji','')} {f.get('name','')}\n"
        f"{lg['name']}  •  {p.rating} ball  •  {wr}% win\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{f.get('motto1','')}</i>\n"
        f"<i>{f.get('motto2','')}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 {p.wins}  ❌ {p.losses}  🤝 {p.draws}  ({tot} jang)\n"
        f"🐉 Boss: {p.boss_wins or 0}  🐀 Splinter: {p.splinter_wins or 0}\n"
        f"⚔️ Zarar: {p.total_damage}  💚 Shifo: {p.total_healed}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 {p.coins}  💎 {p.crystal or 0}\n"
        f"🗡 {wn}{wu}  •  🛡 {an}  •  🍀 {amn}\n"
        f"🐉 Boss bugun: {bl}/{BOSS_DAILY_LIMIT}"
        f"{katana_line}"
    )

    # Profil rasmi — o'zining fraksiya+jins rasmi
    img = get_character_image(p.faction or "", p.gender or "male")
    await send_image(msg.bot, uid, img, None, caption)

# ── Kunlik ───────────────────────────────────────────────
@router.message(Command("daily"))
async def h_daily(msg: Message):
    uid = msg.from_user.id; now = datetime.datetime.utcnow()
    async with AsyncSessionLocal() as s:
        p = await db_upsert(s, uid, msg.from_user.full_name)
        if p.last_daily and (now - p.last_daily).total_seconds() < 86400:
            rem = 86400 - int((now - p.last_daily).total_seconds())
            h, m = divmod(rem//60, 60)
            await msg.answer(f"⏳ Allaqachon olindingiz.\n🕐 Keyingi: <b>{h}s {m}d</b>"); return
        p.coins += COINS_DAILY; p.last_daily = now; await s.commit()
    await msg.answer(f"🎁 <b>Kunlik bonus!</b>\n🪙 +{COINS_DAILY} tanga!")

# ── Do'kon ───────────────────────────────────────────────
@router.message(Command("shop"))
@router.message(F.text == "🏪 Do'kon")
async def h_shop(msg: Message):
    async with AsyncSessionLocal() as s: p = await db_upsert(s, msg.from_user.id, msg.from_user.full_name)
    await msg.answer(
        f"🏪 <b>DO'KON</b>\n🪙 {p.coins}  💎 {p.crystal or 0}\n\n"
        f"⚔️ Qurollar vaqtli.  💎 Talismanlar kristalga.\n\nKategoriyani tanlang:",
        reply_markup=shop_kb())

@router.callback_query(F.data.startswith("shop:cat:"))
async def h_shop_cat(cb: CallbackQuery):
    cat = cb.data.split(":")[-1]
    cn  = {"weapon":"⚔️ Qurollar","armor":"🛡 Zirh","amulet":"🍀 Talismanlar","potion":"🧪 Iksirlar"}
    async with AsyncSessionLocal() as s: p = await db_get(s, cb.from_user.id)
    await cb.message.edit_text(
        f"🏪 <b>{cn.get(cat,'Do\'kon')}</b>\n🪙 {p.coins if p else 0}  💎 {p.crystal or 0 if p else 0}",
        reply_markup=shop_kb(cat)); await cb.answer()

@router.callback_query(F.data == "shop:back")
async def h_shop_back(cb: CallbackQuery):
    async with AsyncSessionLocal() as s: p = await db_get(s, cb.from_user.id)
    await cb.message.edit_text(
        f"🏪 <b>DO'KON</b>\n🪙 {p.coins if p else 0}  💎 {p.crystal or 0 if p else 0}\n\nKategoriyani tanlang:",
        reply_markup=shop_kb()); await cb.answer()

@router.callback_query(F.data.startswith("shop:buy:"))
async def h_shop_buy(cb: CallbackQuery):
    uid = cb.from_user.id; key = cb.data.split(":")[-1]
    if key not in SHOP_ITEMS: await cb.answer("❌ Topilmadi.", show_alert=True); return
    item = SHOP_ITEMS[key]; uc = item.get("currency") == "crystal"
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p: await cb.answer("❌ Profil topilmadi.", show_alert=True); return
        bal = (p.crystal or 0) if uc else p.coins
        if bal < item["price"]:
            await cb.answer(f"❌ Yetarli {'💎' if uc else '🪙'} yo'q!", show_alert=True); return
        if item["type"] == "potion":
            r = await s.execute(select(Inventory).where(Inventory.player_id==p.id, Inventory.item_key==key))
            inv = r.scalar_one_or_none()
            if inv: inv.quantity += 1
            else: s.add(Inventory(player_id=p.id, item_key=key))
        else:
            r = await s.execute(select(Inventory).where(Inventory.player_id==p.id, Inventory.item_key==key))
            if not r.scalar_one_or_none(): s.add(Inventory(player_id=p.id, item_key=key))
        if uc: p.crystal -= item["price"]
        else:  p.coins   -= item["price"]
        await s.commit()
    await cb.message.edit_text(
        f"✅ <b>{item['name']}</b> sotib olindi!\n{'💎' if uc else '🪙'} -{item['price']}",
        reply_markup=equip_kb(key)); await cb.answer(f"✅ {item['name']} sotib olindi!")

@router.callback_query(F.data.startswith("equip:"))
async def h_equip(cb: CallbackQuery):
    uid = cb.from_user.id; key = cb.data.split(":")[-1]
    if key not in SHOP_ITEMS: await cb.answer("❌ Noto'g'ri.", show_alert=True); return
    item = SHOP_ITEMS[key]; t = item["type"]
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p: await cb.answer("❌ Profil topilmadi.", show_alert=True); return
        if t == "potion":
            if uid not in games or not games[uid].eid:
                await cb.answer("⚠️ Iksirni faqat jangda ishlatish mumkin!", show_alert=True); return
            r = await s.execute(select(Inventory).where(Inventory.player_id==p.id, Inventory.item_key==key))
            inv = r.scalar_one_or_none()
            if not inv or inv.quantity < 1: await cb.answer("❌ Inventarda yo'q!", show_alert=True); return
            h = item.get("heal", 0); games[uid].hp = min(MAX_HP, games[uid].hp + h)
            inv.quantity -= 1
            if inv.quantity == 0: await s.delete(inv)
            await s.commit(); await cb.answer(f"💊 +{h} HP!", show_alert=True); return
        if t == "weapon": p.weapon_slot = key; p.weapon_uses_left = item.get("uses", 0)
        elif t == "armor":  p.armor_slot  = key
        elif t == "amulet": p.amulet_slot = key
        await s.commit()
    ul = f" ({item['uses_label']})" if item.get("uses_label") else ""
    await cb.message.edit_text(f"✅ <b>{item['name']}</b> kiyildi!{ul}")
    await cb.answer(f"✅ {item['name']} kiyildi!")

# ── Vazifalar ────────────────────────────────────────────
@router.message(Command("quests"))
@router.message(F.text == "📋 Vazifalar")
async def h_quests(msg: Message):
    uid = msg.from_user.id; today = datetime.date.today().isoformat()
    async with AsyncSessionLocal() as s:
        p = await db_upsert(s, uid, msg.from_user.full_name)
        r = await s.execute(select(QuestProgress).where(
            QuestProgress.player_id==p.id, QuestProgress.reset_date==today))
        pmap = {qp.quest_id: qp for qp in r.scalars().all()}
    qlist = []
    for q in DAILY_QUESTS:
        qp = pmap.get(q["id"])
        qlist.append({"id": q["id"], "desc": q["desc"], "reward": q["reward"],
                      "target": q["target"], "progress": qp.progress or 0 if qp else 0,
                      "completed": qp.completed if qp else False,
                      "claimed":   qp.claimed   if qp else False})
    done = sum(1 for q in qlist if q["completed"])
    await msg.answer(
        f"📋 <b>KUNLIK VAZIFALAR</b>  —  {today}\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Bajarildi: {done}/{len(DAILY_QUESTS)}\n",
        reply_markup=quest_kb(qlist))

@router.callback_query(F.data.startswith("quest:claim:"))
async def h_quest_claim(cb: CallbackQuery):
    uid = cb.from_user.id; qid = cb.data.split(":")[-1]
    today = datetime.date.today().isoformat()
    q = next((x for x in DAILY_QUESTS if x["id"] == qid), None)
    if not q: await cb.answer("❌ Topilmadi.", show_alert=True); return
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p: await cb.answer("❌ Profil topilmadi.", show_alert=True); return
        r = await s.execute(select(QuestProgress).where(
            QuestProgress.player_id==p.id, QuestProgress.quest_id==qid,
            QuestProgress.reset_date==today))
        qp = r.scalar_one_or_none()
        if not qp or not qp.completed: await cb.answer("❌ Bajarilmagan.", show_alert=True); return
        if qp.claimed: await cb.answer("✅ Allaqachon olindi.", show_alert=True); return
        qp.claimed = True; p.coins += q["reward"]; await s.commit()
    await cb.answer(f"🎁 +{q['reward']} tanga!", show_alert=True)

@router.callback_query(F.data == "noop")
async def h_noop(cb: CallbackQuery): await cb.answer()

# ── Tarix ────────────────────────────────────────────────
@router.message(Command("history"))
async def h_history(msg: Message): await _show_hist(msg, msg.from_user.id, 0, False)

@router.callback_query(F.data.startswith("hist:"))
async def h_hist_page(cb: CallbackQuery):
    await _show_hist(cb.message, cb.from_user.id, int(cb.data.split(":")[1]), True)
    await cb.answer()

async def _show_hist(msg, uid: int, page: int, edit: bool):
    async with AsyncSessionLocal() as s:
        p = await db_get(s, uid)
        if not p:
            t = "❌ Profil topilmadi."
            await (msg.edit_text(t) if edit else msg.answer(t)); return
        r = await s.execute(select(MatchHistory).where(MatchHistory.player_id==p.id)
            .order_by(MatchHistory.played_at.desc()).offset(page*5).limit(5))
        rows = list(r.scalars().all())
    if not rows:
        t = "📜 Tarix bo'sh."; await (msg.edit_text(t) if edit else msg.answer(t)); return
    lines = [f"📜 <b>JANG TARIXI</b>  ({page+1}-bet)\n"]
    for m in rows:
        icon = "🏆" if m.result=="win" else ("🤝" if m.result=="draw" else "❌")
        rc   = f"+{m.rating_change}" if m.rating_change >= 0 else str(m.rating_change)
        dt   = m.played_at.strftime("%d.%m %H:%M") if m.played_at else "—"
        lines.append(f"{icon} <b>{m.opponent_name}</b>\n"
                     f"   📊 {m.rounds} raund  •  {m.damage_dealt} zarar\n"
                     f"   +{m.coins_earned}🪙  +{m.crystal_earned}💎  {rc} ball  •  {dt}\n")
    t = "\n".join(lines); kb = hist_kb(page)
    await (msg.edit_text(t, reply_markup=kb) if edit else msg.answer(t, reply_markup=kb))

# ── TOP & Liga ───────────────────────────────────────────
@router.message(Command("top"))
@router.message(F.text == "👑 Reyting")
async def h_top(msg: Message):
    async with AsyncSessionLocal() as s: pl = await db_top(s, 10)
    if not pl: await msg.answer("📊 Hali o'yinchilar yo'q. /game"); return
    medals = {1:"🥇",2:"🥈",3:"🥉"}
    lines  = ["👑 <b>TOP 10 JANGCHILAR</b>\n━━━━━━━━━━━━━━━━━━━━━"]
    for i, p in enumerate(pl, 1):
        lg  = get_league(p.rating); tot = p.wins+p.losses+p.draws; wr = round(p.wins/tot*100) if tot else 0
        nm  = f"{faction_flag(p.faction)}{p.nickname or p.full_name}" if p.faction else (p.nickname or p.full_name)
        katana = " ⚔️" if p.has_katana else ""
        lines.append(f"{medals.get(i,f'{i}.')} <b>{nm}</b>{katana}\n   {lg['name']}  •  {p.rating} ball  •  {wr}% win")
    await msg.answer("\n".join(lines), reply_markup=main_kb)

@router.message(Command("league"))
async def h_league(msg: Message):
    async with AsyncSessionLocal() as s: p = await db_get(s, msg.from_user.id)
    if not p: await msg.answer("Avval /start qiling."); return
    cur = get_league(p.rating)
    lines = [f"🏅 <b>LIGA</b>  {cur['name']} ({p.rating} ball)\n"]
    for lg in LEAGUES:
        m = "◀️" if lg["color"]==cur["color"] else "  "
        lines.append(f"{m} {lg['name']} — {lg['min']}+ ball")
    await msg.answer("\n".join(lines))

# ── Guruh reytingi ───────────────────────────────────────
@router.message(Command("guruh_top"))
@router.message(F.text == "🏰 Guruhlar")
async def h_faction_top(msg: Message):
    async with AsyncSessionLocal() as s:
        data = await db_faction_top(s)

    # Rating bo'yicha tartiblash
    sorted_factions = sorted(data.items(), key=lambda x: x[1]["total"], reverse=True)
    medals = {0:"🥇", 1:"🥈", 2:"🥉"}
    lines  = ["🏰 <b>QIROLLIKLAR REYTINGI</b>\n━━━━━━━━━━━━━━━━━━━━━"]

    for rank, (fkey, fdata) in enumerate(sorted_factions):
        f    = FACTIONS[fkey]
        med  = medals.get(rank, f"{rank+1}.")
        avg  = fdata["total"] // fdata["count"] if fdata["count"] > 0 else 0
        lines.append(
            f"{med} {f['emoji']} <b>{f['name']}</b>\n"
            f"   👥 {fdata['count']} jangchi  •  ⭐ {fdata['total']} jami ball\n"
            f"   📊 O'rtacha: {avg} ball  •  <i>{f['motto1']}</i>"
        )

    await msg.answer("\n".join(lines), reply_markup=main_kb)

# ── Stats ─────────────────────────────────────────────────
@router.message(Command("stats"))
async def h_stats(msg: Message):
    uid = msg.from_user.id
    if uid in splinter_games:
        sp = splinter_games[uid]
        await msg.answer(
            f"🐀 <b>Splinter jang</b>\n\n"
            f"📍 {POSITION_NAMES[sp.position]}\n"
            f"⚡ {sp.player_energy}/{SPLINTER_MAX_ENERGY}\n"
            f"❤️ Siz: {sp.player_hp}/{MAX_HP}\n"
            f"🔴 Splinter: {sp.splinter_hp}/{SPLINTER_HP}\n"
            f"🔄 Raund: {sp.rnd}"); return
    if uid in boss_games:
        bs = boss_games[uid]
        await msg.answer(
            f"🐉 <b>{bs.boss_name}</b>\n\n"
            f"❤️ Siz: {bs.player_hp}/{MAX_HP}\n"
            f"🔴 Boss: {bs.boss_hp}/{bs.boss['hp']}\n"
            f"🔄 Raund: {bs.rnd}"); return
    g = games.get(uid)
    if g and g.eid:
        en = name_cache.get(g.eid, f"#{g.eid}")
        fn = faction_flag(faction_cache.get(g.eid, ""))
        await msg.answer(
            f"📊 <b>PvP jang</b>\n\n"
            f"👤 Raqib: {fn}<b>{en}</b>\n"
            f"{g.hp_bar}  <b>{g.hp}/{MAX_HP}</b>\n"
            f"🔄 Raund: {g.rnd}\n🎯 Zarar: {g.s_dmg}")
    else:
        await msg.answer("💤 Jangda emassiz. /game bilan boshlang!")

# ── Turnir ───────────────────────────────────────────────
@router.message(Command("tournament"))
@router.message(F.text == "🏆 Turnir")
async def h_tour(msg: Message):
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(Tournament).where(Tournament.status=="waiting")
            .order_by(Tournament.created_at.desc()).limit(1))
        t = r.scalar_one_or_none()
        if not t:
            t = Tournament(status="waiting", max_players=8)
            s.add(t); await s.commit(); await s.refresh(t)
        r2 = await s.execute(select(TournamentParticipant).where(TournamentParticipant.tournament_id==t.id))
        cnt = len(list(r2.scalars().all()))
    await msg.answer(
        f"🏆 <b>TURNIR #{t.id}</b>\n\n"
        f"📊 {'⏳ To\'planmoqda' if t.status=='waiting' else '⚔️ Boshlandi'}\n"
        f"👥 {cnt}/{t.max_players}\n\n"
        f"🥇 +{TOUR_PRIZE[0]}🪙  🥈 +{TOUR_PRIZE[1]}🪙  🥉 +{TOUR_PRIZE[2]}🪙",
        reply_markup=tour_kb(t.id, cnt < t.max_players))

@router.callback_query(F.data.startswith("tour:join:"))
async def h_tour_join(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id; tid = int(cb.data.split(":")[-1])
    if uid in games: await cb.answer("⚠️ Avval jangni tugatting.", show_alert=True); return
    async with AsyncSessionLocal() as s:
        p = await db_upsert(s, uid, cb.from_user.full_name)
        if not p.nickname: await cb.answer("❌ Avval /start orqali ro'yxatdan o'ting!", show_alert=True); return
        r = await s.execute(select(Tournament).where(Tournament.id==tid))
        t = r.scalar_one_or_none()
        if not t or t.status!="waiting": await cb.answer("❌ Boshlangan.", show_alert=True); return
        r2 = await s.execute(select(TournamentParticipant).where(
            TournamentParticipant.tournament_id==tid, TournamentParticipant.player_id==p.id))
        if r2.scalar_one_or_none(): await cb.answer("✅ Allaqachon.", show_alert=True); return
        s.add(TournamentParticipant(tournament_id=tid, player_id=p.id))
        r3 = await s.execute(select(TournamentParticipant).where(TournamentParticipant.tournament_id==tid))
        cnt = len(list(r3.scalars().all())) + 1
        if cnt >= t.max_players: t.status="active"; t.started_at=datetime.datetime.utcnow()
        await s.commit()
    await cb.answer(f"✅ Qo'shildingiz! ({cnt}/{t.max_players})", show_alert=True)
    await cb.message.edit_text(
        f"✅ <b>Turnirga qo'shildingiz!</b>\n👥 {cnt}/{t.max_players}\n"
        f"{'🔥 Turnir boshlandi!' if cnt>=t.max_players else '⏳ Qolganlarni kutmoqda...'}")
    if cnt >= t.max_players: asyncio.create_task(_start_tour(tid, bot))

@router.callback_query(F.data.startswith("tour:list:"))
async def h_tour_list(cb: CallbackQuery):
    tid = int(cb.data.split(":")[-1])
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(TournamentParticipant, Player)
            .join(Player, TournamentParticipant.player_id==Player.id)
            .where(TournamentParticipant.tournament_id==tid))
        rows = r.all()
    lines = [f"👥 Turnir #{tid}:"]
    for i, (tp, p) in enumerate(rows, 1):
        nm = f"{faction_flag(p.faction)}{p.nickname or p.full_name}"
        lines.append(f"{i}. {nm}")
    await cb.answer("\n".join(lines[:10]), show_alert=True)

async def _start_tour(tid: int, bot: Bot):
    await asyncio.sleep(3)
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(TournamentParticipant, Player)
            .join(Player, TournamentParticipant.player_id==Player.id)
            .where(TournamentParticipant.tournament_id==tid))
        rows = list(r.all())
    random.shuffle(rows)
    for i in range(0, len(rows)-1, 2):
        _, p1 = rows[i]; _, p2 = rows[i+1]
        gs1 = GameState(tid=p1.telegram_id, eid=p2.telegram_id, tournament_id=tid)
        gs2 = GameState(tid=p2.telegram_id, eid=p1.telegram_id, tournament_id=tid)
        gs1.load_gear(p1.weapon_slot, p1.armor_slot, p1.amulet_slot)
        gs2.load_gear(p2.weapon_slot, p2.armor_slot, p2.amulet_slot)
        games[p1.telegram_id]=gs1; games[p2.telegram_id]=gs2
        n1 = p1.nickname or p1.full_name; n2 = p2.nickname or p2.full_name
        name_cache[p1.telegram_id]=n1; name_cache[p2.telegram_id]=n2
        faction_cache[p1.telegram_id]=p1.faction or ""; faction_cache[p2.telegram_id]=p2.faction or ""
        dn1 = get_display_name(p1.telegram_id, n1); dn2 = get_display_name(p2.telegram_id, n2)
        await bot.send_message(p1.telegram_id, f"🏆 <b>TURNIR #{tid} BOSHLANDI!</b>\nRaqib: <b>{dn2}</b>\n⚔️ Jang!")
        await bot.send_message(p2.telegram_id, f"🏆 <b>TURNIR #{tid} BOSHLANDI!</b>\nRaqib: <b>{dn1}</b>\n⚔️ Jang!")
        await phase_attack(p1.telegram_id, bot); await phase_attack(p2.telegram_id, bot)

# ── Go callbacks ─────────────────────────────────────────
@router.callback_query(F.data == "go:game")
async def h_go_game(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    await _start_game(cb.from_user.id, cb.from_user.full_name,
                      cb.from_user.username, bot, cb.message.answer)

@router.callback_query(F.data == "go:menu")
async def h_go_menu(cb: CallbackQuery):
    await cb.answer()
    await cb.message.answer("🏟 <b>Bosh menyu</b> 👇", reply_markup=main_kb)

# ═══════════════════════════════════════════════════════════
#  ADMIN BUYRUQLARI
# ═══════════════════════════════════════════════════════════
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

@router.message(Command("admin"))
async def h_admin(msg: Message):
    if not is_admin(msg.from_user.id): await msg.answer("❌ Ruxsat yo'q."); return
    active = sum(1 for g in games.values() if g.eid)
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(sqlfunc.count()).select_from(Player))
        total = r.scalar()
    await msg.answer(
        f"🔧 <b>ADMIN PANEL</b>\n\n"
        f"👥 O'yinchilar: {total}\n⚔️ PvP: {active//2}\n"
        f"🐉 Boss: {len(boss_games)}\n🐀 Splinter: {len(splinter_games)}\n"
        f"⏳ Navbat: {len(waiting_queue)}\n\n"
        "<b>Buyruqlar:</b>\n"
        "/admin_reset [id] — stats+coins+rating 0\n"
        "/admin_reset_stats [id] — faqat wins/losses/damage 0\n"
        "/admin_delete [id] — to'liq o'chirish\n"
        "/admin_reset_all — hammani reset (tasdiqlash bilan)\n"
        "/admin_transfer [from] [to] — ma'lumot o'tkazish (tasdiqlash bilan)\n"
        "/admin_coins [id] [miqdor]\n"
        "/admin_crystal [id] [miqdor]\n"
        "/admin_clear — navbat tozalash\n"
        "/admin_broadcast [matn]\n"
        "/get_file_id — rasm file_id olish"
    )

# ── Reset (stats + coins + rating) ───────────────────────
@router.message(Command("admin_reset"))
async def h_admin_reset(msg: Message, bot: Bot):
    if not is_admin(msg.from_user.id): await msg.answer("❌ Ruxsat yo'q."); return
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await msg.answer("Format: /admin_reset [user_id]"); return
    tid = int(parts[1])
    async with AsyncSessionLocal() as s:
        p = await db_get(s, tid)
        if not p: await msg.answer("❌ O'yinchi topilmadi."); return
        pname = p.nickname or p.full_name
        p.wins = 0; p.losses = 0; p.draws = 0
        p.total_damage = 0; p.total_healed = 0
        p.spells_used = 0; p.poison_used = 0
        p.boss_wins = 0; p.splinter_wins = 0
        p.rating = 0; p.league = "bronze"
        p.coins = 0; p.crystal = 0
        p.weapon_slot = None; p.weapon_uses_left = 0
        p.armor_slot = None; p.amulet_slot = None
        p.katana_expires = None
        await s.commit()
    await msg.answer(f"✅ <b>{pname}</b> — stats+coins+rating 0 ga tushirildi.\nNick va tarix saqlanib qoldi.")
    try:
        await bot.send_message(tid,
            f"🔧 <b>Admin tomonidan reset!</b>\n"
            f"Sizning stats, tanga va reyting 0 ga tushirildi.")
    except: pass

# ── Reset faqat stats ─────────────────────────────────────
@router.message(Command("admin_reset_stats"))
async def h_admin_reset_stats(msg: Message, bot: Bot):
    if not is_admin(msg.from_user.id): await msg.answer("❌ Ruxsat yo'q."); return
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await msg.answer("Format: /admin_reset_stats [user_id]"); return
    tid = int(parts[1])
    async with AsyncSessionLocal() as s:
        p = await db_get(s, tid)
        if not p: await msg.answer("❌ O'yinchi topilmadi."); return
        pname = p.nickname or p.full_name
        p.wins = 0; p.losses = 0; p.draws = 0
        p.total_damage = 0; p.total_healed = 0
        p.spells_used = 0; p.poison_used = 0
        p.boss_wins = 0; p.splinter_wins = 0
        await s.commit()
    await msg.answer(f"✅ <b>{pname}</b> — faqat stats 0 ga tushirildi.\nCoins, rating, jihozlar o'zgarmadi.")
    try:
        await bot.send_message(tid, f"🔧 <b>Admin tomonidan stats reset!</b>\nSizning jang statistikangiz 0 ga tushirildi.")
    except: pass

# ── Delete ───────────────────────────────────────────────
@router.message(Command("admin_delete"))
async def h_admin_delete(msg: Message, bot: Bot):
    if not is_admin(msg.from_user.id): await msg.answer("❌ Ruxsat yo'q."); return
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await msg.answer("Format: /admin_delete [user_id]"); return
    tid = int(parts[1])
    async with AsyncSessionLocal() as s:
        p = await db_get(s, tid)
        if not p: await msg.answer("❌ O'yinchi topilmadi."); return
        pname = p.nickname or p.full_name
        await s.delete(p); await s.commit()
    # Keshdan ham o'chirish
    for cache in (name_cache, faction_cache, gender_cache):
        cache.pop(tid, None)
    await msg.answer(f"✅ <b>{pname}</b> ({tid}) to'liq o'chirildi.\nBarcha tarix va inventar o'chirildi.")

# ── Reset All ─────────────────────────────────────────────
@router.message(Command("admin_reset_all"))
async def h_admin_reset_all(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): await msg.answer("❌ Ruxsat yo'q."); return
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(sqlfunc.count()).select_from(Player))
        total = r.scalar()
    await state.set_state(AdminState.confirm_reset_all)
    await msg.answer(
        f"⚠️ <b>DIQQAT!</b>\n\n"
        f"Jami <b>{total}</b> ta o'yinchi bor.\n"
        f"Hammaning stats, coins, rating, jihozlari <b>0 ga tushiriladi</b>.\n"
        f"Nick va tarix saqlanib qoladi.\n\n"
        f"<b>Bu amalni qaytarib bo'lmaydi!</b>\n\n"
        f"Rostan ham bajarmoqchimisiz?",
        reply_markup=confirm_reset_all_kb()
    )

@router.callback_query(F.data == "admin:confirm_reset_all")
async def h_confirm_reset_all(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Ruxsat yo'q.", show_alert=True); return
    await state.clear()
    await cb.message.edit_text("⏳ Reset jarayoni boshlandi...")
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(Player))
        players = list(r.scalars().all())
        for p in players:
            p.wins = 0; p.losses = 0; p.draws = 0
            p.total_damage = 0; p.total_healed = 0
            p.spells_used = 0; p.poison_used = 0
            p.boss_wins = 0; p.splinter_wins = 0
            p.rating = 0; p.league = "bronze"
            p.coins = 0; p.crystal = 0
            p.weapon_slot = None; p.weapon_uses_left = 0
            p.armor_slot = None; p.amulet_slot = None
            p.katana_expires = None
        await s.commit()
    await cb.message.edit_text(f"✅ <b>{len(players)}</b> ta o'yinchi reset qilindi!")
    await cb.answer()

@router.callback_query(F.data == "admin:cancel")
async def h_admin_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("❌ Amal bekor qilindi.")
    await cb.answer()

# ── Transfer ─────────────────────────────────────────────
@router.message(Command("admin_transfer"))
async def h_admin_transfer(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): await msg.answer("❌ Ruxsat yo'q."); return
    parts = msg.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await msg.answer("Format: /admin_transfer [from_id] [to_id]"); return
    from_id = int(parts[1]); to_id = int(parts[2])
    async with AsyncSessionLocal() as s:
        p_from = await db_get(s, from_id)
        p_to   = await db_get(s, to_id)
        if not p_from: await msg.answer(f"❌ from_id ({from_id}) topilmadi."); return
        if not p_to:   await msg.answer(f"❌ to_id ({to_id}) topilmadi."); return
        fn = p_from.nickname or p_from.full_name
        tn = p_to.nickname   or p_to.full_name

    await msg.answer(
        f"⚠️ <b>TRANSFER TASDIQLASH</b>\n\n"
        f"📤 <b>{fn}</b> ({from_id}) ning barcha ma'lumotlari\n"
        f"📥 <b>{tn}</b> ({to_id}) ga o'tkaziladi.\n\n"
        f"<b>{tn}</b> ning mavjud ma'lumotlari <b>o'chiriladi</b>!\n"
        f"Nick va Telegram ID o'zgarmaydi.\n\n"
        f"Tasdiqlaysizmi?",
        reply_markup=confirm_transfer_kb(from_id, to_id)
    )

@router.callback_query(F.data.startswith("admin:confirm_transfer:"))
async def h_confirm_transfer(cb: CallbackQuery, bot: Bot):
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Ruxsat yo'q.", show_alert=True); return
    parts    = cb.data.split(":")
    from_id  = int(parts[2]); to_id = int(parts[3])
    await cb.message.edit_text("⏳ Transfer jarayoni boshlandi...")
    async with AsyncSessionLocal() as s:
        p_from = await db_get(s, from_id)
        p_to   = await db_get(s, to_id)
        if not p_from or not p_to:
            await cb.message.edit_text("❌ O'yinchilardan biri topilmadi."); await cb.answer(); return

        # to ga from ning ma'lumotlarini yozish (nick va telegram_id o'zgarmaydi)
        p_to.wins          = p_from.wins
        p_to.losses        = p_from.losses
        p_to.draws         = p_from.draws
        p_to.total_damage  = p_from.total_damage
        p_to.total_healed  = p_from.total_healed
        p_to.spells_used   = p_from.spells_used
        p_to.poison_used   = p_from.poison_used
        p_to.boss_wins     = p_from.boss_wins
        p_to.splinter_wins = p_from.splinter_wins
        p_to.rating        = p_from.rating
        p_to.league        = p_from.league
        p_to.coins         = p_from.coins
        p_to.crystal       = p_from.crystal
        p_to.weapon_slot      = p_from.weapon_slot
        p_to.weapon_uses_left = p_from.weapon_uses_left
        p_to.armor_slot    = p_from.armor_slot
        p_to.amulet_slot   = p_from.amulet_slot
        p_to.katana_expires= p_from.katana_expires
        p_to.faction       = p_from.faction
        p_to.gender        = p_from.gender
        await s.commit()
        fn = p_from.nickname or p_from.full_name
        tn = p_to.nickname   or p_to.full_name

    await cb.message.edit_text(
        f"✅ <b>Transfer yakunlandi!</b>\n\n"
        f"📤 <b>{fn}</b> ning ma'lumotlari\n"
        f"📥 <b>{tn}</b> ga o'tkazildi.\n"
        f"Nick va Telegram ID o'zgarmagan."
    )
    await cb.answer()
    try:
        await bot.send_message(to_id,
            f"🔧 <b>Admin tomonidan ma'lumot o'tkazildi!</b>\n"
            f"Sizning hisobingiz yangilandi.")
    except: pass

# ── Admin coins / crystal / clear / broadcast ─────────────
@router.message(Command("admin_coins"))
async def h_admin_coins(msg: Message, bot: Bot):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await msg.answer("Format: /admin_coins [user_id] [miqdor]"); return
    tid = int(parts[1]); amount = int(parts[2])
    async with AsyncSessionLocal() as s:
        p = await db_get(s, tid)
        if not p: await msg.answer("❌ O'yinchi topilmadi."); return
        p.coins += amount; await s.commit(); pname = p.nickname or p.full_name
    await msg.answer(f"✅ <b>{pname}</b> ga +{amount} 🪙. Jami: {p.coins}")
    try:
        await bot.send_message(tid,
            f"🎁 <b>Admin tomonidan tanga!</b>\n🪙 Hisobingizga <b>+{amount}</b> tanga tushdi!\n💰 Jami: <b>{p.coins}</b> 🪙")
    except: pass

@router.message(Command("admin_crystal"))
async def h_admin_crystal(msg: Message, bot: Bot):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await msg.answer("Format: /admin_crystal [user_id] [miqdor]"); return
    tid = int(parts[1]); amount = int(parts[2])
    async with AsyncSessionLocal() as s:
        p = await db_get(s, tid)
        if not p: await msg.answer("❌ O'yinchi topilmadi."); return
        p.crystal = (p.crystal or 0) + amount; await s.commit(); pname = p.nickname or p.full_name
    await msg.answer(f"✅ <b>{pname}</b> ga +{amount} 💎. Jami: {p.crystal}")
    try:
        await bot.send_message(tid,
            f"🎁 <b>Admin tomonidan kristal!</b>\n💎 Hisobingizga <b>+{amount}</b> kristal tushdi!\n💎 Jami: <b>{p.crystal}</b>")
    except: pass

@router.message(Command("admin_clear"))
async def h_admin_clear(msg: Message):
    if not is_admin(msg.from_user.id): return
    waiting_queue.clear(); await msg.answer("✅ Navbat tozalandi.")

@router.message(Command("admin_broadcast"))
async def h_admin_bc(msg: Message, bot: Bot):
    if not is_admin(msg.from_user.id): return
    text = msg.text.replace("/admin_broadcast","").strip()
    if not text: await msg.answer("❌ Matn yo'q."); return
    async with AsyncSessionLocal() as s:
        r = await s.execute(select(Player.telegram_id)); ids = [row[0] for row in r.all()]
    ok = 0
    for uid in ids:
        try: await bot.send_message(uid, f"📢 <b>YANGILIK</b>\n\n{text}"); ok += 1
        except: pass
    await msg.answer(f"✅ {ok}/{len(ids)} o'yinchiga yuborildi.")

# ── Rasm file_id olish ────────────────────────────────────
@router.message(Command("get_file_id"))
async def h_get_file_id(msg: Message):
    if not is_admin(msg.from_user.id): return
    await msg.answer(
        "📸 Rasm yuboring, file_id ni olish uchun.\n\n"
        "Fraksiya rasmlari uchun:\n"
        "CHARACTER_IMAGES['stark']['male'] = ...\n"
        "CHARACTER_IMAGES['stark']['female'] = ...\n"
        "va hokazo."
    )

@router.message(F.photo)
async def h_photo_file_id(msg: Message):
    if not is_admin(msg.from_user.id): return
    fid = msg.photo[-1].file_id
    await msg.answer(
        f"✅ <b>File ID:</b>\n<code>{fid}</code>\n\n"
        f"Bu qiymatni CHARACTER_IMAGES yoki SPLINTER_FILE_ID ga yozing."
    )
#  RENDER UCHUN SOXTA WEB SERVER
# ───────────────────────────────────────────────────────────
async def handle(request):
    from aiohttp import web
    return web.Response(text="OK")

async def start_web_server():
    from aiohttp import web
    import os
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Render beradigan portni oladi, bo'lmasa 10000
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    # Log ob'ekti sizda 'log' deb nomlangan bo'lsa:
    try:
        log.info(f"🌐 Web server {port}-portda ishga tushdi")
    except:
        print(f"🌐 Web server {port}-portda ishga tushdi")
# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
async def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN topilmadi! .env faylda BOT_TOKEN=... qo'ying."); return
    
    await init_db()
    logger.info("✅ Database tayyor")

    # --- MANA SHU QISMNI QO'SHING ---
    await start_web_server() 
    # --------------------------------

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    logger.info("⚔️ Fight Arena Bot v7.0 ishga tushdi!")
    
    try:
        # drop_pending_updates=True eski xabarlarni o'chirib yuboradi, bu yaxshi
        await dp.start_polling(bot, allowed_updates=["message","callback_query"], drop_pending_updates=True)
    finally:
        await bot.session.close()
