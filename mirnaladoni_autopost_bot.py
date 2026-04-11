import asyncio
import html
import logging
import os
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import httpx
from dotenv import load_dotenv
from openai import AsyncOpenAI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, ReplyKeyboardMarkup
from telegram.error import Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
TEST_CHANNEL_ID = os.getenv("TEST_CHANNEL_ID", "").strip()
TELEGRAM_ADMIN_ID_RAW = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY", "").strip()
DEFAULT_POST_TIMES = os.getenv("POST_TIMES", "09:00,14:00,19:00").strip()

BOT_TZ = timezone(timedelta(hours=3))
TELEGRAM_ADMIN_ID = int(TELEGRAM_ADMIN_ID_RAW) if TELEGRAM_ADMIN_ID_RAW else None

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHANNEL_ID:
