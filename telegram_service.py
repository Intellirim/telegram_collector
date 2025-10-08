# telegram_service.py
import os, json, asyncio
from datetime import datetime, timedelta
from typing import List, Optional
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from fastapi import FastAPI, Query
from pydantic import BaseModel

# ===============================
# 🔑 Telegram API (환경변수 기반)
# ===============================
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "session_main")

# 🔐 Render용 세션 문자열
from telethon.sessions import StringSession
SESSION_STR = os.getenv("TELETHON_STRING_SESSION")
client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)

# ===============================
# 📦 출력 디렉토리/파일
# ===============================
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "exports")
LATEST_PATH = os.path.join(OUTPUT_DIR, "latest.json")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===============================
# 📡 채널 리스트
# ===============================
CHANNELS = os.getenv("CHANNELS", "WhaleFollower,cryptoquant_kr,cq_alert_kr,Gorae_Insight,coinnesskr,Gorae_gorae,emperorcoin,insidertracking").split(",")

# ===============================
# ⚙️ FastAPI 설정
# ===============================
app = FastAPI(title="Telegram Collector API")

class MsgQuery(BaseModel):
    channel: str
    id: int
    text: Optional[str]
    views: Optional[int]
    forwards: Optional[int]
    reactions: Optional[dict]
    reply_count: Optional[int]
    date: str
    url: str

async def fetch_channel_messages(channel: str, since_dt, limit: int = 1000) -> List[dict]:
    out = []
    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        print(f"[WARN] get_entity failed for {channel}: {e}")
        return out

    try:
        async for msg in client.iter_messages(entity, limit=None):
            if msg.date is None:
                continue
            if msg.date.replace(tzinfo=None) < since_dt.replace(tzinfo=None):
                break
            rec = {
                "channel": channel,
                "id": getattr(msg, "id", None),
                "text": getattr(msg, "message", None) or getattr(msg, "text", None),
                "views": getattr(msg, "views", None),
                "forwards": getattr(msg, "forwards", None),
                "reactions": (msg.reactions.to_dict() if getattr(msg, "reactions", None) else None),
                "reply_count": getattr(msg, "replies", None) and getattr(msg.replies, "comments", None),
                "date": msg.date.isoformat(),
                "url": f"https://t.me/{channel}/{getattr(msg, 'id', '')}",
            }
            out.append(rec)
            if limit and len(out) >= limit:
                break
        await asyncio.sleep(1)
    except FloodWaitError as fw:
        print(f"[RATE] Flood wait {fw.seconds}s for {channel}")
        await asyncio.sleep(fw.seconds + 1)
    except Exception as e:
        print(f"[ERR] iter_messages {channel}: {e}")
    return out

async def collect_all(channels: List[str], since_hours: int = 24, per_channel_limit: int = 500):
    since_dt = datetime.utcnow() - timedelta(hours=since_hours)
    results = []
    async with client:
        for ch in channels:
            print(f"[INFO] Fetching {ch} since {since_dt.isoformat()}")
            msgs = await fetch_channel_messages(ch, since_dt, limit=per_channel_limit)
            print(f"[INFO] {ch} -> {len(msgs)} messages")
            results.extend(msgs)

    dedup = {f"{r['channel']}:{r['id']}": r for r in results}
    final = list(dedup.values())

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    outfile = os.path.join(OUTPUT_DIR, f"telegram_messages_{run_ts}.json")

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Saved {len(final)} messages to {outfile}")
    return {"path": outfile, "count": len(final)}

@app.get("/telegram/messages")
async def get_messages(
    since_hours: int = Query(24, ge=1, le=168),
    refresh: bool = Query(False)
):
    if refresh:
        await collect_all(CHANNELS, since_hours)

    if not os.path.exists(LATEST_PATH):
        return {"messages": [], "count": 0, "note": "no data collected"}

    with open(LATEST_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    cutoff = datetime.utcnow() - timedelta(hours=since_hours)
    filtered = []
    for m in data:
        try:
            msg_time = datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
            if msg_time.replace(tzinfo=None) >= cutoff:
                filtered.append(m)
        except Exception:
            continue
    return {"messages": filtered, "count": len(filtered)}

@app.get("/telegram/files")
def list_files():
    files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".json")])
    return {"files": files}

if __name__ == "__main__":
    import uvicorn, argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--collect-once", action="store_true")
    args = parser.parse_args()

    if args.collect_once:
        asyncio.get_event_loop().run_until_complete(collect_all(CHANNELS))
    elif args.serve:
        uvicorn.run("telegram_service:app", host="0.0.0.0", port=args.port)
