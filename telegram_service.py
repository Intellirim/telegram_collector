# telegram_service.py
import os, json, asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from fastapi import FastAPI, Query
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

# ===== ENV =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STR = os.getenv("TELETHON_STRING_SESSION")
CHANNELS = os.getenv(
    "CHANNELS",
    "WhaleFollower,cryptoquant_kr,cq_alert_kr,Gorae_Insight,coinnesskr,Gorae_gorae,emperorcoin,insidertracking"
).split(",")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "exports")
os.makedirs(OUTPUT_DIR, exist_ok=True)
LATEST_PATH = os.path.join(OUTPUT_DIR, "latest.json")
CP_PATH = os.path.join(OUTPUT_DIR, "checkpoints.json")   # 채널별 last_id 저장

client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
app = FastAPI(title="Telegram Collector API")

# ===== models =====
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

# ===== checkpoints =====
def load_cp() -> Dict[str, int]:
    if os.path.exists(CP_PATH):
        with open(CP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cp(cp: Dict[str, int]) -> None:
    with open(CP_PATH, "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)

# ===== collectors =====
async def fetch_channel_messages(channel: str, since_dt: datetime, limit: int, min_id: Optional[int]) -> List[dict]:
    out = []
    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        print(f"[WARN] get_entity {channel}: {e}")
        return out

    try:
        # min_id가 있으면 그 이후만(증분). 없으면 since_dt 기준(부트스트랩).
        async for msg in client.iter_messages(entity, limit=None, min_id=min_id):
            if msg.date is None:
                continue
            if min_id is None and msg.date.replace(tzinfo=None) < since_dt.replace(tzinfo=None):
                break
            rec = {
                "channel": channel,
                "id": msg.id,
                "text": getattr(msg, "message", None) or getattr(msg, "text", None),
                "views": getattr(msg, "views", None),
                "forwards": getattr(msg, "forwards", None),
                "reactions": (msg.reactions.to_dict() if getattr(msg, "reactions", None) else None),
                "reply_count": getattr(getattr(msg, "replies", None), "comments", None),
                "date": msg.date.isoformat(),
                "url": f"https://t.me/{channel}/{msg.id}",
            }
            out.append(rec)
            if limit and len(out) >= limit:
                break
        await asyncio.sleep(1)
    except FloodWaitError as fw:
        print(f"[RATE] Flood wait {fw.seconds}s {channel}")
        await asyncio.sleep(fw.seconds + 1)
    except Exception as e:
        print(f"[ERR] iter_messages {channel}: {e}")
    return out

async def collect_all(channels: List[str], since_hours: int = 24, per_channel_limit: int = 500):
    since_dt = datetime.utcnow() - timedelta(hours=since_hours)
    cp = load_cp()                         # {"channel": last_id}
    new_cp = dict(cp)
    results = []

    mode = "incremental" if cp else "bootstrap"
    print(f"[MODE] {mode} collection (since_hours={since_hours})")

    async with client:
        for ch in channels:
            last_id = cp.get(ch) if cp else None
            print(f"[INFO] Fetching {ch} ({'inc' if last_id else 'boot'}) since {since_dt.isoformat()} last_id={last_id}")
            msgs = await fetch_channel_messages(ch, since_dt, per_channel_limit, min_id=last_id)
            print(f"[INFO] {ch} -> {len(msgs)}")
            results.extend(msgs)
            if msgs:
                new_cp[ch] = max(m["id"] for m in msgs)

    # dedupe + save
    final = list({f"{r['channel']}:{r['id']}": r for r in results}.values())
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    outfile = os.path.join(OUTPUT_DIR, f"telegram_messages_{run_ts}.json")
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    save_cp(new_cp)

    print(f"[INFO] Saved {len(final)} -> {outfile}")
    return {"path": outfile, "count": len(final), "mode": mode}

# ===== API =====
@app.get("/telegram/messages")
async def get_messages(since_hours: int = Query(24, ge=1, le=168), refresh: bool = Query(False)):
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
            t = datetime.fromisoformat(m["date"].replace("Z", "+00:00")).replace(tzinfo=None)
            if t >= cutoff:
                filtered.append(m)
        except Exception:
            continue
    return {"source": os.path.basename(LATEST_PATH), "messages": filtered, "count": len(filtered)}

@app.get("/telegram/files")
def list_files():
    files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".json")])
    return {"files": files}

@app.get("/telegram/checkpoints")
def get_checkpoints():
    return load_cp()

# ===== main =====
if __name__ == "__main__":
    import uvicorn, argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    parser.add_argument("--collect-once", action="store_true")
    parser.add_argument("--since-hours", type=int, default=24)
    args = parser.parse_args()

    if args.collect_once and not args.serve:
        asyncio.get_event_loop().run_until_complete(collect_all(CHANNELS, args.since_hours))
    elif args.serve and not args.collect_once:
        uvicorn.run("telegram_service:app", host="0.0.0.0", port=args.port)
    else:
        asyncio.get_event_loop().run_until_complete(collect_all(CHANNELS, args.since_hours))
        uvicorn.run("telegram_service:app", host="0.0.0.0", port=args.port)
