import asyncio
import json
import os
import random
from pathlib import Path

from TikTokApi import TikTokApi
import pandas as pd

CHECKPOINT_FILE = Path("checkpoint.json")
OUTPUT_FILE = Path("beauty_kols_data.xlsx")
FAILED_FILE = Path("failed_users.json")

# Set your ms_token from TikTok cookies
# To get ms_token: Log in to TikTok, open dev tools, go to Application > Cookies > tiktok.com > msToken
ms_token = os.environ.get("ms_token", None)
if not ms_token:
    raise ValueError("Please set the ms_token environment variable. Get it from your TikTok cookies.")

MAX_USERS_PER_RUN = int(os.environ.get("MAX_USERS_PER_RUN", "300"))


def load_json(path):
    if path.is_file():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def backoff_sleep(attempt):
    delay = min(60, (2 ** attempt) + random.uniform(0, 2))
    print(f"Waiting {delay:.1f}s before retry...")
    return asyncio.sleep(delay)


async def safe_user_info(api, username, retries=5):
    for attempt in range(retries):
        try:
            user = api.user(username=username)
            user_info = await user.info()
            return user_info
        except Exception as e:
            print(f"[{username}] attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise
            await backoff_sleep(attempt)


async def build_user_list(api, beauty_hashtags, users, videos_per_hashtag):
    for hashtag_name in beauty_hashtags:
        try:
            hashtag = api.hashtag(name=hashtag_name)
            found = 0
            async for video in hashtag.videos(count=videos_per_hashtag):
                if video.author and video.author.username:
                    users.add(video.author.username)
                    found += 1
                # slow crawl with time
                await asyncio.sleep(random.uniform(1.5, 4.0))
                if found >= videos_per_hashtag:
                    break
            print(f"Collected {found} users from #{hashtag_name}")
        except Exception as e:
            print(f"Error with hashtag {hashtag_name}: {e}")
            await asyncio.sleep(10)

    return users


def save_progress(kol_data, failed_users):
    df = pd.DataFrame(kol_data)
    df.to_excel(OUTPUT_FILE, index=False)
    save_json(FAILED_FILE, failed_users)


async def crawl_beauty_kols():
    beauty_hashtags = ["beauty", "makeup", "skincare", "cosmetics", "beautytips"]
    vietnam_hashtags = [
        "beautyvietnam", "beautyvn", "vnbeauty", "kemtrangdiem", "lamdep",
        "sieudep", "skincarevietnam", "khoedep", "hochiminh", "hanoi", "vietnam", "haiphong"
    ]
    # Combine global beauty focus + Vietnam tags
    all_hashtags = beauty_hashtags + vietnam_hashtags
    videos_per_hashtag = 200

    vietnam_only = os.environ.get("VIETNAM_ONLY", "1") in ["1", "true", "True"]

    checkpoint = {
        "processed_users": [],
        "pending_users": [],
        "failed_users": [],
    }

    if CHECKPOINT_FILE.is_file():
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

    kol_data = []
    processed_users = set(checkpoint.get("processed_users", []))
    pending = set(checkpoint.get("pending_users", []))
    failed_users = set(checkpoint.get("failed_users", []))

    if OUTPUT_FILE.is_file():
        old_df = pd.read_excel(OUTPUT_FILE)
        for u in old_df.get("username", []):
            processed_users.add(str(u))
            if u in failed_users:
                failed_users.remove(u)
        kol_data = old_df.to_dict("records")

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token],
            num_sessions=1,
            sleep_after=5,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
            # If available, make ingestion more human-like
            browser_context_args={"userAgent": os.getenv("TIKTOK_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")},
        )

        if not pending:
            pending_users = await build_user_list(api, all_hashtags, set(), videos_per_hashtag)
            pending = pending_users - processed_users

        pending = list(pending)
        if not pending:
            print("No users pending. Check hashtags or token.")
            return

        if len(pending) > MAX_USERS_PER_RUN:
            pending = pending[:MAX_USERS_PER_RUN]

        total = len(pending)
        print(f"Starting processing {total} users (max {MAX_USERS_PER_RUN} per run)")

        for i, username in enumerate(pending, start=1):
            if username in processed_users:
                continue

            try:
                user_info = await safe_user_info(api, username)

                data = {
                    "username": user_info.get("uniqueId", ""),
                    "user_id": user_info.get("id", ""),
                    "sec_uid": user_info.get("secUid", ""),
                    "nickname": user_info.get("nickname", ""),
                    "bio": user_info.get("signature", ""),
                    "verified": user_info.get("verified", False),
                    "private_account": user_info.get("privateAccount", False),
                    "followers": user_info.get("stats", {}).get("followerCount", 0),
                    "following": user_info.get("stats", {}).get("followingCount", 0),
                    "likes": user_info.get("stats", {}).get("heartCount", 0),
                    "videos": user_info.get("stats", {}).get("videoCount", 0),
                    "friends": user_info.get("stats", {}).get("friendCount", 0),
                    "country": user_info.get("country", ""),
                    "city": user_info.get("city", ""),
                }

                is_vietnam = False
                locale_text = " ".join([
                    (user_info.get("country", "") or ""),
                    (user_info.get("city", "") or ""),
                    (user_info.get("signature", "") or ""),
                    (user_info.get("uniqueId", "") or ""),
                    (user_info.get("nickname", "") or ""),
                ]).lower()
                vietnam_tokens = ["vietnam", "vn", "việt", "hanoi", "hochiminh", "saigon", "hcm"]
                if any(tok in locale_text for tok in vietnam_tokens):
                    is_vietnam = True
                data["is_vietnam"] = is_vietnam

                if vietnam_only and not is_vietnam:
                    print(f"Skipped {username}: not identified as Vietnam KOL")
                    processed_users.add(username)
                    continue

                kol_data.append(data)
                processed_users.add(username)

                print(f"Processed {i}/{total} ({username}). Processed total: {len(processed_users)}")

                await asyncio.sleep(random.uniform(3.0, 7.0))

            except Exception as e:
                print(f"Failed {username} after retries: {e}")
                failed_users.add(username)

            if i % 20 == 0 or i == total:
                save_progress(kol_data, list(failed_users))
                new_checkpoint = {
                    "processed_users": list(processed_users),
                    "pending_users": pending[i:],
                    "failed_users": list(failed_users),
                }
                with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                    json.dump(new_checkpoint, f, ensure_ascii=False, indent=2)

        print(f"Crawl complete. Saved {len(kol_data)} KOL profiles. Failed users: {len(failed_users)}")


if __name__ == "__main__":
    asyncio.run(crawl_beauty_kols())