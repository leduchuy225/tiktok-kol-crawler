import asyncio
import json
import os
import random
from pathlib import Path

from dotenv import load_dotenv
from TikTokApi import TikTokApi
import pandas as pd

# Auto-load .env file if present
load_dotenv()


class BotDetectedError(Exception):
    pass


CHECKPOINT_FILE = Path("checkpoint.json")
OUTPUT_FILE = Path("beauty_kols_data.xlsx")
FAILED_FILE = Path("failed_users.json")
# COLLECTED_USERS_FILE = Path("collected_users.json")

# Set your ms_token from TikTok cookies
# To get ms_token: Log in to TikTok, open dev tools, go to Application > Cookies > tiktok.com > msToken
ms_token = os.environ.get("ms_token", None)
if not ms_token:
    raise ValueError(
        "Please set the ms_token environment variable. Get it from your TikTok cookies."
    )

MAX_USERS_PER_RUN = int(os.environ.get("MAX_USERS_PER_RUN", "3"))
HEADLESS = os.environ.get("HEADLESS", "1") in ["1", "true", "True"]
BROWSER_LOCALE = os.environ.get("BROWSER_LOCALE", "vi-VN")
BROWSER_TIMEZONE = os.environ.get("BROWSER_TIMEZONE", "Asia/Ho_Chi_Minh")
# PROXY_URL = os.environ.get("PROXY_URL", "").strip()
RUN_MODE = os.environ.get("RUN_MODE", "collect").lower()  # collect | enrich

if RUN_MODE not in {"collect", "enrich"}:
    raise ValueError("RUN_MODE must be 'collect' or 'enrich'.")


def extract_user_data(user_info):
    user = user_info.get("userInfo", {}).get("user", {})
    stats = user_info.get("userInfo", {}).get("stats", {})
    user_id = str(user.get("id", ""))
    data = {
        "username": user.get("uniqueId", ""),
        "user_id": user_id,
        "sec_uid": user.get("secUid", ""),
        "nickname": user.get("nickname", ""),
        "bio": user.get("signature", ""),
        "verified": user.get("verified", False),
        "private_account": user.get("privateAccount", False),
        "followers": stats.get("followerCount", 0),
        "following": stats.get("followingCount", 0),
        "likes": stats.get("heartCount", 0),
        "videos": stats.get("videoCount", 0),
        "friends": stats.get("friendCount", 0),
        "country": user.get("country", ""),
        "city": user.get("city", ""),
    }

    return data, user_id


def load_json(path):
    if path.is_file():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


async def test_session(api):
    """Test if TikTokApi session is working by fetching trending."""
    try:
        print("Testing TikTokApi session with trending videos...")
        count = 0
        async for video in api.trending.videos(count=1):
            count += 1
            print(f"✓ Session test passed. Got video: {video.id}")
            break
        if count == 0:
            print("⚠ Warning: No trending videos returned. Session might be inactive.")
    except Exception as e:
        print(f"✗ Session test failed: {e}")
        raise


async def heartbeat(interval=30):
    """Print heartbeat to confirm process is still running."""
    count = 0
    while True:
        await asyncio.sleep(interval)
        count += 1
        print(f"[heartbeat] still running ({count * interval}s elapsed)")


def backoff_sleep(attempt):
    delay = min(60, (2**attempt) + random.uniform(0, 2))
    print(f"Waiting {delay:.1f}s before retry...")
    return asyncio.sleep(delay)


async def safe_user_info(api, username, retries=5, timeout=30):
    for attempt in range(retries):
        try:
            user = api.user(username=username)
            # Wrap in timeout to prevent infinite hangs
            user_info = await asyncio.wait_for(user.info(), timeout=timeout)
            # Check if user info is valid (not empty)
            if not user_info or not user_info.get("userInfo"):
                raise BotDetectedError(
                    f"User {username} not found or empty response from TikTok (possible bot detection)"
                )
            return user_info
        except asyncio.TimeoutError:
            print(f"[{username}] attempt {attempt+1} timed out after {timeout}s")
            if attempt == retries - 1:
                raise
            await backoff_sleep(attempt)
        except Exception as e:
            bot_msg = (
                " (possible bot detect; try HEADLESS=0, browser=webkit, or PROXY_URL)"
                if "unexpected status code" in str(e).lower()
                or "empty" in str(e).lower()
                else ""
            )
            if bot_msg:
                raise BotDetectedError(
                    f"[{username}] attempt {attempt+1} failed: {e}{bot_msg}"
                )
            print(f"[{username}] attempt {attempt+1} failed: {e}{bot_msg}")
            if attempt == retries - 1:
                raise
            await backoff_sleep(attempt)


async def build_user_list(
    api, beauty_hashtags, users, videos_per_hashtag, exclude_users=None
):
    if exclude_users is None:
        exclude_users = set()

    collected_user_data = []  # Store basic user data from hashtag API

    for hashtag_name in beauty_hashtags:
        try:
            print(f"[Hashtag: #{hashtag_name}] Starting to fetch videos...")
            hashtag = api.hashtag(name=hashtag_name)
            found = 0
            new_found = 0
            video_timeout = 15  # timeout per video fetch

            async for video in hashtag.videos(count=videos_per_hashtag):
                try:
                    # Wrap each video access in timeout to detect hangs
                    if video.author and video.author.username:
                        username = video.author.username
                        if username not in exclude_users and username not in users:
                            users.add(username)
                            # Try to collect basic info available from hashtag API
                            user_basic = {
                                "hashtag": hashtag_name,
                                "username": username,
                                "nickname": getattr(video.author, "nickname", "")
                                or getattr(video.author, "display_name", ""),
                            }
                            collected_user_data.append(user_basic)
                            new_found += 1
                            if new_found % 50 == 0:
                                print(
                                    f"[Hashtag: #{hashtag_name}] Found {new_found} new users so far..."
                                )
                        found += 1
                    # slow crawl with time
                    await asyncio.sleep(random.uniform(1.5, 4.0))
                    if found >= videos_per_hashtag:
                        break
                except asyncio.TimeoutError:
                    print(
                        f"  ⚠ Video processing timeout after {video_timeout}s. Skipping this video..."
                    )
                    continue
                except Exception as e:
                    print(f"  ⚠ Error processing video: {e}. Continuing...")
                    continue

            print(
                f"✓ Found {new_found} new users from #{hashtag_name} (processed {found} videos)"
            )

        except asyncio.TimeoutError:
            print(
                f"✗ Hashtag #{hashtag_name} timed out after 60s. Likely blocked by TikTok. Skipping..."
            )
            print(
                f"   Tip: ms_token might be expired. Try refreshing from TikTok cookies."
            )
            await asyncio.sleep(10)
        except Exception as e:
            print(f"✗ Error with hashtag {hashtag_name}: {e}")
            print(
                f"   This could mean: (1) ms_token expired, (2) TikTok blocked the session, (3) network issue"
            )
            await asyncio.sleep(10)

    return users, collected_user_data


def save_progress(kol_data, failed_users):
    df = pd.DataFrame(kol_data)
    # remove duplicate usernames in case there are repeats from multiple runs
    if "username" in df.columns:
        df = df.drop_duplicates(subset=["username"], keep="first")

    if "hashtag" in df.columns:
        ordered_columns = ["hashtag", "username"] + [
            column for column in df.columns if column not in {"hashtag", "username"}
        ]
        df = df[ordered_columns]

    df.to_excel(OUTPUT_FILE, index=False)

    save_json(FAILED_FILE, failed_users)


def save_user_list(user_rows):
    new_df = pd.DataFrame(user_rows)
    if new_df.empty:
        new_df = pd.DataFrame(columns=["hashtag", "username", "nickname"])

    if "username" in new_df.columns:
        new_df.drop_duplicates(subset=["username"], keep="first", inplace=True)

    if OUTPUT_FILE.is_file():
        try:
            existing_df = pd.read_excel(OUTPUT_FILE)
        except Exception as e:
            print(
                f"⚠ Warning: could not read existing {OUTPUT_FILE}: {e}. Rewriting from scratch."
            )
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()

    if "username" in existing_df.columns and not existing_df.empty:
        merged = pd.concat([existing_df, new_df], ignore_index=True)
        merged.drop_duplicates(subset=["username"], keep="first", inplace=True)
    else:
        merged = new_df

    if "hashtag" in merged.columns:
        ordered_columns = ["hashtag", "username"] + [
            column for column in merged.columns if column not in {"hashtag", "username"}
        ]
        merged = merged[ordered_columns]

    merged.to_excel(OUTPUT_FILE, index=False)


async def enrich_users_from_excel(api, existing_df):
    kol_data = []
    failed_users = set()

    usernames = [str(u) for u in existing_df.get("username", []) if pd.notna(u)]
    print(f"Enriching {len(usernames)} users from {OUTPUT_FILE}")

    for i, username in enumerate(usernames, start=1):
        try:
            user_info = await safe_user_info(api, username)
            data, user_id = extract_user_data(user_info)
            kol_data.append(data)
            print(f"Enriched {i}/{len(usernames)}: {username}")
            save_progress(kol_data, list(failed_users))
            await asyncio.sleep(random.uniform(1.0, 3.0))
        except BotDetectedError as e:
            print(f"Bot detected while enriching {username}: {e}. Stopping.")
            raise SystemExit("Bot detection triggered during enrich. Exiting.")
        except Exception as e:
            print(f"Failed to enrich {username}: {e}")
            failed_users.add(username)
            save_progress(kol_data, list(failed_users))
            await asyncio.sleep(1)

    return kol_data, failed_users


async def crawl_beauty_kols():
    all_hashtags = [
        # "beautyvietnam",
        # "beautyvn",
        # "skincarevn",
        # "kemtrangdiem",
        # "lamdep",
        # "HợptáccùngUnilever",
        # "Hợptáccùng3ce"
        # "sieudep",
        "skincarevietnam",
        # "makeupvietnam",
        # "cosmeticsvietnam",
        # "lamdepvietnam",
        # "sieudepvietnam",
        # "makeupvn",
        # "cosmeticsvn",
        # "vnbeauty",
        # "beauty",
        # "makeup",
        # "skincare",
        # "cosmetics",
        # "beautytips",
        # "hợptáccùngLorealParis",
        # "ObagimedicalVietnam",
    ]
    # Combine global beauty focus + Vietnam tags
    # all_hashtags = beauty_hashtags + vietnam_hashtags
    videos_per_hashtag = 20

    # vietnam_only = os.environ.get("VIETNAM_ONLY", "1") in ["1", "true", "True"]

    checkpoint = {
        "processed_users": [],
        "pending_users": [],
        "failed_users": [],
        "completed_hashtags": [],
    }

    if CHECKPOINT_FILE.is_file():
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

    kol_data = []
    processed_users = set(checkpoint.get("processed_users", []))
    processed_user_ids = set(checkpoint.get("processed_user_ids", []))
    # pending = set(checkpoint.get("pending_users", []))
    failed_users = set(checkpoint.get("failed_users", []))
    completed_hashtags = set(checkpoint.get("completed_hashtags", []))
    # collected_users = set()

    if OUTPUT_FILE.is_file():
        old_df = pd.read_excel(OUTPUT_FILE)
        if "username" in old_df.columns:
            for u in old_df["username"]:
                if pd.notna(u):
                    processed_users.add(str(u))
        if "user_id" in old_df.columns:
            for uid in old_df["user_id"]:
                if pd.notna(uid):
                    processed_user_ids.add(str(uid))
        kol_data = old_df.to_dict("records")

    async with TikTokApi() as api:
        print("Starting TikTokApi session initialization...")
        heartbeat_task = asyncio.create_task(heartbeat(interval=30))

        try:
            # browser_context_args = {}
            # if PROXY_URL:
            #     print(f"Using proxy: {PROXY_URL}")
            #     browser_context_args["proxy"] = {"server": PROXY_URL}
            #     print(
            #         "Note: Proxy support may require manual configuration in TikTokApi or system-level proxy."
            #     )

            create_sessions_kwargs = {
                "ms_tokens": [ms_token],
                "num_sessions": 1,
                "sleep_after": 5,
                "browser": os.getenv("TIKTOK_BROWSER", "chromium"),
                "headless": HEADLESS,
            }

            await api.create_sessions(**create_sessions_kwargs)
            # Note: locale and timezone help TikTok geolocate requests to Vietnam
            # Some versions of TikTokApi may accept these in create_sessions() or browser_context_args
            print(
                f"Session configured with locale={BROWSER_LOCALE}, timezone={BROWSER_TIMEZONE}"
            )

            print("Waiting 8 seconds for browser to fully load...")
            await asyncio.sleep(8)

            print("Testing session connectivity...")
            await test_session(api)
            print("✓ Session is active and responding.")

            if RUN_MODE == "collect":
                all_users = set(processed_users)
                collected_rows = []

                if OUTPUT_FILE.is_file():
                    existing_collect_df = pd.read_excel(OUTPUT_FILE)
                    if not existing_collect_df.empty:
                        collected_rows = existing_collect_df.to_dict("records")

                for hashtag in all_hashtags:
                    if hashtag in completed_hashtags:
                        print(f"Skipping already completed hashtag: #{hashtag}")
                        continue

                    print(f"Collecting users from hashtag: #{hashtag}")
                    users = set()
                    users, user_data = await build_user_list(
                        api,
                        [hashtag],
                        users,
                        videos_per_hashtag,
                        exclude_users=all_users,
                    )
                    new_users = users - all_users
                    collected_rows.extend(user_data)
                    print(f"Found {len(new_users)} new users for #{hashtag}")
                    all_users.update(new_users)
                    completed_hashtags.add(hashtag)

                    if len(all_users) >= MAX_USERS_PER_RUN:
                        print(
                            f"Reached MAX_USERS_PER_RUN={MAX_USERS_PER_RUN} while collecting. Stopping."
                        )
                        break

                save_user_list(collected_rows)
                print(f"Saved {len(all_users)} users to {OUTPUT_FILE} (collect mode)")
                return

            if RUN_MODE == "enrich":
                if not OUTPUT_FILE.is_file():
                    raise FileNotFoundError(f"{OUTPUT_FILE} not found for enrich mode")

                existing_df = pd.read_excel(OUTPUT_FILE)
                if existing_df.empty:
                    raise ValueError(f"{OUTPUT_FILE} is empty, cannot enrich")

                kol_data, failed_users = await enrich_users_from_excel(api, existing_df)
                print(
                    f"Enrich complete. Total profiles: {len(kol_data)} Failed: {len(failed_users)}"
                )
                return

        finally:
            # Ensure heartbeat is always cancelled to prevent resource leaks
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(crawl_beauty_kols())
