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

CHECKPOINT_FILE = Path("checkpoint.json")
OUTPUT_FILE = Path("beauty_kols_data.xlsx")
FAILED_FILE = Path("failed_users.json")
COLLECTED_USERS_FILE = Path("collected_users.json")

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
PROCESS_SAVED_USERS = os.environ.get("PROCESS_SAVED_USERS", "0") in [
    "1",
    "true",
    "True",
]


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
            if not user_info or not user_info.get("uniqueId"):
                raise Exception(
                    f"User {username} not found or empty response from TikTok"
                )
            return user_info
        except asyncio.TimeoutError:
            print(f"[{username}] attempt {attempt+1} timed out after {timeout}s")
            if attempt == retries - 1:
                raise
            await backoff_sleep(attempt)
        except Exception as e:
            print(f"[{username}] attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise
            await backoff_sleep(attempt)


async def build_user_list(api, beauty_hashtags, users, videos_per_hashtag):
    for hashtag_name in beauty_hashtags:
        try:
            print(f"[Hashtag: #{hashtag_name}] Starting to fetch videos...")
            hashtag = api.hashtag(name=hashtag_name)
            found = 0
            video_timeout = 15  # timeout per video fetch

            async for video in hashtag.videos(count=videos_per_hashtag):
                try:
                    # Wrap each video access in timeout to detect hangs
                    if video.author and video.author.username:
                        users.add(video.author.username)
                        found += 1
                        if found % 50 == 0:
                            print(
                                f"[Hashtag: #{hashtag_name}] Collected {found} users so far..."
                            )
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

            print(f"✓ Collected {found} users from #{hashtag_name}")

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

    return users


def save_progress(kol_data, failed_users):
    df = pd.DataFrame(kol_data)
    # remove duplicate usernames in case there are repeats from multiple runs
    if "username" in df.columns:
        df = df.drop_duplicates(subset=["username"], keep="first")
    df.to_excel(OUTPUT_FILE, index=False)
    save_json(FAILED_FILE, failed_users)


async def crawl_beauty_kols():
    beauty_hashtags = ["beauty", "makeup", "skincare", "cosmetics", "beautytips"]
    vietnam_hashtags = [
        "beautyvietnam",
        "beautyvn",
        "vnbeauty",
        "kemtrangdiem",
        "lamdep",
        "sieudep",
        "skincarevietnam",
        "makeupvietnam",
        "cosmeticsvietnam",
        "lamdepvietnam",
        "sieudepvietnam",
        "skincarevn",
        "makeupvn",
        "cosmeticsvn",
    ]
    # Combine global beauty focus + Vietnam tags
    all_hashtags = beauty_hashtags + vietnam_hashtags
    videos_per_hashtag = 100

    vietnam_only = os.environ.get("VIETNAM_ONLY", "1") in ["1", "true", "True"]

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
    pending = set(checkpoint.get("pending_users", []))
    failed_users = set(checkpoint.get("failed_users", []))
    completed_hashtags = set(checkpoint.get("completed_hashtags", []))
    collected_users = set()

    if OUTPUT_FILE.is_file():
        old_df = pd.read_excel(OUTPUT_FILE)
        for u in old_df.get("username", []):
            processed_users.add(str(u))
            if u in failed_users:
                failed_users.remove(u)
        for uid in old_df.get("user_id", []):
            if pd.notna(uid):
                processed_user_ids.add(str(uid))
        kol_data = old_df.to_dict("records")

    async with TikTokApi() as api:
        print("Starting TikTokApi session initialization...")
        heartbeat_task = asyncio.create_task(heartbeat(interval=30))

        try:
            await api.create_sessions(
                ms_tokens=[ms_token],
                num_sessions=1,
                sleep_after=5,
                browser=os.getenv("TIKTOK_BROWSER", "chromium"),
                headless=HEADLESS,
            )
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

            if PROCESS_SAVED_USERS:
                print("Processing saved users mode enabled.")
                if COLLECTED_USERS_FILE.is_file():
                    with open(COLLECTED_USERS_FILE, "r", encoding="utf-8") as f:
                        collected_users = set(json.load(f))
                    print(f"Loaded {len(collected_users)} collected users from file.")
                else:
                    print("No collected_users.json found. Nothing to process.")
                    return

                pending = collected_users - processed_users
                pending = list(pending)
                total = len(pending)
                print(f"Processing {total} saved users. Pending users: {pending}")

                for i, username in enumerate(pending, start=1):
                    if username in processed_users:
                        continue

                    try:
                        user_info = await safe_user_info(api, username)

                        user_id = str(user_info.get("id", ""))
                        if user_id and user_id in processed_user_ids:
                            print(
                                f"Skipped {username} ({user_id}): user_id already processed"
                            )
                            processed_users.add(username)
                            continue

                        data = {
                            "username": user_info.get("uniqueId", ""),
                            "user_id": user_id,
                            "sec_uid": user_info.get("secUid", ""),
                            "nickname": user_info.get("nickname", ""),
                            "bio": user_info.get("signature", ""),
                            "verified": user_info.get("verified", False),
                            "private_account": user_info.get("privateAccount", False),
                            "followers": user_info.get("stats", {}).get(
                                "followerCount", 0
                            ),
                            "following": user_info.get("stats", {}).get(
                                "followingCount", 0
                            ),
                            "likes": user_info.get("stats", {}).get("heartCount", 0),
                            "videos": user_info.get("stats", {}).get("videoCount", 0),
                            "friends": user_info.get("stats", {}).get("friendCount", 0),
                            "country": user_info.get("country", ""),
                            "city": user_info.get("city", ""),
                        }

                        is_vietnam = False
                        locale_text = " ".join(
                            [
                                (user_info.get("country", "") or ""),
                                (user_info.get("city", "") or ""),
                                (user_info.get("signature", "") or ""),
                                (user_info.get("uniqueId", "") or ""),
                                (user_info.get("nickname", "") or ""),
                            ]
                        ).lower()
                        vietnam_tokens = [
                            "vietnam",
                            "vn",
                            "việt",
                            "hanoi",
                            "hochiminh",
                            "saigon",
                            "hcm",
                        ]
                        if any(tok in locale_text for tok in vietnam_tokens):
                            is_vietnam = True
                        data["is_vietnam"] = is_vietnam

                        if vietnam_only and not is_vietnam:
                            print(f"Skipped {username}: not identified as Vietnam KOL")
                            processed_users.add(username)
                            continue

                        kol_data.append(data)
                        processed_users.add(username)
                        if user_id:
                            processed_user_ids.add(user_id)

                        print(
                            f"Processed {i}/{total} ({username}) from saved users. Processed total: {len(processed_users)}"
                        )

                        # Save progress immediately after each successful KOL
                        save_progress(kol_data, list(failed_users))

                        await asyncio.sleep(random.uniform(3.0, 7.0))

                    except Exception as e:
                        print(f"Failed {username} after retries: {e}")
                        failed_users.add(username)

                # Save final checkpoint
                new_checkpoint = {
                    "processed_users": list(processed_users),
                    "processed_user_ids": list(processed_user_ids),
                    "pending_users": [],
                    "failed_users": list(failed_users),
                    "completed_hashtags": list(completed_hashtags),
                }
                with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                    json.dump(new_checkpoint, f, ensure_ascii=False, indent=2)

            else:
                for hashtag in all_hashtags:
                    if hashtag in completed_hashtags:
                        print(f"Skipping already completed hashtag: #{hashtag}")
                        continue

                    print(f"Processing hashtag: #{hashtag}")
                    users = set()
                    await build_user_list(api, [hashtag], users, videos_per_hashtag)
                    collected_users.update(users)
                    pending = users - processed_users

                    if not pending:
                        print(f"No new users from #{hashtag}. Marking as completed.")
                        completed_hashtags.add(hashtag)
                        continue

                    pending = list(pending)
                    # If more users found than MAX_USERS_PER_RUN, process all; else process all (no limit)
                    total = len(pending)
                    print(
                        f"Starting processing {total} users from #{hashtag} (found {len(pending)} users). Pending users: {pending}"
                    )

                    for i, username in enumerate(pending, start=1):
                        if username in processed_users:
                            continue

                        try:
                            user_info = await safe_user_info(api, username)

                            user_id = str(user_info.get("id", ""))
                            if user_id and user_id in processed_user_ids:
                                print(
                                    f"Skipped {username} ({user_id}): user_id already processed"
                                )
                                processed_users.add(username)
                                continue

                            data = {
                                "username": user_info.get("uniqueId", ""),
                                "user_id": user_id,
                                "sec_uid": user_info.get("secUid", ""),
                                "nickname": user_info.get("nickname", ""),
                                "bio": user_info.get("signature", ""),
                                "verified": user_info.get("verified", False),
                                "private_account": user_info.get(
                                    "privateAccount", False
                                ),
                                "followers": user_info.get("stats", {}).get(
                                    "followerCount", 0
                                ),
                                "following": user_info.get("stats", {}).get(
                                    "followingCount", 0
                                ),
                                "likes": user_info.get("stats", {}).get(
                                    "heartCount", 0
                                ),
                                "videos": user_info.get("stats", {}).get(
                                    "videoCount", 0
                                ),
                                "friends": user_info.get("stats", {}).get(
                                    "friendCount", 0
                                ),
                                "country": user_info.get("country", ""),
                                "city": user_info.get("city", ""),
                            }

                            is_vietnam = False
                            locale_text = " ".join(
                                [
                                    (user_info.get("country", "") or ""),
                                    (user_info.get("city", "") or ""),
                                    (user_info.get("signature", "") or ""),
                                    (user_info.get("uniqueId", "") or ""),
                                    (user_info.get("nickname", "") or ""),
                                ]
                            ).lower()
                            vietnam_tokens = [
                                "vietnam",
                                "vn",
                                "việt",
                                "hanoi",
                                "hochiminh",
                                "saigon",
                                "hcm",
                            ]
                            if any(tok in locale_text for tok in vietnam_tokens):
                                is_vietnam = True
                            data["is_vietnam"] = is_vietnam

                            if vietnam_only and not is_vietnam:
                                print(
                                    f"Skipped {username}: not identified as Vietnam KOL"
                                )
                                processed_users.add(username)
                                continue

                            kol_data.append(data)
                            processed_users.add(username)
                            if user_id:
                                processed_user_ids.add(user_id)

                            print(
                                f"Processed {i}/{total} ({username}) from #{hashtag}. Processed total: {len(processed_users)}"
                            )

                            # Save progress immediately after each successful KOL
                            save_progress(kol_data, list(failed_users))

                            await asyncio.sleep(random.uniform(3.0, 7.0))

                        except Exception as e:
                            print(f"Failed {username} after retries: {e}")
                            failed_users.add(username)

                    # Mark hashtag as completed after processing its users
                    completed_hashtags.add(hashtag)
                    print(f"Completed processing hashtag: #{hashtag}")

                    # Save checkpoint after each hashtag
                    new_checkpoint = {
                        "processed_users": list(processed_users),
                        "processed_user_ids": list(processed_user_ids),
                        "pending_users": [],
                        "failed_users": list(failed_users),
                        "completed_hashtags": list(completed_hashtags),
                    }
                    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                        json.dump(new_checkpoint, f, ensure_ascii=False, indent=2)

                    # Check if total processed users exceed MAX_USERS_PER_RUN, stop if so
                    if len(processed_users) >= MAX_USERS_PER_RUN:
                        print(
                            f"Reached max users per run ({MAX_USERS_PER_RUN}). Stopping crawl."
                        )
                        break

                    # Save collected users after all hashtags
                    with open(COLLECTED_USERS_FILE, "w", encoding="utf-8") as f:
                        json.dump(
                            list(collected_users), f, ensure_ascii=False, indent=2
                        )
                    print(
                        f"Saved {len(collected_users)} collected users to {COLLECTED_USERS_FILE}"
                    )

            print(
                f"Crawl complete. Saved {len(kol_data)} KOL profiles. Failed users: {len(failed_users)}"
            )

        finally:
            # Ensure heartbeat is always cancelled to prevent resource leaks
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(crawl_beauty_kols())
