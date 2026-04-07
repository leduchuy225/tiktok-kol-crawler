import asyncio
import argparse
import json
import os
import random
import re
from pathlib import Path

import httpx
from dotenv import load_dotenv
from bs4 import BeautifulSoup
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

MAX_USERS_PER_RUN = int(os.environ.get("MAX_USERS_PER_RUN", "3"))
HEADLESS = os.environ.get("HEADLESS", "1") in ["1", "true", "True"]
BROWSER_LOCALE = os.environ.get("BROWSER_LOCALE", "vi-VN")
BROWSER_TIMEZONE = os.environ.get("BROWSER_TIMEZONE", "Asia/Ho_Chi_Minh")
# PROXY_URL = os.environ.get("PROXY_URL", "").strip()

all_hashtags = [
    # "beautyvietnam",
    # "beautyvn",
    # "skincarevn",
    # "kemtrangdiem",
    # "lamdep",
    # "Hợptáccùng3ce"
    # "sieudep",
    # "tiplamdep",
    # "carslan",
    # "innisfreevietnam",
    # "KemChốngNắng",
    # "ChămSócDa",
    # "makeupvietnam",
    # "reviewlamdep",
    # "skincarevietnam",
    # "cosmeticsvietnam",
    # "lamdepvietnam",
    # "HợptáccùngUnilever",
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
    # "EsteeLauderVN",
    # "LaneigeVN",
    # "LaRochePosayVN",
    # "Skin1004Vietnam",
    # "DysonBeautyVN",
    # "HợpTácCùngLemonade",
    # "GocLamDep",
    # "SkinCareRoutineVN",
    # "CeraVeVN",
    # "VichyVietnam",
    # "EucerinVN",
    # "BiodermaVietnam",
    # "PaulaChoiceVN",
    # "TheOrdinaryVietnam",
    # "CocoonVietnam",
    # "skincarekhoahoc",
    # "hoatchatduongda",
    # "reviewkemchongnang",
    # "phuchoida",
    # "nghienskincare",
    # "dưỡngda",
    # "skincaretipsvietnam",
    # "duongtrangda",
    # "chiasekinhnghiemlamdep",
    "chonglaohoa",
    "trimunvietnam",
    "danhaycam",
    "nghiendapmatna",
    "tipsduongda",
    "skincaremoingay",
    "routinevn",
    "duongtrangantoan",
    "trimunthammong",
]

videos_per_hashtag = 200


def get_cli_options():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--mode", "-mode", "-m", dest="mode", type=str)
    parser.add_argument(
        "--retry-failed",
        dest="retry_failed",
        action="store_true",
        help="In enrich mode, set update=1 for usernames in failed_users.json before enriching",
    )
    args, _ = parser.parse_known_args()
    return args


cli_options = get_cli_options()
RUN_MODE = (
    cli_options.mode or os.environ.get("RUN_MODE", "collect")
).lower()  # collect | enrich
RETRY_FAILED = cli_options.retry_failed

if RUN_MODE not in {"collect", "enrich"}:
    raise ValueError("RUN_MODE must be 'collect' or 'enrich'.")

if RUN_MODE == "collect" and not ms_token:
    raise ValueError(
        "Please set the ms_token environment variable. Get it from your TikTok cookies."
    )


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


def extract_gmail(text):
    if text is None:
        return ""
    match = re.search(
        r"([a-zA-Z0-9._%+\-]+@gmail\.com)", str(text), flags=re.IGNORECASE
    )
    return match.group(1).lower() if match else ""


def extract_phone(text):
    if text is None:
        return ""

    value = str(text).replace("\n", " ")
    # Match Vietnamese and international formats:
    # +84xxxxxxxxx, 0xxxxxxxxx, +1-xxx-xxx-xxxx, separated by spaces/dashes/dots
    # Must be 7–15 digits total; avoid matching pure years or IDs
    match = re.search(
        r"(?<![\d])(?:\+?\d{1,3}[\s.\-]?)?(?:\(?\d{2,4}\)?[\s.\-]?){2,4}\d{3,4}(?![\d])",
        value,
    )
    if match:
        raw = match.group(0).strip()
        digits = re.sub(r"[^\d+]", "", raw)  # keep only digits and leading +
        if 7 <= len(digits.lstrip("+")) <= 15:
            return raw.strip()
    return ""


def extract_ig_handle(text):
    if text is None:
        return ""

    value = str(text).replace("\n", " ")
    patterns = [
        # Handles: ig: name, ig for work: name, ig for collab: name, etc.
        r"\b(?:ig|insta|instagram)\b(?:\s+for\s+[a-zA-Z0-9_&/+\-. ]{1,40})?\s*[:\-|]\s*@?([a-zA-Z0-9._]{1,30})\b",
        # Handles: ig @name, instagram @name, insta for booking @name
        r"\b(?:ig|insta|instagram)\b(?:\s+for\s+[a-zA-Z0-9_&/+\-. ]{1,40})?\s+@([a-zA-Z0-9._]{1,30})\b",
        r"(?:instagram\.com/|instagr\.am/)@?([a-zA-Z0-9._]{1,30})\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, value, flags=re.IGNORECASE)
        if match:
            handle = match.group(1).strip("._").lower()
            if handle and not handle.endswith("gmail"):
                return handle

    return ""


def mark_failed_users_for_retry(existing_df):
    if existing_df.empty or "username" not in existing_df.columns:
        return existing_df, 0, set()

    failed_list = load_json(FAILED_FILE)
    failed_users = {
        str(item).strip().lstrip("@") for item in failed_list if str(item).strip()
    }
    if not failed_users:
        return existing_df, 0, set()

    df = existing_df.copy()
    if "update" not in df.columns:
        df["update"] = 1

    username_norm = df["username"].astype(str).str.strip().str.lstrip("@")
    retry_mask = username_norm.isin(failed_users)
    retry_count = int(retry_mask.sum())
    retry_usernames = set(username_norm[retry_mask].tolist())
    if retry_count > 0:
        df.loc[retry_mask, "update"] = 1

    return df, retry_count, retry_usernames


def remove_users_from_output(usernames):
    if not usernames or not OUTPUT_FILE.is_file():
        return 0

    df = pd.read_excel(OUTPUT_FILE)
    if df.empty or "username" not in df.columns:
        return 0

    normalized_usernames = {
        str(username).strip().lstrip("@")
        for username in usernames
        if str(username).strip()
    }
    username_norm = df["username"].astype(str).str.strip().str.lstrip("@")
    keep_mask = ~username_norm.isin(normalized_usernames)
    removed_count = int((~keep_mask).sum())

    if removed_count > 0:
        filtered = df[keep_mask].copy()
        filtered.to_excel(OUTPUT_FILE, index=False)

    return removed_count


def is_update_enabled(raw_value):
    """Normalize Excel/text flags for update column."""
    if pd.isna(raw_value):
        return True

    if isinstance(raw_value, bool):
        return raw_value

    if isinstance(raw_value, (int, float)):
        return int(raw_value) == 1

    value = str(raw_value).strip().lower()
    if value in {"1", "1.0", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "0.0", "false", "no", "n", "off"}:
        return False

    # Fallback: try numeric conversion for strings like "1.00"
    try:
        return int(float(value)) == 1
    except (TypeError, ValueError):
        return False


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


async def fetch_user_from_web(client, username, hashtag="", retries=3):
    url = f"https://www.tiktok.com/@{username}"
    for attempt in range(retries):
        try:
            await asyncio.sleep(random.uniform(1.2, 2.8))
            response = await client.get(url, timeout=20.0)

            if response.status_code == 429:
                print(f"[{username}] got 429 rate limit, retrying...")
                await asyncio.sleep(5 + attempt * 2)
                continue

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")

            soup = BeautifulSoup(response.text, "html.parser")
            script = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
            if not script or not script.string:
                raise Exception("missing rehydration data")

            data = json.loads(script.string)
            user_info = (
                data.get("__DEFAULT_SCOPE__", {})
                .get("webapp.user-detail", {})
                .get("userInfo", {})
            )
            user = user_info.get("user", {})
            stats = user_info.get("stats", {})
            if not user:
                raise Exception("user payload missing")

            bio = user.get("signature", "")
            gmail = extract_gmail(bio)
            ig = extract_ig_handle(bio)
            phone = extract_phone(bio)

            return {
                "hashtag": hashtag,
                "username": username,
                "display_name": user.get("nickname", "")
                or user.get("uniqueId", username),
                "nickname": user.get("nickname", ""),
                "followers": stats.get("followerCount", 0),
                "following": stats.get("followingCount", 0),
                "likes": stats.get("heartCount", 0),
                "videos": stats.get("videoCount", 0),
                "friends": stats.get("friendCount", 0),
                "bio": bio,
                "gmail": gmail,
                "ig": ig,
                "phone": phone,
                "verified": user.get("verified", False),
                "private_account": user.get("privateAccount", False),
                "sec_uid": user.get("secUid", ""),
                "user_id": str(user.get("id", "")),
                "avatar_url": user.get("avatarLarger", "")
                or user.get("avatarMedium", "")
                or user.get("avatarThumb", ""),
                "region": user.get("region", "") or user.get("country", ""),
                "_ok": True,
            }
        except Exception as e:
            if attempt == retries - 1:
                print(f"[{username}] web enrich failed: {e}")
                return {
                    "hashtag": hashtag,
                    "username": username,
                    "display_name": username,
                    "nickname": "",
                    "followers": 0,
                    "following": 0,
                    "likes": 0,
                    "videos": 0,
                    "friends": 0,
                    "bio": "",
                    "gmail": "",
                    "ig": "",
                    "phone": "",
                    "verified": False,
                    "private_account": False,
                    "sec_uid": "",
                    "user_id": "",
                    "avatar_url": "",
                    "region": "",
                    "_ok": False,
                }
            await asyncio.sleep(2 + attempt)


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
                                "update": 1,
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
    new_df = pd.DataFrame(kol_data)
    if "username" in new_df.columns:
        new_df = new_df.drop_duplicates(subset=["username"], keep="first")

    # Merge with whatever is currently on disk so rows not yet in kol_data are never lost
    if OUTPUT_FILE.is_file():
        try:
            existing_df = pd.read_excel(OUTPUT_FILE)
            if (
                not existing_df.empty
                and "username" in existing_df.columns
                and "username" in new_df.columns
            ):
                known = set(new_df["username"].dropna().astype(str).str.lstrip("@"))
                extra = existing_df[
                    ~existing_df["username"].astype(str).str.lstrip("@").isin(known)
                ]
                if not extra.empty:
                    new_df = pd.concat([new_df, extra], ignore_index=True)
        except Exception as e:
            print(f"⚠ Warning: could not read existing {OUTPUT_FILE} for merge: {e}")

    if "update" not in new_df.columns:
        new_df["update"] = 1

    if "hashtag" in new_df.columns:
        ordered_columns = ["hashtag", "username", "update"] + [
            column
            for column in new_df.columns
            if column not in {"hashtag", "username", "update"}
        ]
        new_df = new_df[ordered_columns]
    elif "username" in new_df.columns:
        ordered_columns = ["username", "update"] + [
            column for column in new_df.columns if column not in {"username", "update"}
        ]
        new_df = new_df[ordered_columns]

    # Atomic write: write to a temp file first, then replace the real file.
    # This ensures the original is never left in a half-written/corrupt state.
    tmp_file = OUTPUT_FILE.with_suffix(".tmp.xlsx")
    try:
        new_df.to_excel(tmp_file, index=False)
        tmp_file.replace(OUTPUT_FILE)
    except Exception as e:
        print(f"⚠ Warning: could not save progress to {OUTPUT_FILE}: {e}")
        if tmp_file.is_file():
            tmp_file.unlink(missing_ok=True)

    save_json(FAILED_FILE, failed_users)


def save_user_list(user_rows):
    new_df = pd.DataFrame(user_rows)
    if new_df.empty:
        new_df = pd.DataFrame(columns=["hashtag", "username", "update", "nickname"])

    if "update" not in new_df.columns:
        new_df["update"] = 1

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

    if "update" not in merged.columns:
        merged["update"] = 1
    merged["update"] = merged["update"].fillna(1)

    if "hashtag" in merged.columns:
        ordered_columns = ["hashtag", "username", "update"] + [
            column
            for column in merged.columns
            if column not in {"hashtag", "username", "update"}
        ]
        merged = merged[ordered_columns]
    elif "username" in merged.columns:
        ordered_columns = ["username", "update"] + [
            column for column in merged.columns if column not in {"username", "update"}
        ]
        merged = merged[ordered_columns]

    merged.to_excel(OUTPUT_FILE, index=False)


async def enrich_users_from_excel(existing_df):
    # Pre-populate with ALL rows so an interrupt never loses unprocessed rows
    kol_data = [dict(row) for row in existing_df.to_dict("records")]
    failed_users = set()

    source_rows = kol_data  # same list, updates are in-place by index
    skip_save_interval = 100
    skipped_since_last_save = 0
    has_unsaved_changes = False
    print(f"Enriching {len(source_rows)} users from {OUTPUT_FILE} using web scraping")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        for i, row in enumerate(source_rows, start=1):
            username = str(row.get("username", "")).strip().lstrip("@")
            hashtag = str(row.get("hashtag", "")).strip()
            if not username:
                continue

            raw_update = row.get("update", 1)
            should_update = is_update_enabled(raw_update)

            if not should_update:
                row["username"] = username
                print(f"Skipped {i}/{len(source_rows)}: {username} (update=0)")
                skipped_since_last_save += 1
                has_unsaved_changes = True
                if skipped_since_last_save >= skip_save_interval:
                    save_progress(kol_data, list(failed_users))
                    skipped_since_last_save = 0
                    has_unsaved_changes = False
                continue

            result = await fetch_user_from_web(client, username, hashtag=hashtag)

            row["username"] = username
            row["display_name"] = result.get("display_name") or row.get(
                "display_name", username
            )
            row["nickname"] = result.get("nickname") or row.get("nickname", "")
            row["followers"] = result.get("followers", row.get("followers", 0))
            row["following"] = result.get("following", row.get("following", 0))
            row["likes"] = result.get("likes", row.get("likes", 0))
            row["videos"] = result.get("videos", row.get("videos", 0))
            row["friends"] = result.get("friends", row.get("friends", 0))
            row["bio"] = result.get("bio") or row.get("bio", "")
            row["gmail"] = result.get("gmail") or row.get("gmail", "")
            row["ig"] = result.get("ig") or row.get("ig", "")
            row["phone"] = result.get("phone") or row.get("phone", "")
            row["verified"] = result.get("verified", row.get("verified", False))
            row["private_account"] = result.get(
                "private_account", row.get("private_account", False)
            )
            row["sec_uid"] = result.get("sec_uid") or row.get("sec_uid", "")
            row["user_id"] = result.get("user_id") or row.get("user_id", "")
            row["avatar_url"] = result.get("avatar_url") or row.get("avatar_url", "")
            row["region"] = result.get("region") or row.get("region", "")
            # Mark as processed so it will be skipped in the next enrich run
            row["update"] = 0

            if not result.get("_ok", False):
                failed_users.add(username)

            print(f"Enriched {i}/{len(source_rows)}: {username}")
            save_progress(kol_data, list(failed_users))
            skipped_since_last_save = 0
            has_unsaved_changes = False

    if has_unsaved_changes:
        save_progress(kol_data, list(failed_users))

    return kol_data, failed_users


async def crawl_beauty_kols():
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

    if RUN_MODE == "enrich":
        if not OUTPUT_FILE.is_file():
            raise FileNotFoundError(f"{OUTPUT_FILE} not found for enrich mode")

        existing_df = pd.read_excel(OUTPUT_FILE)
        if existing_df.empty:
            raise ValueError(f"{OUTPUT_FILE} is empty, cannot enrich")

        retry_usernames = set()
        if RETRY_FAILED:
            existing_df, retry_count, retry_usernames = mark_failed_users_for_retry(
                existing_df
            )
            if retry_count > 0:
                save_user_list(existing_df.to_dict("records"))
                print(
                    f"Marked {retry_count} users from {FAILED_FILE} with update=1 for retry"
                )
            else:
                print(f"No users found in {FAILED_FILE} to retry")

        try:
            kol_data, failed_users = await enrich_users_from_excel(existing_df)

            if RETRY_FAILED and retry_usernames:
                failed_retry_users = {
                    str(username).strip().lstrip("@")
                    for username in failed_users
                    if str(username).strip().lstrip("@") in retry_usernames
                }
                removed_count = remove_users_from_output(failed_retry_users)
                if removed_count > 0:
                    print(
                        f"Removed {removed_count} retried users that still failed from {OUTPUT_FILE}"
                    )

                # Clear failed_users.json — retried users that keep failing were removed above;
                # any remaining failures will be written fresh by save_progress.
                save_json(FAILED_FILE, [])
                print(f"Cleared {FAILED_FILE}")

            print(
                f"Enrich complete. Total profiles: {len(kol_data)} Failed: {len(failed_users)}"
            )
        except KeyboardInterrupt:
            print("\nEnrich interrupted — progress already saved by last checkpoint.")
        return

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
                existing_hashtags = set()

                if OUTPUT_FILE.is_file():
                    existing_collect_df = pd.read_excel(OUTPUT_FILE)
                    if not existing_collect_df.empty:
                        collected_rows = existing_collect_df.to_dict("records")
                        if "hashtag" in existing_collect_df.columns:
                            existing_hashtags = {
                                str(value).strip()
                                for value in existing_collect_df["hashtag"]
                                if pd.notna(value) and str(value).strip()
                            }

                for hashtag in all_hashtags:
                    if hashtag in completed_hashtags:
                        print(f"Skipping already completed hashtag: #{hashtag}")
                        continue

                    if hashtag in existing_hashtags:
                        print(f"Skipping hashtag already in Excel: #{hashtag}")
                        completed_hashtags.add(hashtag)
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
                    existing_hashtags.add(hashtag)

                    # Persist after each hashtag so Ctrl+C does not lose collected rows
                    save_user_list(collected_rows)
                    print(
                        f"Saved progress after #{hashtag}: {len(all_users)} total users"
                    )

                    if len(all_users) >= MAX_USERS_PER_RUN:
                        print(
                            f"Reached MAX_USERS_PER_RUN={MAX_USERS_PER_RUN} while collecting. Stopping."
                        )
                        break

                save_user_list(collected_rows)
                print(f"Saved {len(all_users)} users to {OUTPUT_FILE} (collect mode)")
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
