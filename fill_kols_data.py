import asyncio
import httpx
import json
import pandas as pd
from bs4 import BeautifulSoup
import random
import os

# --- CONFIGURATION ---
INPUT_FILE = "beauty_kols_data.xlsx"
OUTPUT_FILE = "beauty_kols_filtered_updated.xlsx"
CHECKPOINT_FILE = "scraping_checkpoint.csv"
USERNAME_COL = "username"
UPDATE_COL = "update"  # The column used for filtering
BATCH_SIZE = 50
CONCURRENT_LIMIT = 2


async def fetch_tiktok_details(client, username, index, total):
    url = f"https://www.tiktok.com/@{username}"
    await asyncio.sleep(random.uniform(2.0, 4.5))

    try:
        response = await client.get(url, timeout=15.0)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            script = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
            if script:
                data = json.loads(script.string)
                user_detail = data["__DEFAULT_SCOPE__"]["webapp.user-detail"]
                user_info = user_detail["userInfo"]

                print(f"[{index}/{total}] ✅ @{username} fetched.")
                return {
                    "username_key": username,
                    "display_name": user_info["user"]["uniqueId"],
                    "nickname": user_info["user"]["nickname"],
                    "followers": user_info["stats"]["followerCount"],
                }
    except Exception as e:
        print(f"[{index}/{total}] 🔥 Error @{username}: {str(e)[:40]}")

    return {
        "username_key": username,
        "display_name": "Error/Missing",
        "nickname": "N/A",
        "followers": 0,
    }


async def main():
    # 1. Load Data
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # --- THE FIX: SANITIZE UPDATE COLUMN ---
    # Convert everything to string, strip whitespace, then check for "1" or "1.0"
    df[UPDATE_COL] = (
        df[UPDATE_COL].astype(str).str.strip().str.replace(".0", "", regex=False)
    )

    # Filter for update == "1"
    to_update_df = df[df[UPDATE_COL] == "1"].copy()

    # Extract usernames and remove '@'
    usernames = (
        to_update_df[USERNAME_COL]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace("@", "")
        .tolist()
    )
    total = len(usernames)

    print(f"Total rows in Excel: {len(df)}")
    print(f"Users found for update (update=1): {total}")

    if total == 0:
        print(
            "❌ Still found 0 users. Check if your column header is exactly 'update' (all lowercase)."
        )
        print(f"Available columns are: {list(df.columns)}")
        return

    # 2. Scraping Logic
    all_results = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36..."
    }

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

        async def limited_task(u, idx):
            async with semaphore:
                return await fetch_tiktok_details(client, u, idx, total)

        for i in range(0, total, BATCH_SIZE):
            batch_usernames = usernames[i : i + BATCH_SIZE]
            print(f"\n--- Processing Batch: {i//BATCH_SIZE + 1} ---")

            tasks = [
                limited_task(u, i + idx + 1) for idx, u in enumerate(batch_usernames)
            ]
            batch_results = await asyncio.gather(*tasks)
            all_results.extend(batch_results)

            # Checkpoint Save
            pd.DataFrame(all_results).to_csv(CHECKPOINT_FILE, index=False)

    # 3. Final Merge
    print("\n--- Finalizing Excel File ---")
    results_df = pd.DataFrame(all_results)

    # Merge using a unique key to prevent row duplication
    final_df = pd.merge(
        df, results_df, left_on=USERNAME_COL, right_on="username_key", how="left"
    )

    if "username_key" in final_df.columns:
        final_df = final_df.drop(columns=["username_key"])

    final_df.to_excel(OUTPUT_FILE, index=False)

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    print(f"✨ DONE! Final file saved as: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
