# TikTok Beauty KOL Crawler

This project crawls TikTok Key Opinion Leaders (KOLs) who create beauty content and stores their profile data in an Excel file.

## Tech Stack

- **TikTokApi**: For accessing TikTok data
- **Playwright**: Used internally by TikTokApi for browser automation
- **Pandas**: For data manipulation and Excel export
- **Python 3.9+**: Required for TikTokApi

## Setup

1. **Clone or download this project**

2. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   python -m playwright install
   ```

3. **Create a `.env` file**:
   - Copy `.env.example` to `.env`
   - Fill in your `ms_token` and other values

   ```bash
   cp .env.example .env
   # edit .env and replace placeholder
   ```

4. **Get ms_token**:
   - Log in to TikTok in your browser
   - Open Developer Tools (F12)
   - Go to Application > Cookies > tiktok.com
   - Find the `msToken` cookie value
   - Set it as an environment variable:
     ```bash
     export ms_token="your_ms_token_here"
     ```

## Usage

Run the crawler:

```bash
python main.py
```

The script will:

1. Search videos from beauty-related hashtags (#beauty, #makeup, #skincare, #cosmetics, #beautytips)
2. Collect unique usernames from video authors
3. Fetch profile information for each user
4. Save the data to `beauty_kols_data.xlsx`

## Configuration

The `.env` file supports the following variables:

- `ms_token`: **Required** — Your TikTok session token (from browser cookies)
- `MAX_USERS_PER_RUN`: Number of users to crawl per run (default: 300, recommended: 3 for testing)
- `VIETNAM_ONLY`: Filter to Vietnam-based creators (1=true, 0=false; default: 1)
- `HEADLESS`: Run browser in headless mode (1=headless/no UI, 0=show browser; default: 1)
- `BROWSER_LOCALE`: Browser locale for Vietnam detection (default: `vi-VN`). Set to `vi` for generic Vietnamese.
- `BROWSER_TIMEZONE`: Browser timezone for Vietnam geo-location (default: `Asia/Ho_Chi_Minh`)
- `TIKTOK_USER_AGENT`: Custom user agent string (optional)
- `PROCESS_SAVED_USERS`: Process previously collected users instead of searching hashtags (1=true, 0=false; default: 0)

### Two-Mode Operation

The crawler has two modes:

1. **Collect Mode** (default, `PROCESS_SAVED_USERS=0`):
   - Searches hashtags for users
   - Saves collected usernames to `collected_users.json`
   - Processes profiles and saves to Excel

2. **Process Mode** (`PROCESS_SAVED_USERS=1`):
   - Loads users from `collected_users.json`
   - Processes their profiles without re-searching hashtags
   - Useful for re-processing saved users or resuming

### Example: Collect users first, then process

```bash
# First run: collect users
PROCESS_SAVED_USERS=0
python main.py

# Second run: process the collected users
PROCESS_SAVED_USERS=1
python main.py
```

```bash
# In .env
MAX_USERS_PER_RUN=3
VIETNAM_ONLY=1
BROWSER_LOCALE=vi-VN
BROWSER_TIMEZONE=Asia/Ho_Chi_Minh
HEADLESS=0  # See browser progress
```

Then run:

```bash
python main.py
```

This will:

- Configure browser to appear as if from Vietnam (locale + timezone)
- Fetch only Vietnamese beauty content hashtags
- Limit crawl to 3 KOL profiles for quick testing
- Display browser UI so you can monitor crawling progress

## Output Data

The Excel file contains the following columns:

- username: TikTok username
- user_id: Internal user ID
- sec_uid: Secondary user ID
- nickname: Display name
- bio: User bio/description
- verified: Whether account is verified
- private_account: Whether account is private
- followers: Number of followers
- following: Number of accounts followed
- likes: Total likes received
- videos: Number of videos posted
- friends: Number of friends
- country: User country (if available)
- city: User city (if available)
- is_vietnam: Whether flagged as Vietnam-based KOL

## Notes

- **Rate Limiting**: TikTok may block requests if too many are made. The script includes delays.
- **ms_token**: This token expires, you may need to refresh it periodically.
- **Large Scale**: For crawling thousands of KOLs, consider using proxies and multiple sessions.
- **Legal**: Ensure compliance with TikTok's terms of service and local laws regarding data scraping.

## Troubleshooting

- **Stuck on TikTok Explore page**: The session might be initializing. Try:
  - Set `HEADLESS=0` to see browser state
  - Check if `ms_token` is fresh (refresh from browser cookies)
  - Verify ms_token is correct: `echo $ms_token` in terminal
  - Wait 30-60 seconds (initial session warmup can take time)
  - Check heartbeat logs for "still running" messages to confirm process is active

- **Session test failed / No response**:
  - `ms_token` might be expired or invalid; refresh from TikTok cookies
  - TikTok may have changed API; try with a fresh browser session

- **EmptyResponseException**: TikTok is blocking requests. Try:
  - Using a proxy (not currently supported in no-proxy mode, but future enhancement)
  - Waiting longer between runs (increase delays in code)
  - Refreshing ms_token

- **Browser issues**: Ensure Playwright is properly installed: `python -m playwright install --with-deps`

- **Async errors**: Make sure you're using Python 3.9+

- **Resource leak warnings**: These are normal if cleanup takes time. The `finally` block ensures cleanup happens.
