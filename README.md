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

3. **Get ms_token**:
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

## Notes

- **Rate Limiting**: TikTok may block requests if too many are made. The script includes delays.
- **ms_token**: This token expires, you may need to refresh it periodically.
- **Large Scale**: For crawling thousands of KOLs, consider using proxies and multiple sessions.
- **Legal**: Ensure compliance with TikTok's terms of service and local laws regarding data scraping.

## Troubleshooting

- **EmptyResponseException**: TikTok is blocking requests. Try using a proxy or different ms_token.
- **Browser issues**: Ensure Playwright is properly installed: `python -m playwright install`
- **Async errors**: Make sure you're using Python 3.9+