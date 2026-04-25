import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup


async def scrape_tiktok_user(username):
    url = f"https://www.tiktok.com/@{username}"

    async with async_playwright() as p:
        # Launch browser (headless=True means no window pops up)
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Set a realistic User-Agent to avoid immediate blocks
        await page.set_extra_http_headers(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            }
        )

        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="networkidle")

        # Give the page a moment to settle
        await asyncio.sleep(5)

        # Get the rendered HTML
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        # Extracting Data (Note: TikTok class names change frequently)
        try:
            stats = soup.find_all("strong", {"data-e2e": True})
            data = {
                "following": stats[0].text if len(stats) > 0 else "N/A",
                "followers": stats[1].text if len(stats) > 1 else "N/A",
                "likes": stats[2].text if len(stats) > 2 else "N/A",
                "bio": (
                    soup.find("h2", {"data-e2e": "user-bio"}).text
                    if soup.find("h2", {"data-e2e": "user-bio"})
                    else "No Bio"
                ),
            }
            print(f"\nResults for @{username}:")
            for key, value in data.items():
                print(f"{key.capitalize()}: {value}")

        except Exception as e:
            print(f"Error parsing data: {e}")

        await browser.close()


# Run the script
if __name__ == "__main__":
    user_to_scrape = "justbeminnie"  # Change this to the target username
    asyncio.run(scrape_tiktok_user(user_to_scrape))
