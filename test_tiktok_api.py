import json

from TikTokApi import TikTokApi
import asyncio
import os

from dotenv import load_dotenv

load_dotenv()
ms_token = os.environ.get("ms_token", None)  # set your own ms_token


async def get_hashtag_videos():
    async with TikTokApi() as api:
        await api.create_sessions(
            headless=os.getenv("HEADLESS", "1") == "1",
            ms_tokens=[ms_token],
            num_sessions=1,
            sleep_after=3,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )
        tag = api.hashtag(name="funny")
        async for video in tag.videos(count=1):
            print(video)
            print(video.as_dict)


async def user_example():
    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token],
            num_sessions=1,
            sleep_after=3,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )
        user = api.user("justbeminnie")
        user_data = await user.info()
        print(json.dumps(user_data))

        # async for video in user.videos(count=30):
        #     print(video)
        #     print(video.as_dict)

        # async for playlist in user.playlists():
        #     print(playlist)


if __name__ == "__main__":
    # asyncio.run(get_hashtag_videos())
    asyncio.run(get_hashtag_videos())
