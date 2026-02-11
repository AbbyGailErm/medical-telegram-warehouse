import os
import json
import logging
import asyncio
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv

import os
# ... other imports

print("DEBUG: Loader script starting...")  # <--- ADD THIS
print(f"DEBUG: Looking for data in: {os.path.abspath('data/raw')}") # <--- ADD THIS

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=f"logs/scraper_{datetime.now().strftime('%Y%m%d')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
SESSION_NAME = os.getenv("TG_SESSION_NAME")

# Channels to scrape
CHANNELS = [
    'CheMed123',
    'lobelia4cosmetics',
    'tikvahpharma',
    # Add more channels here
]

async def scrape_channel(client, channel_name):
    print(f"Scraping {channel_name}...")
    logging.info(f"Started scraping {channel_name}")
    
    today = datetime.now().strftime("%Y-%m-%d")
    data_dir = f"data/raw/telegram_messages/{today}"
    image_dir = f"data/raw/images/{channel_name}"
    
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(image_dir, exist_ok=True)
    
    messages_data = []
    
    try:
        # Get last 100 messages (adjust limit as needed)
        async for message in client.iter_messages(channel_name, limit=100):
            msg_dict = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.isoformat() if message.date else None,
                "message_text": message.text,
                "has_media": bool(message.media),
                "image_path": None,
                "views": message.views,
                "forwards": message.forwards,
            }

            # Download Image if present
            if message.photo:
                path = f"{image_dir}/{message.id}.jpg"
                await client.download_media(message.photo, file=path)
                msg_dict["image_path"] = path

            messages_data.append(msg_dict)
            
        # Save JSON
        output_file = f"{data_dir}/{channel_name}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(messages_data, f, ensure_ascii=False, indent=4)
            
        logging.info(f"Successfully scraped {len(messages_data)} messages from {channel_name}")

    except Exception as e:
        logging.error(f"Error scraping {channel_name}: {str(e)}")
        print(f"Error: {e}")

async def main():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        for channel in CHANNELS:
            await scrape_channel(client, channel)

if __name__ == "__main__":
    asyncio.run(main())