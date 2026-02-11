import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

import os
# ... other imports

print("DEBUG: Loader script starting...")  # <--- ADD THIS
print(f"DEBUG: Looking for data in: {os.path.abspath('data/raw')}") # <--- ADD THIS

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Create connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def load_json_to_postgres():
    base_path = "data/raw/telegram_messages"
    all_data = []

    # Iterate through all dates and channels
    for date_folder in os.listdir(base_path):
        date_path = os.path.join(base_path, date_folder)
        if os.path.isdir(date_path):
            for file in os.listdir(date_path):
                if file.endswith(".json"):
                    file_path = os.path.join(date_path, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        all_data.extend(data)

    if not all_data:
        print("No data found to load.")
        return

    df = pd.DataFrame(all_data)
    
    # Load to 'raw' schema (create schema if not exists manually in DB first)
    # Using 'public' for simplicity if you haven't created a 'raw' schema
    df.to_sql('telegram_messages', engine, schema='public', if_exists='replace', index=False)
    print("Data loaded successfully to PostgreSQL!")

if __name__ == "__main__":
    load_json_to_postgres()