import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

DB_URL = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@localhost/postgres"
