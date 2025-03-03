import os, csv, time
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

supabase_client = create_client(url, key)

response = (supabase_client.schema("silver").rpc("load_silver", None).execute())