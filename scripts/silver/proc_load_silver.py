import os, time
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

supabase_client = create_client(url, key)

def load_silver_crm_cust_info():
    response = (supabase_client.schema("silver").rpc("load_silver_crm_cust_info", None).execute())

def load_silver_crm_prd_info():
    response = (supabase_client.schema("silver").rpc("load_silver_crm_prd_info", None).execute())

def load_silver_crm_sales_details():
    response = (supabase_client.schema("silver").rpc("load_silver_crm_sales_details", None).execute())

def load_silver_erp_cust_az12():
    response = (supabase_client.schema("silver").rpc("load_silver_erp_cust_az12", None).execute())

def load_silver_erp_loc_a101():
    response = (supabase_client.schema("silver").rpc("load_silver_erp_loc_a101", None).execute())

def load_silver_erp_px_cat_g1v2():
    response = (supabase_client.schema("silver").rpc("load_silver_erp_px_cat_g1v2", None).execute())

# Execute insertion
# Start batch processing
batch_start_time = time.time()
print("==========================================")
print("Loading Silver Layer")
print("==========================================")

load_silver_crm_cust_info()
load_silver_crm_prd_info()
load_silver_crm_sales_details()

load_silver_erp_cust_az12()
load_silver_erp_loc_a101()
load_silver_erp_px_cat_g1v2()

# End batch processing
batch_end_time = time.time()
print("==========================================")
print("Loading Silver Layer is Completed")
print(f"   - Total Load Duration: {round(batch_end_time - batch_start_time, 2)} seconds")
print("==========================================")