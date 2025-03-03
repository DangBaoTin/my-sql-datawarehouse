import os, csv, time
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

supabase_client = create_client(url, key)

BATCH_SIZE = 1000  # Adjust as needed

def reset_tables():
    response = (supabase_client.rpc("reset_bronze_tables").execute())

# Read CSV and insert into Supabase
def insert_crm_cust_info():
    file_path = "datasets/source_crm/cust_info.csv"
    data_to_insert = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Skip rows that have missing or empty 'cst_id' or 'cst_key'
            if not row["cst_id"].strip() or not row["cst_key"].strip():
                print(f"Skipping crm_cust_info malformed row: {row}")
                continue

            # Format row as a dictionary (trim spaces if needed)
            data = {
                "cst_id": int(row["cst_id"]),
                "cst_key": row["cst_key"].strip(),
                "cst_firstname": row["cst_firstname"].strip(),
                "cst_lastname": row["cst_lastname"].strip(),
                "cst_marital_status": row["cst_marital_status"].strip(),
                "cst_gndr": row["cst_gndr"].strip(),
                "cst_create_date": row["cst_create_date"].strip(),
            }
            data_to_insert.append(data)

    # Insert data in batches (to avoid exceeding API limits)
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        response = (supabase_client.schema("bronze").table("crm_cust_info").insert(batch).execute())
        print(f"Inserted crm_cust_info {len(batch)} records: {response}")

def insert_crm_prd_info():
    file_path = "datasets/source_crm/prd_info.csv"
    data_to_insert = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if not row["prd_id"].strip() or not row["prd_cost"].strip():
                print(f"Skipping crm_prd_info malformed row: {row}")
                continue

            data = {
                "prd_id": int(row["prd_id"]),
                "prd_key": row["prd_key"].strip(),
                "prd_nm": row["prd_nm"].strip(),
                "prd_cost": int(row["prd_cost"]),
                "prd_line": row["prd_line"].strip(),
                "prd_start_dt": row["prd_start_dt"].strip() if row["prd_start_dt"].strip() else None,
                "prd_end_dt": row["prd_end_dt"].strip() if row["prd_end_dt"].strip() else None,
            }
            data_to_insert.append(data)

    # Insert data in batches (to avoid exceeding API limits)
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        response = (supabase_client.schema("bronze").table("crm_prd_info").insert(batch).execute())
        print(f"Inserted crm_prd_info {len(batch)} records: {response}")

def insert_crm_sales_details():
    file_path = "datasets/source_crm/sales_details.csv"
    data_to_insert = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if not row["sls_cust_id"].strip() or not row["sls_order_dt"].strip() or not row["sls_ship_dt"].strip() or not row["sls_due_dt"].strip() or not row["sls_sales"].strip() or not row["sls_quantity"].strip() or not row["sls_price"].strip():
                print(f"Skipping crm_sales_details malformed row: {row}")
                continue

            data = {
                "sls_ord_num": row["sls_ord_num"].strip(),
                "sls_prd_key": row["sls_prd_key"].strip(),
                "sls_cust_id": int(row["sls_cust_id"]),
                "sls_order_dt": int(row["sls_order_dt"]),
                "sls_ship_dt": int(row["sls_ship_dt"]),
                "sls_due_dt": int(row["sls_due_dt"]),
                "sls_sales": int(row["sls_sales"]),
                "sls_quantity": int(row["sls_quantity"]),
                "sls_price": int(row["sls_price"]),
            }
            data_to_insert.append(data)

    # Insert data in batches (to avoid exceeding API limits)
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        response = (supabase_client.schema("bronze").table("crm_sales_details").insert(batch).execute())
        print(f"Inserted crm_sales_details {len(batch)} records: {response}")

def insert_erp_cust_az12():
    file_path = "datasets/source_erp/CUST_AZ12.csv"
    data_to_insert = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:

            data = {
                "cid": row["CID"].strip(),
                "bdate": row["BDATE"].strip(),
                "gen": row["GEN"].strip(),
            }
            data_to_insert.append(data)

    # Insert data in batches (to avoid exceeding API limits)
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        response = (supabase_client.schema("bronze").table("erp_cust_az12").insert(batch).execute())
        print(f"Inserted erp_cust_az12 {len(batch)} records: {response}")

def insert_erp_loc_a101():
    file_path = "datasets/source_erp/LOC_A101.csv"
    data_to_insert = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:

            data = {
                "cid": row["CID"].strip(),
                "cntry": row["CNTRY"].strip(),
            }
            data_to_insert.append(data)

    # Insert data in batches (to avoid exceeding API limits)
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        response = (supabase_client.schema("bronze").table("erp_loc_a101").insert(batch).execute())
        print(f"Inserted erp_loc_a101 {len(batch)} records: {response}")

def insert_erp_px_cat_g1v2():
    file_path = "datasets/source_erp/PX_CAT_G1V2.csv"
    data_to_insert = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:

            data = {
                "id": row["ID"].strip(),
                "cat": row["CAT"].strip(),
                "subcat": row["SUBCAT"].strip(),
                "maintenance": row["MAINTENANCE"].strip(),
            }
            data_to_insert.append(data)

    # Insert data in batches (to avoid exceeding API limits)
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        response = (supabase_client.schema("bronze").table("erp_px_cat_g1v2").insert(batch).execute())
        print(f"Inserted erp_px_cat_g1v2 {len(batch)} records: {response}")

reset_tables()

# Execute insertion
# Start batch processing
batch_start_time = time.time()
print("==========================================")
print("Loading Bronze Layer")
print("==========================================")

# Load CRM Tables
insert_crm_cust_info()
insert_crm_prd_info()
insert_crm_sales_details()

# Load ERP Tables
insert_erp_cust_az12()
insert_erp_loc_a101()
insert_erp_px_cat_g1v2()

# End batch processing
batch_end_time = time.time()
print("==========================================")
print("Loading Bronze Layer is Completed")
print(f"   - Total Load Duration: {round(batch_end_time - batch_start_time, 2)} seconds")
print("==========================================")