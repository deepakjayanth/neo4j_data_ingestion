import pandas as pd
from datetime import datetime
from neo4j import GraphDatabase
from tqdm import tqdm
import time
 
# === TIMER HELPERS ===
step_times = {}
overall_start = time.time()
 
def log_step_start(step):
    print(f"\n🚀 Starting {step}...")
    step_times[step] = time.time()
 
def log_step_end(step):
    elapsed = time.time() - step_times[step]
    print(f"✅ {step} completed in {elapsed:.2f} seconds")
    return elapsed
 
# === Step 1: Read CSV ===
step = "Step 1: Reading CSV"
log_step_start(step)
csv_path = "/home/dell/Jagan/LnTDatascience/cleaning_outputs/cleaned_data.csv"
df = pd.read_csv(csv_path, low_memory=False)
print(f"📦 Read {df.shape[0]} rows and {df.shape[1]} columns")
 
print("\n🔎 Unique ID counts per label:")
print(f"PurchaseOrder IDs : {df['ID'].nunique()}")
print(f"MaterialCode      : {df['MaterialCode'].nunique()}")
print(f"WarehouseLocation : {df['WarehouseLocation'].nunique()}")
print(f"VendorCode        : {df['VendorCode'].nunique()}")
print(f"BusinessUnitCode  : {df['BusinessUnitCode'].nunique()}")
 
log_step_end(step)
 
# === Step 2: Format Date Columns ===
step = "Step 2: Formatting Dates"
log_step_start(step)
 
def format_date(date_str):
    try:
        return datetime.strptime(str(date_str), "%Y%m%d").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None
 
date_columns = ['PODate', 'ExpectedDeliveryStartDate', 'ExpectedDeliveryEndDate', 'ActualDeliveryDate', 'ReceivedDate']
for col in date_columns:
    if col in df.columns:
        df[col] = df[col].apply(format_date)
 
log_step_end(step)
 
# === Step 3: Connect to Neo4j ===
step = "Step 3: Connecting to Neo4j"
log_step_start(step)
 
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password123"
driver = GraphDatabase.driver(uri, auth=(username, password))
 
log_step_end(step)
 
# === Step 4: Create Constraints ===
step = "Step 4: Creating Constraints"
log_step_start(step)
 
def create_constraints(tx):
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (po:PurchaseOrder) REQUIRE po.ID IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Material) REQUIRE m.MaterialCode IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (w:Warehouse) REQUIRE w.WarehouseLocation IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (v:Vendor) REQUIRE v.VendorCode IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (b:BusinessUnit) REQUIRE b.BusinessUnitCode IS UNIQUE")
 
with driver.session() as session:
    session.execute_write(create_constraints)
 
log_step_end(step)
 
# === Step 5: Bulk Ingest Nodes ===
def ingest_label(session, label, id_field, columns, chunk_size=20000):
    step = f"Ingesting {label} Nodes"
    log_step_start(step)
 
    before = df.shape[0]
    rows = df[columns].dropna(subset=[id_field]).to_dict(orient="records")
    after = len(rows)
    dropped = before - after
    if dropped > 0:
        print(f"⚠️ Dropped {dropped} rows due to missing `{id_field}`")
 
    query = f"""
        UNWIND $batch AS row
        MERGE (n:{label} {{ {id_field}: row.{id_field} }})
        SET n += row
    """
    for i in tqdm(range(0, len(rows), chunk_size), desc=f"{label}"):
        batch = rows[i:i+chunk_size]
        session.execute_write(lambda tx: tx.run(query, batch=batch))
 
    return log_step_end(step)
 
# === Step 6: Ingest Nodes ===
ingestion_times = {}
with driver.session() as session:
    ingestion_times["PurchaseOrder"] = ingest_label(session, "PurchaseOrder", "ID", [
        "ID", "PONumber", "PurchaseOrderItem", "POQuantity", "POAmount", "POUOM",
        "POPricePerUOM", "DocumentCurrency", "ExchangeRate", "POAmountInINR",
        "POPricePerUOMInINR", "PODate", "PaymentTerms", "ExpectedDeliveryStartDate",
        "ExpectedDeliveryEndDate", "ActualDeliveryDate", "ReceivedDate", "AmountInDocCurrency",
        "MRNNumber", "MRNItemNumber", "WarehouseLocation", "VendorCode", "BusinessUnitCode"
    ])
    ingestion_times["Material"] = ingest_label(session, "Material", "MaterialCode", [
        "MaterialCode", "MRNNumber", "MRNItemNumber", "MaterialGroup", "MaterialGroupText",
        "MaterialQuantity", "MovementType", "MaterialName", "MaterialDescription",
        "WarehouseLocation", "VendorCode"
    ])
    ingestion_times["Warehouse"] = ingest_label(session, "Warehouse", "WarehouseLocation", [
        "WarehouseLocation", "WarehouseCountry", "WarehouseState", "WarehouseCity",
        "WarehousePostalCode", "BusinessUnitCode"
    ])
    ingestion_times["Vendor"] = ingest_label(session, "Vendor", "VendorCode", [
        "VendorCode", "VendorName", "VendorGSTIN", "VendorPostalCode", "VendorCity", "VendorPAN",
        "ContactPersonName", "VendorPhoneNumber", "VendorFullAddress", "VendorCountry",
        "VendorCountryName", "BusinessUnitCode"
    ])
    ingestion_times["BusinessUnit"] = ingest_label(session, "BusinessUnit", "BusinessUnitCode", [
        "BusinessUnitCode", "BusinessUnitDescription", "Business"
    ])
 
# === Step 7: Confirm Node Counts ===
step = "Step 7: Confirming Node Counts"
log_step_start(step)
 
with driver.session() as session:
    result = session.run("""
        MATCH (po:PurchaseOrder) WITH count(po) AS PurchaseOrders
        MATCH (m:Material) WITH PurchaseOrders, count(m) AS Materials
        MATCH (w:Warehouse) WITH PurchaseOrders, Materials, count(w) AS Warehouses
        MATCH (v:Vendor) WITH PurchaseOrders, Materials, Warehouses, count(v) AS Vendors
        MATCH (bu:BusinessUnit)
        RETURN PurchaseOrders, Materials, Warehouses, Vendors, count(bu) AS BusinessUnits
    """)
    record = result.single()
    print("\n🔍 Node Count Summary (in Neo4j):")
    print(f"  PurchaseOrders: {record['PurchaseOrders']}")
    print(f"  Materials     : {record['Materials']}")
    print(f"  Warehouses    : {record['Warehouses']}")
    print(f"  Vendors       : {record['Vendors']}")
    print(f"  BusinessUnits : {record['BusinessUnits']}")
 
log_step_end(step)
 
# === Step 8: Create Relationships ===
step = "Step 8: Creating Relationships"
log_step_start(step)
 
def create_relationships(tx):
    tx.run("""
        MATCH (po:PurchaseOrder), (m:Material)
        WHERE po.MRNNumber = m.MRNNumber AND po.MRNItemNumber = m.MRNItemNumber
        MERGE (po)-[:ORDERS]->(m)
    """)
    tx.run("""
        MATCH (po:PurchaseOrder), (w:Warehouse)
        WHERE po.WarehouseLocation = w.WarehouseLocation
        MERGE (po)-[:DELIVERED_TO]->(w)
    """)
    tx.run("""
        MATCH (po:PurchaseOrder), (v:Vendor)
        WHERE po.VendorCode = v.VendorCode
        MERGE (po)-[:PROCURED_FROM]->(v)
    """)
    tx.run("""
        MATCH (po:PurchaseOrder), (bu:BusinessUnit)
        WHERE po.BusinessUnitCode = bu.BusinessUnitCode
        MERGE (po)-[:RAISED_BY]->(bu)
    """)
    tx.run("""
        MATCH (m:Material), (w:Warehouse)
        WHERE m.WarehouseLocation = w.WarehouseLocation
        MERGE (m)-[:STORED_IN]->(w)
    """)
    tx.run("""
        MATCH (m:Material), (v:Vendor)
        WHERE m.VendorCode = v.VendorCode
        MERGE (m)-[:SUPPLIED_BY]->(v)
    """)
    tx.run("""
        MATCH (v:Vendor), (bu:BusinessUnit)
        WHERE v.BusinessUnitCode = bu.BusinessUnitCode
        MERGE (v)-[:SUPPLIES_TO]->(bu)
    """)
    tx.run("""
        MATCH (w:Warehouse), (bu:BusinessUnit)
        WHERE w.BusinessUnitCode = bu.BusinessUnitCode
        MERGE (w)-[:BELONGS_TO]->(bu)
    """)
 
with driver.session() as session:
    session.execute_write(create_relationships)
 
log_step_end(step)
 
driver.close()
 
# === Final Summary ===
total_time = time.time() - overall_start
print("\n🎉 All steps completed successfully!")
print(f"🕒 Total time elapsed: {total_time:.2f} seconds")
 
print("\n📊 Ingestion Time per Node Type:")
for label, seconds in ingestion_times.items():
    print(f"  {label:<15}: {seconds:.2f} sec")
