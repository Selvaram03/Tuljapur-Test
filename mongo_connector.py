from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import pytz

# MongoDB config
MONGO_URI = "mongodb+srv://selvaram58_db_user:cFhijYBal60CGpAi@dgr-demo.dh1kxon.mongodb.net/"
DATABASE_NAME = "scada_db"
COLLECTION_NAME = "opcua_data"

IST = pytz.timezone("Asia/Kolkata")

def get_mongo_client():
    """Returns a MongoDB client."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return None

def normalize_timestamp(ts):
    """
    Converts any timestamp to IST-aware datetime.
    Handles:
    - string timestamps ("YYYY-MM-DD HH:MM")
    - naive datetime (assume IST)
    - UTC-aware datetime
    """
    if isinstance(ts, str):
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M")
        return IST.localize(dt)
    elif isinstance(ts, datetime):
        if ts.tzinfo is None:
            return IST.localize(ts)  # naive → IST
        else:
            return ts.astimezone(IST)  # convert UTC → IST
    return None

def fetch_data_for_timeframe(start_date: datetime, end_date: datetime, last_only=False):
    """
    Fetches data from MongoDB for a date range, normalizes timestamps to IST.
    - start_date / end_date: Python datetime.date or datetime
    - last_only: if True, fetch only the last reading per day
    Returns a list of dictionaries with 'timestamp' as datetime.
    """
    client = get_mongo_client()
    if not client:
        return []

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

    if last_only and start_date == end_date:
        # Single day last record
        cursor = collection.find(
            {"timestamp": {"$regex": f"^{start_str}"}},
            {"_id": 0}
        ).sort("timestamp", -1).limit(1)
    else:
        # Range or all records
        cursor = collection.find(
            {"timestamp": {"$gte": start_str, "$lt": end_str}},
            {"_id": 0}
        ).sort("timestamp", 1)

    data = list(cursor)

    # Normalize timestamps
    for doc in data:
        if "timestamp" in doc:
            doc["timestamp"] = normalize_timestamp(doc["timestamp"])

    client.close()
    return data

def fetch_last_record_of_day(date: datetime):
    """
    Fetches the last record of a single day (23:59 IST) with normalized timestamp.
    """
    return fetch_data_for_timeframe(date, date, last_only=True)

def fetch_yearly_total():
    """SIMULATED: Year-to-date generation for each inverter."""
    yearly_data = {f"Inverter No-{i}": 0 for i in range(1, 19)}
    return pd.DataFrame(yearly_data.items(), columns=['Inverter No.', 'Yearly Generation (KW)'])
