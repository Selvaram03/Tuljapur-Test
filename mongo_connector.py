# mongo_connector_patch.py
# This is a drop-in patch for Railway deployment
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import pytz  # timezone conversion

# Use your existing connection
MONGO_URI = "mongodb+srv://selvaram58_db_user:cFhijYBal60CGpAi@dgr-demo.dh1kxon.mongodb.net/" 
DATABASE_NAME = "scada_db"
COLLECTION_NAME = "opcua_data"

IST = pytz.timezone("Asia/Kolkata")  # IST timezone object

def get_mongo_client():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def fetch_data_for_timeframe(start_date: datetime, end_date: datetime):
    """
    Fetch data from MongoDB and convert timestamps from UTC → IST.
    Works live with Railway deployment without downtime.
    """
    client = get_mongo_client()
    if not client:
        return []

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    query = {
        'timestamp': {
            '$gte': start_date,
            '$lt': end_date + timedelta(days=1)
        }
    }

    data = list(collection.find(query).sort("timestamp", 1))

    # UTC → IST conversion
    for doc in data:
        if "timestamp" in doc and doc["timestamp"] is not None:
            ts_utc = pd.to_datetime(doc["timestamp"], utc=True)
            ts_ist = ts_utc.tz_convert(IST)
            doc["timestamp"] = ts_ist.to_pydatetime()  # naive IST datetime

    client.close()
    return data

def fetch_yearly_total():
    yearly_data = {
        "Inverter No-1": 0, "Inverter No-2": 0, "Inverter No-3": 0,
        "Inverter No-4": 0, "Inverter No-5": 0, "Inverter No-6": 0,
        "Inverter No-7": 0, "Inverter No-8": 0, "Inverter No-9": 0,
        "Inverter No-10": 0, "Inverter No-11": 0, "Inverter No-12": 0,
        "Inverter No-13": 0, "Inverter No-14": 0, "Inverter No-15": 0,
        "Inverter No-16": 0, "Inverter No-17": 0, "Inverter No-18": 0,
    }
    df_yearly = pd.DataFrame(yearly_data.items(), columns=['Inverter No.', 'Yearly Generation (KW)'])
    return df_yearly