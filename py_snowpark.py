import os, sys, logging
import pandas as pd
import json

from snowflake.snowpark import Session
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization


load_dotenv()
logging.basicConfig(level=logging.WARN)


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    
    session = Session.builder.configs({"account":os.getenv("SNOWFLAKE_ACCOUNT"),
                                   "user":os.getenv("SNOWFLAKE_USER"),
                                   "private_key":pkb,
                                   "role":"INGEST_ROLE",
                                   "database":"INGEST_DATABASE",
                                   "SCHEMA":"INGEST_SCHEMA",
                                   "WAREHOUSE":"INGEST_WAREHOUSE"}).create()
    df = session.sql("ALTER SESSION SET QUERY_TAG='py-snowpark'")
    df.collect()
    return session

def save_to_snowflake(snow, batch):
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=["TXID","RFID","RESORT","PURCHASE_TIME", "EXPIRATION_TIME",
                                            "DAYS","NAME","ADDRESS","PHONE","EMAIL", "EMERGENCY_CONTACT"])
    snow.write_pandas(pandas_df, "LIFT_TICKETS_PY_SNOWPARK", auto_create_table=False)
    logging.debug(f"inserted {len(batch)} tickets")


if __name__ == "__main__":    
    args = sys.argv[1:]
    batch_size = int(args[0])
    
    snow = connect_snow()
    batch = []
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((record['txid'],record['rfid'],record["resort"],record["purchase_time"],record["expiration_time"],
                        record['days'],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact']))
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch)
                batch = []
        else:
            break
    if len(batch) > 0:
        save_to_snowflake(snow, batch)    
    snow.close()
    logging.info("Ingest complete")