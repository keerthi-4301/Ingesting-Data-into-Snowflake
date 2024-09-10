import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

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

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST_ROLE",
        database="INGEST_DATABASE",
        schema="INGEST_SCHEMA",
        warehouse="INGEST_WAREHOUSE",
        session_parameters={'QUERY_TAG': 'py-serverless'}, 
    )

def save_to_snowflake(snow, batch, temp_dir):
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=["TXID","RFID","RESORT","PURCHASE_TIME", "EXPIRATION_TIME",
                                            "DAYS","NAME","ADDRESS","PHONE","EMAIL", "EMERGENCY_CONTACT"])
    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path =  f"{temp_dir.name}/{str(uuid.uuid1())}.parquet"
    pq.write_table(arrow_table, out_path, use_dictionary=False,compression='SNAPPY')
    snow.cursor().execute("PUT 'file://{0}' @%LIFT_TICKETS_PY_SERVERLESS".format(out_path))
    os.unlink(out_path)
    # this will be skipped if the task is already scheduled, no warehouse will be used
    # when ran, the task will run as serverless
    snow.cursor().execute("EXECUTE TASK LIFT_TICKETS_PY_SERVERLESS")
    logging.debug(f"{len(batch)} tickets in stage")

if __name__ == "__main__":    
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((record['txid'],record['rfid'],record["resort"],record["purchase_time"],record["expiration_time"],
                        record['days'],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact']))
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir)
                batch = []
        else:
            break    
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")