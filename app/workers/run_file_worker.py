import glob
from formulatrix_uploader import FormulatrixUploader
import asyncio
import re
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import ispyb.sqlalchemy
import time
import json

async def main(engine, session):
    # Create an instance of the FormulatrixUploader
    config_file_ef = "../../config/config_ef.json"
    config_file_z = "../../config/config_z.json"
    with open(config_file_ef, 'r') as j:
        config_ef = json.loads(j.read())
    with open(config_file_z, 'r') as j:
        config_z = json.loads(j.read())
    
    date_dirs = [glob.glob(f'{config_z["holding_dir"]}/*')]
    ef_files = [glob.glob(f'{config_ef["holding_dir"]}/*.*')]
    start = time.time()
    worker = FormulatrixUploader()
    result_ef = await worker.process_job(ef_files,config_ef,session)
    worker = FormulatrixUploader()
    result_z = await worker.process_job(date_dirs,config_z,session)
    elapsed = time.time() - start
    print(f"{result_ef} \n {result_z} \n Execution Time: {elapsed}")
    #await asyncio.sleep(10)

if __name__ == "__main__":
    credentials_path = "../../config/dbconf.json"
    with open(credentials_path, 'r') as j:
        credentials = json.loads(j.read())

    # Generate Db URL, but use asyncmy driver
    url = re.sub(r'\+(.*?)\:',r'+asyncmy:' , ispyb.sqlalchemy.url(credentials))
    try:
        engine = create_async_engine(url, pool_size=1, max_overflow=1)
        session = sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False
            )
    except Exception as e:
        print(f"Failed to establish ISPyB connection: {e}")
    try:
        asyncio.run(main(engine, session))
    except KeyboardInterrupt:
        print("Exiting uploader...")