"""
Copyright (c) 2024 - Bizware International
"""

from typing import Union
from pydantic import BaseModel
import uvicorn
from fastapi import FastAPI, HTTPException, Response, status
import Pymongodb
from utils.AWSS3Util import s3_download, s3_download_file
from bson import json_util
import json
from fastapi.middleware.cors import CORSMiddleware
from time import time
from bson import json_util
import logging

# creating the logger object
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
counter = 1


def job_counter():
    global counter
    counter = counter + 1
    logging.info('cron job: call you https requests here...', counter)


def getS3File():
    s3_download_file()


# @asynccontextmanager
# async def lifespan(_: FastAPI):
#     logging.info('app started....')
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(id="job1", func=getS3File, trigger='cron', minute='*/1')
#     scheduler.start()
#     yield
#     logging.info('app stopped...')
#     scheduler.shutdown(wait=False)
#
#
# app = FastAPI(lifespan=lifespan)
app = FastAPI()


origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:4200",
    "https://localhost:4200",
    "http://103.151.107.153:4200",
    "https://103.151.107.153:4200/"
]

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials=True,
                   allow_methods=["*"],
                   allow_headers=["*"])

@app.get("/")
def read_root():
    return {"Hello": "Bizware International"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


# @app.post('/upload')
# async def upload(file: UploadFile | None = None):
#     if not file:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail='No file found!!'
#         )
#
#     contents = await file.read()
#     size = len(contents)
#
#     if not 0 < size <= 1 * MB:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail='Supported file size is 0 - 1 MB'
#         )
#
#     file_type = magic.from_buffer(buffer=contents, mime=True)
#     if file_type not in SUPPORTED_FILE_TYPES:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f'Unsupported file type: {file_type}. Supported types are {SUPPORTED_FILE_TYPES}'
#         )
#     file_name = f'{uuid4()}.{SUPPORTED_FILE_TYPES[file_type]}'
#     await s3_upload(contents=contents, key=file_name)
#     return {'file_name': file_name}

@app.get('/sales-overview-data')
async def getSalesOverviewData():
    startTime_getSalesOverviewData = time()
    # Get the database
    dbname = Pymongodb.get_database()

    # Retrieve a collection named "salesdata" from database
    collectionName = dbname["salesdata"]
    salesData = Pymongodb.getData(collectionName)

    # response = json.loads(json_util.dumps(salesData))
    response = json.dumps(list(salesData), default=json_util.default)

    endTime_getSalesOverviewData = time()
    logging.info("Time taken for GET getSalesOverviewData() {}".format(endTime_getSalesOverviewData - startTime_getSalesOverviewData))
    return {'output': response}


@app.get('/sales-data')
async def getSalesData():
    startTime_getSalesData = time()
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "sales_data" from database
    collectionName = dbname["sales_data"]
    salesData = Pymongodb.getData(collectionName)
    response = json.loads(json_util.dumps(salesData))

    # datafile = 'com/bizware/data/ZSD_LOG_08-03-2024.csv'
    # response = Pymongodb.loadSalesData('saleswoman', datafile)
    endTime_getSalesData = time()
    logging.info("Time taken for GET getSalesData() {}".format(endTime_getSalesData - startTime_getSalesData))
    return {'output': response}


class SalesRequest(BaseModel):
    year: str = None
    month: str = None
    companyCode: str = None


@app.post('/sales-data')
async def getSalesData(request: SalesRequest):
    startTime_getSalesData = time()
    # datafile = 'com/bizware/data/ZSD_LOG_08-03-2024.csv'
    # response = Pymongodb.loadSalesData('saleswoman', datafile)
    response = Pymongodb.getSaleDataByYearMonthCompanyCode(request)
    # response = json.loads(json_util.dumps(salesData))
    # logging.info("Sales Data - ", response)
    # return JSONResponse(response)
    endTime_getSalesData = time()
    logging.info("Time taken for POST getSalesData() {}".format(endTime_getSalesData - startTime_getSalesData))
    return response


@app.get('/customer-ageing-data')
async def getCustomerAgeingData():
    startTime_getCustomerAgeingData = time()
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "ageing_data" from database
    collectionName = dbname["ageing_data"]
    ageingData = Pymongodb.getData(collectionName)
    response = json.loads(json_util.dumps(list(ageingData)))
    # response = json.dumps(list(ageingData), default=json_util.default)
    endTime_getCustomerAgeingData = time()
    logging.info("Time taken for getCustomerAgeingData() {}".format(endTime_getCustomerAgeingData - startTime_getCustomerAgeingData))
    return {'output': response}


@app.get('/customer-ageing-overview-data')
async def getCustomerAgeingOverviewData():
    startTime_getCustomerAgeingOverviewData = time()
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "ageing_master_data" from database
    collectionName = dbname["ageing_master_data"]
    ageingData = Pymongodb.getData(collectionName)
    response = json.loads(json_util.dumps(ageingData))
    # response = json.dumps(list(ageingData), default=json_util.default)
    endTime_getCustomerAgeingOverviewData = time()
    logging.info("Time taken for getAccountReceivables() {}".format(endTime_getCustomerAgeingOverviewData - startTime_getCustomerAgeingOverviewData))
    return {'output': response}


@app.get('/download')
async def download(file_name: str):
    if not file_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='No file name provided'
        )

    contents = await s3_download(key=file_name)
    return Response(
        content=contents,
        headers={
            'Content-Disposition': f'attachment;filename={file_name}',
            'Content-Type': 'application/octet-stream',
        }
    )


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
