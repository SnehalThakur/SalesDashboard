"""
Copyright (c) 2024 - Bizware International
"""
import logging
from datetime import datetime, timedelta
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Union
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, HTTPException, Response, UploadFile, status
from com.bizware.services import Pymongodb
from com.bizware.utils.AWSS3Util import s3_upload, SUPPORTED_FILE_TYPES, s3_download, s3_download_file
from bson import json_util
import json
counter = 1


def job_counter():
    global counter
    counter = counter + 1
    print('cron job: call you https requests here...', counter)


def getS3File():
    s3_download_file()


# @asynccontextmanager
# async def lifespan(_: FastAPI):
#     print('app started....')
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(id="job1", func=getS3File, trigger='cron', minute='*/1')
#     scheduler.start()
#     yield
#     print('app stopped...')
#     scheduler.shutdown(wait=False)
#
#
# app = FastAPI(lifespan=lifespan)
app = FastAPI()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


@app.get("/")
def read_root():
    return {"Hello": "Bizware World"}


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
async def getSalesData():
    # Get the database
    dbname = Pymongodb.get_database()

    # Retrieve a collection named "salesdata" from database
    collectionName = dbname["salesdata"]
    salesData = Pymongodb.getData(collectionName)

    response = json.loads(json_util.dumps(salesData))
    return {'output': response}\

@app.get('/sales-data')
async def getSalesData():
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "sales_data" from database
    collectionName = dbname["sales_data"]
    salesData = Pymongodb.getData(collectionName)
    response = json.loads(json_util.dumps(salesData))

    # datafile = 'com/bizware/data/ZSD_LOG_08-03-2024.csv'
    # response = Pymongodb.loadSalesData('saleswoman', datafile)
    return {'output': response}


@app.get('/customer-ageing-data')
async def getSalesData():
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "ageing_data" from database
    collectionName = dbname["ageing_data"]
    salesData = Pymongodb.getData(collectionName)
    response = json.loads(json_util.dumps(salesData))

    return {'output': response}


@app.get('/customer-ageing-overview-data')
async def getSalesData():
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "ageing_master_data" from database
    collectionName = dbname["ageing_master_data"]
    salesData = Pymongodb.getData(collectionName)
    response = json.loads(json_util.dumps(salesData))

    return {'output': response}


@app.get('/download')
async def download(file_name: str | None = None):
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