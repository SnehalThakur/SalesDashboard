"""
Copyright (c) 2024 - Bizware International
"""
from typing import Optional, Union, Dict, List

from bson import json_util
# from pydantic.tools import parse_obj_as
import json
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
import Pymongodb
import pandas as pd


class SalesTarget(BaseModel):
    Month: int = Field(..., alias='Month')
    Year: int = Field(..., alias='Year')
    ZONE: str = Field(..., alias='Zone', nullable=True)
    # ZONE: Optional['ZONE'] = None
    EmployeeCode: str = Field(..., alias='Employee Code')
    EmployeeName: str = Field(..., alias='Employee Name')
    EmployeeDesignation: str = Field(..., alias='Employee Designation')
    HODEmpCode: str = Field(..., alias='HOD Emp code')
    HODName: str = Field(..., alias='HOD Name')
    Country: str = Field(..., alias='Country')
    RegionState: str = Field(..., alias='Region', nullable=True)
    # RegionState: Optional['Region'] = None
    # HQ: str = Field(..., alias='HQ', nullable=True)
    # HQ: Optional['HQ'] = None
    HQCode: str = Field(..., alias='HQ Code')
    City: str = Field(..., alias='City')
    SAPRegion: int = Field(..., alias='SAP Region')
    SAPRegionDescription: str = Field(..., alias='SAP Region Descripation')
    SAPCity: str = Field(..., alias='SAP City')
    Division: str = Field(..., alias='Division')
    MonthlySalesTarget: int = Field(..., alias='Monthly sales Target')


def setSalesTargetData(salesTargetData):
    salesTargetDt = SalesTarget(**salesTargetData)
    salesTargetJsonData = salesTargetDt.dict()
    # print('salesTargetJsonData -', salesTargetJsonData)
    return salesTargetJsonData


def salesTargetDataLoader(collection_name, salesTargetData):
    # print('Inserting ageing Data to collection {}'.format(collection_name))
    collection_name.insert_many(salesTargetData)
    print('Inserted sales target Data to collection {}'.format(collection_name))


def salesTargetFileReaderAndLoader(filename):
    salesTargetData = pd.read_csv(filename, encoding='cp1252')
    salesTargetData = salesTargetData.fillna('')
    salesTargetDataDict = salesTargetData.to_dict(orient='records')
    salesTargetList = []
    for salesTarget in salesTargetDataDict:
        salesTargetObj = setSalesTargetData(salesTarget)
        salesTargetList.append(salesTargetObj)
    # print('salesTargetList for MongoDB -', salesTargetList)
    # f = open("salesTargetList.txt", "w")
    # f.write(str(salesTargetList))
    # f.close()
    # collection_name.insert_many(salesDataList)
    print('Inserting salesTargetList Done')
    return salesTargetList
