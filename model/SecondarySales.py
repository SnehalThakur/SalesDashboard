"""
Copyright (c) 2024 - Bizware International
"""
import logging
from typing import Optional, Union, Dict, List

from bson import json_util
# from pydantic.tools import parse_obj_as
import json
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
import Pymongodb


# Division	Employee code	Employee name	HQ	Stockist code	Stockist Name	Material Code	Material Description
# Material  Category	Month	PTR	Opening Quantity	Sales Quantity	Reciepts Quantity	Transit Quantity	Return Quantity	Closing Quantity	Opening Value
# Sales Value	Reciepts Quantity	Transit Value	Return Value	Closing Value


class SecondarySales(BaseModel):
    Division: str = Field(..., alias='Division')
    EmployeeCode: str = Field(..., alias='Employee code')
    EmployeeName: str = Field(..., alias='Employee name')
    HeadQtr: str = Field(..., alias='HQ')
    StockistCode: str = Field(..., alias='Stockist code')
    StockistName: str = Field(..., alias='Stockist Name')
    MaterialCode: str = Field(..., alias='Material Code')
    MaterialDescription: str = Field(..., alias='Material Description')
    MaterialCategory: str = Field(..., alias='Material  Category')
    Month: str = Field(..., alias='Month')
    PTR: str = Field(..., alias='PTR')
    OpeningQuantity: str = Field(..., alias='Opening Quantity')
    SalesQuantity: str = Field(..., alias='Sales Quantity')
    RecieptsQuantity: str = Field(..., alias='Reciepts Quantity')
    TransitQuantity: str = Field(..., alias='Transit Quantity')
    ReturnQuantity: str = Field(..., alias='Return Quantity')
    ClosingQuantity: str = Field(..., alias='Closing Quantity')
    OpeningValue: str = Field(..., alias='Opening Value')
    SalesValue: str = Field(..., alias='Sales Value')
    RecieptsQuantity: str = Field(..., alias='Reciepts Quantity')
    TransitValue: str = Field(..., alias='Transit Value')
    ReturnValue: str = Field(..., alias='Return Value')
    ClosingValue: str = Field(..., alias='Closing Value')


def setSecondarySalesData(secondarySalesData):
    secondarySalesDt = SecondarySales(**secondarySalesData)
    secondarySalesJsonData = secondarySalesDt.dict()
    # print('secondarySalesJsonData -', secondarySalesJsonData)
    return secondarySalesJsonData


def secondarySalesDataLoader(collection_name, secondarySalesData):
    # print('Inserting secondary sales Data to collection {}'.format(collection_name))
    try:
        collection_name.insert_many(secondarySalesData)
    except:
        logging.error("Error inserting secondarySalesData")
    print('Inserted secondary sales Data to collection {}'.format(collection_name))


def secondarySalesReaderAndLoader(secondarySalesData):
    # salesTargetData = pd.read_csv(filename, encoding='cp1252')
    secondarySalesData = secondarySalesData.fillna('')
    secondarySalesDataDict = secondarySalesData.to_dict(orient='records')
    secondarySalesList = []
    try:
        for secondarySales in secondarySalesDataDict:
            secondarySalesObj = secondarySalesData(secondarySales)
            secondarySalesList.append(secondarySalesObj)
        # print('salesTargetList for MongoDB -', salesTargetList)
        # f = open("salesTargetList.txt", "w")
        # f.write(str(salesTargetList))
        # f.close()
        # collection_name.insert_many(salesDataList)
    except:
        logging.error("Error occurred while secondary Sales Obj mapping")

    print('Inserting secondary Sales List Done')
    return secondarySalesList
