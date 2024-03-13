"""
Copyright (c) 2024 - Bizware International
"""

'''
Customer Code	Customer Name	Customer Group	Profit Center	Billing Document	Billing Type	Invoice Date	Reference No.	Reference Date
Document No.	Document Type	Balanace Due	Due Date	Due Days	Not Due	Doubtful Debt	Legal	Sales Group	Sales Organization	Division
Above 180 Days	000to30Days	31to60Days	61to90Days	91to120Days	121to150Days	151to180Days	Written Off
'''
from pydantic import BaseModel, Field
import pandas as pd

companyDict = {
    1000: "AISHWARYA HEALTHCARE",
    2000: 'AISHWARYA LIFESCIENCES',
    3000: 'CELEBRITY BIOPHARMA LTD.'
}


class CustomerAgeing(BaseModel):
    customerCode: int = Field(..., alias='Customer Code')
    customerName: str = Field(..., alias='Customer Name')
    customerGroup: str = Field(..., alias='Customer Group')
    profitCenter: str = Field(..., alias='Profit Center')
    billingDocument: int = Field(..., alias='Billing Document')
    billingType: str = Field(..., alias='Billing Type')
    invoiceDate: str = Field(..., alias='Invoice Date')
    referenceNumber: str = Field(..., alias='Reference No.')
    referenceDate: str = Field(..., alias='Reference Date')
    documentNumber: int = Field(..., alias='Document No.')
    documentType: str = Field(..., alias='Document Type')
    balanceDue: str = Field(..., alias='Balanace Due')
    dueDate: str = Field(..., alias='Due Date')
    dueDays: int = Field(..., alias='Due Days')
    notDue: float = Field(..., alias='Not Due')
    doubtfulDebt: str = Field(..., alias='Doubtful Debt')
    legal: str = Field(..., alias='Legal')
    salesGroup: str = Field(..., alias='Sales Group')
    salesOrganization: float = Field(..., alias='Sales Organization')
    division: int = Field(..., alias='Division')
    above180Days: str = Field(..., alias='Above 180 Days')
    zeroTo30Days: str = Field(..., alias='000to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Days')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Days')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Days')
    writtenOff: str = Field(..., alias='Written Off')


class CustomerAgeingReport(BaseModel):
    customerCode: int = Field(..., alias='Customer Code')
    customerName: str = Field(..., alias='Customer Name')
    customerGroup: str = Field(..., alias='Customer Group')
    billingDocument: int = Field(..., alias='Billing Document')
    billingType: str = Field(..., alias='Billing Type')
    invoiceDate: str = Field(..., alias='Invoice Date')
    balanceDue: str = Field(..., alias='Balanace Due')
    dueDate: str = Field(..., alias='Due Date')
    dueDays: int = Field(..., alias='Due Days')
    division: int = Field(..., alias='Division')
    above180Days: str = Field(..., alias='Above 180 Days')
    zeroTo30Days: str = Field(..., alias='000to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Days')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Days')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Days')


def setCustomerAgeingData(ageingData):
    ageing = CustomerAgeing(**ageingData)
    ageingJsonData = ageing.dict()
    print('ageingJsonData -', ageingJsonData)
    return ageingJsonData


def setCustomerAgeingReportData(ageingData):
    ageing = CustomerAgeingReport(**ageingData)
    ageingJsonData = ageing.dict()
    print('ageing Json Report Data -', ageingJsonData)
    return ageingJsonData

def customerAgeingDataLoader(collection_name, ageingData):
    print('Inserting ageing Data')
    collection_name.insert_many(ageingData)
    print('Inserted ageing Data')


def customerAgeingFileReaderAndLoader(filename):
    ageingCompleteData = pd.read_csv(filename, encoding='cp1252')
    ageingCompleteData = ageingCompleteData.fillna('0')
    ageingCompleteDataDict = ageingCompleteData.to_dict(orient='records')
    customerAgeingList = []
    for ageing in ageingCompleteDataDict:
        customerAgeingObj = setCustomerAgeingData(ageing)
        customerAgeingList.append(customerAgeingObj)
    ageingColumns = ageingCompleteData[
        ['Customer Code', 'Customer Name', 'Customer Group', 'Billing Document', 'Billing Type', 'Invoice Date',
         'Balanace Due', 'Due Date', 'Due Days', 'Division', 'Above 180 Days', '000to30Days', '31to60Days',
         '61to90Days', '91to120Days', '121to150Days', '151to180Days']]
    ageingColumns = ageingColumns.fillna('')
    ageingColumns = ageingColumns.to_dict(orient='records')
    customerAgeingReportDataList = []
    for ageing in ageingColumns:
        ageingObj = setCustomerAgeingReportData(ageing)
        customerAgeingReportDataList.append(ageingObj)
    print('Inserting customerAgeingReportDataList to MongoDB -', customerAgeingReportDataList)
    # collection_name.insert_many(salesDataList)
    print('Inserting customerAgeingReportDataList Done')
    return customerAgeingList, customerAgeingReportDataList
