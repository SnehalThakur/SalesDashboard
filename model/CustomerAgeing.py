"""
Copyright (c) 2024 - Bizware International
"""

'''
Customer Code	Customer Name	Customer Group	Profit Center	Billing Document	Billing Type	Invoice Date	Reference No.	Reference Date
Document No.	Document Type	Balanace Due	Due Date	Due Days	Not Due	Doubtful Debt	Legal	Sales Group	Sales Organization	Division
Above 180 Days	000to30Days	31to60Days	61to90Days	91to120Days	121to150Days	151to180Days	Written Off
'''
from pydantic import BaseModel, Field, ConfigDict
import pandas as pd

companyDict = {
    1000: "AISHWARYA HEALTHCARE",
    2000: 'AISHWARYA LIFESCIENCES',
    3000: 'CELEBRITY BIOPHARMA LTD.'
}


class CustomerAgeing(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    customerCode: int = Field(..., alias='CustCode.')
    customerName: str = Field(..., alias='CustName')
    customerGroup: str = Field(..., alias='CustGrp')
    profitCenter: str = Field(..., alias='PrfitCentr')
    billingDocument: int = Field(..., alias='DocumeNo.')
    billingType: str = Field(..., alias='BillinType')
    invoiceDate: str = Field(..., alias='InvDate')
    referenceNumber: str = Field(..., alias='ReferNo')
    referenceDate: str = Field(..., alias='ReferenDt')
    documentNumber: int = Field(..., alias='DocumeNo.')
    documentType: str = Field(..., alias='DocuType')
    balanceDue: str = Field(..., alias='BalanaDue')
    dueDate: str = Field(..., alias='DueDate.')
    dueDays: str = Field(..., alias='DueDays.')
    notDue: int = Field(..., alias='NotDue')
    doubtfulDebt: str = Field(..., alias='DoutfulDeb')
    legal: str = Field(..., alias='Legal')
    salesGroup: str = Field(..., alias='SalesGroup')
    salesOrganization: float = Field(..., alias='SalesOrga')
    division: int = Field(..., alias='Division')
    zeroTo30Days: str = Field(..., alias='0to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Day')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Da')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Da')
    above180Days: str = Field(..., alias='Abve180Day')
    writtenOff: str = Field(..., alias='WrittenOff')


class CustomerAgeingReport(BaseModel):
    customerCode: int = Field(..., alias='CustCode.')
    customerName: str = Field(..., alias='CustName')
    customerGroup: str = Field(..., alias='CustGrp')
    billingDocument: int = Field(..., alias='DocumeNo.')
    billingType: str = Field(..., alias='BillinType')
    invoiceDate: str = Field(..., alias='InvDate')
    balanceDue: str = Field(..., alias='BalanaDue')
    dueDate: str = Field(..., alias='DueDate.')
    dueDays: str = Field(..., alias='DueDays.')
    division: int = Field(..., alias='Division')
    zeroTo30Days: str = Field(..., alias='0to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Day')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Da')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Da')
    above180Days: str = Field(..., alias='Abve180Day')


'''
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
    dueDays: str = Field(..., alias='Due Days')
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
    above180Days: str = Field(..., alias='Above 180 Days')
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
    dueDays: str = Field(..., alias='Due Days')
    division: int = Field(..., alias='Division')
    above180Days: str = Field(..., alias='Above 180 Days')
    zeroTo30Days: str = Field(..., alias='000to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Days')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Days')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Days')
    above180Days: str = Field(..., alias='Above 180 Days')
'''


def setCustomerAgeingData(ageingData):
    ageing = CustomerAgeing(**ageingData)
    ageingJsonData = ageing.dict()
    # print('ageingJsonData -', ageingJsonData)
    return ageingJsonData


def setCustomerAgeingReportData(ageingData):
    ageing = CustomerAgeingReport(**ageingData)
    ageingJsonData = ageing.dict()
    # print('ageing Json Report Data -', ageingJsonData)
    return ageingJsonData


def customerAgeingDataLoader(collection_name, ageingData):
    # print('Inserting ageing Data to collection {}'.format(collection_name))
    collection_name.insert_many(ageingData)
    print('Inserted ageing Data to collection {}'.format(collection_name))


def customerAgeingFileReaderAndLoader(filename):
    ageingCompleteData = pd.read_csv(filename, encoding='cp1252')
    ageingCompleteData = ageingCompleteData.fillna('0')
    ageingCompleteDataDict = ageingCompleteData.to_dict(orient='records')
    customerAgeingList = []
    for ageing in ageingCompleteDataDict:
        customerAgeingObj = setCustomerAgeingData(ageing)
        customerAgeingList.append(customerAgeingObj)
    ageingColumns = ageingCompleteData[
        ['CustCode.', 'CustName', 'CustGrp', 'DocumeNo.', 'BillinType', 'InvDate',
         'BalanaDue', 'DueDate.', 'DueDays.', 'Division', '0to30Days', '31to60Days',
         '61to90Days', '91to120Day', '121to150Da', '151to180Da', 'Abve180Day']]
    ageingColumns = ageingColumns.fillna('')
    ageingColumns = ageingColumns.to_dict(orient='records')
    customerAgeingReportDataList = []
    for ageing in ageingColumns:
        ageingObj = setCustomerAgeingReportData(ageing)
        customerAgeingReportDataList.append(ageingObj)
    # print('CustomerAgeingList for MongoDB -', customerAgeingList)
    # print('CustomerAgeingReportDataList for MongoDB -', customerAgeingReportDataList)
    f = open("CustomerAgeingList.txt", "w")
    f.write(str(customerAgeingList))
    f.close()
    f = open("CustomerAgeingReportDataList.txt", "w")
    f.write(str(customerAgeingReportDataList))
    f.close()
    # collection_name.insert_many(salesDataList)
    print('Inserting customerAgeingReportDataList Done')
    return customerAgeingList, customerAgeingReportDataList
