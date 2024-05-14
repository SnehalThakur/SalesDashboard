"""
Copyright (c) 2024 - Bizware International
"""

'''
CustCode.;Doc. No.;CustName;CustGrp;PrfitCentr;DocumeNo.;DocuType;InvDate;DueDate.;ReferNo;ReferenDt;BalanaDue;DueDays.;DoutfulDeb;
NotDue;BillinType;SalesGroup;SalesOrga;Division;BillinDocu;0to30Days;31to60Days;61to90Days;91to120Day;121to150Day;151to180Da;
Abve180Day;Legal;WrittenOff;Due Amount;Overdue Amount;Timestamp;Status

CustCode.;Doc. No.;CustName;CustGrp;PrfitCentr;DocuType;InvDate;DueDate.;ReferNo;ReferenDt;BalanaDue;DueDays.;DoutfulDeb;
NotDue;BillinType;SalesGroup;SalesOrga;Division;BillinDocu;0to30Days;31to60Days;61to90Days;91to120Day;121to150Day;151to180Da;
Abve180Day;Legal;WrittenOff;Due Amount;Overdue Amount;Timestamp;Status

'''
from pydantic import BaseModel, Field, ConfigDict
import pandas as pd

companyDict = {
    1000: "AISHWARYA HEALTHCARE",
    2000: 'AISHWARYA LIFESCIENCES',
    3000: 'CELEBRITY BIOPHARMA LTD.'
}


class CustomerAgeing(BaseModel):
    customerCode: int = Field(..., alias='CustCode.')
    documentNumber: str = Field(..., alias='Doc. No.')
    customerName: str = Field(..., alias='CustName')
    customerGroup: str = Field(..., alias='CustGrp')
    profitCenter: str = Field(..., alias='PrfitCentr')
    billingType: str = Field(..., alias='BillinType')
    invoiceDate: str = Field(..., alias='InvDate')
    referenceNumber: str = Field(..., alias='ReferNo')
    referenceDate: str = Field(..., alias='ReferenDt')
    documentType: str = Field(..., alias='DocuType')
    balanceDue: str = Field(..., alias='BalanaDue')
    dueDate: str = Field(..., alias='DueDate.')
    dueDays: float = Field(..., alias='DueDays.')
    notDue: str = Field(..., alias='NotDue')
    doubtfulDebt: str = Field(..., alias='DoutfulDeb')
    legal: str = Field(..., alias='Legal')
    salesGroup: str = Field(..., alias='SalesGroup')
    salesOrganization: float = Field(..., alias='SalesOrga')
    division: str = Field(..., alias='Division')
    zeroTo30Days: str = Field(..., alias='0to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Day')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Day')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Da')
    above180Days: str = Field(..., alias='Abve180Day')
    writtenOff: str = Field(..., alias='WrittenOff')
    dueAmount: str = Field(..., alias='Due Amount', nullable=True)
    overdueAmount: str = Field(..., alias='Overdue Amount', nullable=True)
    # timestamp: str = Field(..., alias='Timestamp', nullable=True)
    status: str = Field(..., alias='Status', nullable=True)
    model_config = ConfigDict(coerce_numbers_to_str=True)



class CustomerAgeingReport(BaseModel):
    customerCode: int = Field(..., alias='CustCode.')
    documentNumber: int = Field(..., alias='Doc. No.')
    customerName: str = Field(..., alias='CustName')
    customerGroup: str = Field(..., alias='CustGrp')
    billingType: str = Field(..., alias='BillinType')
    invoiceDate: str = Field(..., alias='InvDate')
    balanceDue: str = Field(..., alias='BalanaDue')
    dueDate: str = Field(..., alias='DueDate.')
    dueDays: str = Field(..., alias='DueDays.')
    division: str = Field(..., alias='Division')
    zeroTo30Days: str = Field(..., alias='0to30Days')
    thirtyOneTo60Days: str = Field(..., alias='31to60Days')
    sixtyOneTo90Days: str = Field(..., alias='61to90Days')
    nintyOneTo120Days: str = Field(..., alias='91to120Day')
    oneTwentyOneTo150Days: str = Field(..., alias='121to150Day')
    oneFiftyoneTo180Days: str = Field(..., alias='151to180Da')
    above180Days: str = Field(..., alias='Abve180Day')
    dueAmount: str = Field(..., alias='Due Amount', nullable=True)
    overdueAmount: str = Field(..., alias='Overdue Amount', nullable=True)
    # timestamp: str = Field(..., alias='Timestamp', nullable=True)
    status: str = Field(..., alias='Status', nullable=True)
    model_config = ConfigDict(coerce_numbers_to_str=True)



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


def amountValueConverter(x):
    x = str(x).replace(',', '')
    if "-" in x:
        x = float(x[:-1])
        x *= -1
    return x


def balAmountValueConverter(x):
    x = str(x).split(',')[0]
    x = x.replace('.', '')
    if "-" in x:
        x = float(x[:-1])
        x *= -1
    return x


#  CustCode.;Doc. No.;CustName;CustGrp;PrfitCentr;DocumeNo.;DocuType;InvDate;DueDate.;ReferNo;ReferenDt;BalanaDue;DueDays.;DoutfulDeb;NotDue;
#  BillinType;SalesGroup;SalesOrga;Division;BillinDocu;0to30Days;31to60Days;61to90Days;91to120Day;121to150Day;151to180Da;Abve180Day;
#  Legal;WrittenOff;Due Amount;Overdue Amount;Timestamp;Status
def customerAgeingFileReaderAndLoader(filename):
    ageingCompleteData = pd.read_csv(filename, encoding='cp1252', delimiter=';')
    ageingCompleteData = ageingCompleteData.fillna('0')
    ageingCompleteDataDict = ageingCompleteData.to_dict(orient='records')
    customerAgeingList = []
    for ageing in ageingCompleteDataDict:
        customerAgeingObj = setCustomerAgeingData(ageing)
        customerAgeingList.append(customerAgeingObj)
    ageingColumns = ageingCompleteData[
        ['CustCode.', 'Doc. No.', 'CustName', 'CustGrp',  'BillinType', 'InvDate',
         'BalanaDue', 'DueDate.', 'DueDays.', 'Division', '0to30Days', '31to60Days',
         '61to90Days', '91to120Day', '121to150Day', '151to180Da', 'Abve180Day', 'Due Amount', 'Overdue Amount', 'Status']]
    ageingColumns = ageingColumns.fillna('')
    columnsToFormat = ['BalanaDue', '0to30Days', '31to60Days', '61to90Days', '91to120Day', '121to150Day', '151to180Da', 'Abve180Day', 'Due Amount', 'Overdue Amount']
    for i in columnsToFormat:
        ageingColumns[i] = ageingColumns[i].apply(lambda x: balAmountValueConverter(x))

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
