"""
Copyright (c) 2024 - Bizware International
"""
from typing import Optional, Union, Dict, List

from bson import json_util
from pymongo import MongoClient
from pydantic.dataclasses import dataclass
# from pydantic.tools import parse_obj_as
from pydantic import BaseModel, Field, parse_obj_as
import json
from pydantic import BaseModel, Field

from com.bizware.services import Pymongodb

companyDict = {
    1000: "AISHWARYA HEALTHCARE",
    2000: 'AISHWARYA LIFESCIENCES',
    3000: 'CELEBRITY BIOPHARMA LTD.'
}

plantDict = {
    0o001: 'Werk 0001',
    0o003: 'Plant 0003 (is-ht-sw)',
    1000: 'Aishwarya Healthcare',
    1101: 'Aishwarya Healthcare',
    1102: 'Aishwarya Healthcare',
    1200: 'Aishwarya Healthcare',
    1301: 'Aishwarya Healthcare C&F',
    1302: 'Aishwarya Healthcare C&F',
    1303: 'Aishwarya Healthcare C&F',
    1304: 'Aishwarya Healthcare C&F',
    1305: 'Aishwarya Healthcare C&F',
    1307: 'Aishwarya Healthcare C&F',
    2100: 'Aishwarya Lifesciences',
    2101: 'Aishwarya Lifesciences',
    2102: 'Aishwarya Lifesciences',
    2103: 'Aishwarya Lifesciences',
    2200: 'Aishwarya Lifesciences',
    3100: 'Celebrity Biopharma Limited',
    3200: 'Celebrity Biopharma Limited',
    3301: 'Celebrity Biopharma Limited',
    3302: 'Celebrity Biopharma Limited',
    3303: 'Celebrity Biopharma ltd. C&F',
    3305: 'Celebrity Biopharma ltd. C&F',
    3306: 'Celebrity Biopharma ltd. C&F',
    3307: 'Celebrity Biopharma ltd. C&F',
    3309: 'Celebrity Biopharma ltd. C&F',
    3310: 'Celebrity Biopharma ltd. C&F',
    3311: 'Celebrity Biopharma ltd. C&F',
    3314: 'Celebrity Biopharma ltd. C&F',
    3315: 'Celebrity Biopharma ltd. C&F',
    3316: 'Celebrity Biopharma ltd. C&F',
    3318: 'Celebrity Biopharma ltd. C&F',
    3319: 'Celebrity Biopharma ltd. C&F',
    3320: 'Celebrity Biopharma ltd. C&F'
}


# Invno.,BillingTyp,CompCode,Plant,PlantName,HeadOffice,InvDate,TaxInvNo.,ActDocNo.,Bill2Party,Bil2PrtNme,Bil2PrtPAN,PartyGSTNo,BillStatCd,BillStatNm,Bill2Distc,Ship2Party,
# ShpPrtyNm,Shp2PAN,Shp2GSTNo,Shp2StatCd,ShpStatNme,Shp2Dist,Shp2City,PayToParty,Pay2PrtNme,MatGroup1,MatGrp1Des,LineItemNo,ItemCode,ItemDes,HSNCode,BatchNo,UOM,BatchQnt,FreeSale,MfgDate,
# ExpDate,ExcRate,MRP,PTR,SalesRate,SalesAmnt,Discnt%,DiscntAmnt,NetAmt,PlateChrgs,Freight,TaxableAmt,IGSTRate,IGSTAmount,CGSTRate,CGSTAmount,SGSTRate,SGSTAmount,TAmtAftTax,TCSBase,TCSAmount,
# RoundOff,GrandTotal,PrtOrderNo,PatOrdDte,VehclNo,LRNo,TrnsprtNam,PymntTerm,NoofBoxes,GrsWightof,DistrnChnl,Division,DivDiscri,SalesOffic,SlsOffDes,SalesGroup,SalsGrpDes,MatGrp,MatGrpDesc,IRNNo,\
#     AcknowleNo,EwayBillNo,ShipmentNo,ShipCost,FreightAmn,SerEntrySh,FreightPON,SalesEmply,HQCode,HospName


class Sales(BaseModel):
    invoiceNumber: int = Field(..., alias='Invno.')
    invoiceDate: str = Field(..., alias='InvDate')
    companyCode: int = Field(..., alias='CompCode')
    companyName: str = Field(..., alias='CompanyName')
    division: int = Field(..., alias='Division')
    divisionDescription: str = Field(..., alias='DivDiscri')
    plant: int = Field(..., alias='Plant')
    plantName: str = Field(..., alias='PlantName')
    billToParty: int = Field(..., alias='Bill2Party')
    billToPartName: str = Field(..., alias='Bil2PrtNme')
    billToStateCode: int = Field(..., alias='BillStatCd', nullable=True)
    billToStateName: str = Field(..., alias='BillStatNm', nullable=True)
    billingType: str = Field(..., alias='BillingTyp')
    # shipToDistrict: str = Field(..., alias='Shp2Dist')
    # shipToCity: str = Field(..., alias='Shp2City')
    shipToParty: int = Field(..., alias='Ship2Party')
    shipPartyName: str = Field(..., alias='ShpPrtyNm')
    itemCode: int = Field(..., alias='ItemCode')
    itemDescription: str = Field(..., alias='ItemDes')
    hsnCode: int = Field(..., alias='HSNCode')
    batchNumber: str = Field(..., alias='BatchNo')
    saleUnitUOM: str = Field(..., alias='UOM')
    batchQuantity: str = Field(..., alias='BatchQnt')
    netAmount: str = Field(..., alias='NetAmt')
    totalAmtAfterTax: str = Field(..., alias='TAmtAftTax')
    grandTotal: str = Field(..., alias='GrandTotal')
    distributionChannel: int = Field(..., alias='DistrnChnl')
    salesEmpolyee: str = Field(..., alias='SalesEmply')
    hqCode: str = Field(..., alias='HQCode')


# Month	Year	ZONE	Employee Code	Employee Name	Employee Designation	HOD Emp code	HOD Name	Country	Region/State	HQ	HQ Code	City	Division	Monthly sales Target

class SecondarySales(BaseModel):
    month: str = Field(..., alias='Month')
    year: str = Field(..., alias='Year')
    zone: str = Field(..., alias='ZONE')
    employeeCode: str = Field(..., alias='Employee Code')
    employeeName: str = Field(..., alias='Employee Name')
    employeeDesignation: str = Field(..., alias='Employee Designation')
    hodEmployeeCode: str = Field(..., alias='HOD Emp code')
    hodName: str = Field(..., alias='HOD Name')
    country: str = Field(..., alias='Country')
    regionState: str = Field(..., alias='Region/State')
    hq: str = Field(..., alias='HQ')
    hqCode: str = Field(..., alias='HQ Code')
    city: str = Field(..., alias='City')
    division: str = Field(..., alias='Division')
    monthlySalesTarget: str = Field(..., alias='Monthly sales Target')


# {"salesData": SalesResponse, "year": SalesRequest.year, "month": SalesRequest.month, "companyCode": SalesRequest.companyCode,
#                 "sales": "Sales Value", "salesTarget": "targetSalesValue", "targetAchievement": "Target ach value", "salesLastYear": "Sales last yr value", "account"}
class SalesResponse(BaseModel):
    salesData: Optional[Union[str, List[Dict], Dict]] = None
    year: str
    month: str
    companyCode: str
    sales: str
    salesTarget: Optional[Union[str, Dict, List]] = None
    targetAchievement: str = None
    salesLastYear: str = None
    accountReceivables: str = None
    overdueReceivablesVal: str = None
    overdueReceivablesPct: str = None


# sales = Sales(invoiceNumber='0090000609', invoiceDate='16-08-2023', companyCode='1000', companyName='aish',
#               billingType='ZST2', division='', divisionDescription='', plant='1101', plantName="None",
#               billToStateCode="None", billToStateName="None", shipToDistrict="None", shipToCity="None", itemCode="None",
#               itemDescription="None", hsnCode="None", batchNumber="None", saleUnit="None", batchQuantity="None",
#               netAmount="None",
#               totalAmountAfterTax="None", grandTotal="None", distributionChannel="None", salesEmployee="None",
#               hqCode="None"
#               )

salesDict = {
    "Invoice No.": 90020978,
    "Invoice Date": "19-10-2023",
    "Company Code": 3000,
    "Company Name": "aish",
    "Division": 2,
    "Division Description": "Services",
    "Plant": 3100,
    "Plant Name": "",
    "Bill To State Code": "27",
    "Bill to State Name": "Maharashtra",
    "Billing Type": "ZSRI",
    "Ship To District": "EAST MUMBAI",
    "Ship To City": "MUMBAI",
    "Item Code": 2000000002,
    "Item Description": "CONVERSION CHARGES",
    "HSN Code": 998843,
    "Batch Number": "None",
    "Sale Unit (UOM)": "EA",
    "Batch Quantity": "1.000",
    "Net Amount": "10,000.00",
    "Total Amt After Tax": "0.00",
    "Grand Total": "11,800.00",
    "Distribution Channel": 10,
    "Sales Empolyee": "O001",
    "HQ Code": "001"
}


def setSalesData(saleData):
    sale = Sales(**saleData)
    saleJsonData = sale.dict()
    # print('saleJsonData -', saleJsonData)
    return saleJsonData


def getSaleDataForRequest(request):
    # Get the database
    dbname = Pymongodb.get_database()
    # Retrieve a collection named "sales_data" from database
    collectionName = dbname["sales_data"]
    salesData = Pymongodb.getData(collectionName)
    SalesResponse = json.loads(json_util.dumps(salesData))
    response = {"salesData": SalesResponse, "year": request.year, "month": request.month,
                "companyCode": request.companyCode,
                "sales": "Sales Value", "salesTarget": "targetSalesValue", "targetAchievement": "Target ach value",
                "salesLastYear": "Sales last yr value",
                "accountReceivables": 'accountReceivables Value', "overdueReceivablesVal": "overdueReceivablesVal",
                "overdueReceivablesPct": "overdueReceivablesPct"}
    return response
# sales_json = json.dumps(dataclasses.asdict(sales))
# print('sales_json =', sales_json)  # '{"id": 123, "name": "James"}'
#
# sales_dict = json.loads(sales_json)
# sales = parse_obj_as(Sales, sales_dict)
# print('sales =', sales)  # User(id=123, name='James')
