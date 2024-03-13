"""
Copyright (c) 2024 - Bizware International
"""
from typing import Optional, Union, Dict, List

from pydantic.dataclasses import dataclass
# from pydantic.tools import parse_obj_as
from pydantic import BaseModel, Field, parse_obj_as
import json
from pydantic import BaseModel, Field

companyDict = {
    1000: "AISHWARYA HEALTHCARE",
    2000: 'AISHWARYA LIFESCIENCES',
    3000: 'CELEBRITY BIOPHARMA LTD.'
}

plantDict ={
    0o001: 'Werk 0001',
    0o003: 'Plant 0003 (is-ht-sw)',
    1000:	'Aishwarya Healthcare',
    1101:	'Aishwarya Healthcare',
    1102:	'Aishwarya Healthcare',
    1200:	'Aishwarya Healthcare',
    1301:	'Aishwarya Healthcare C&F',
    1302:	'Aishwarya Healthcare C&F',
    1303:	'Aishwarya Healthcare C&F',
    1304:	'Aishwarya Healthcare C&F',
    1305:	'Aishwarya Healthcare C&F',
    1307:	'Aishwarya Healthcare C&F',
    2100:	'Aishwarya Lifesciences',
    2101:	'Aishwarya Lifesciences',
    2102:	'Aishwarya Lifesciences',
    2103:	'Aishwarya Lifesciences',
    2200:	'Aishwarya Lifesciences',
    3100:	'Celebrity Biopharma Limited',
    3200:	'Celebrity Biopharma Limited',
    3301:	'Celebrity Biopharma Limited',
    3302:	'Celebrity Biopharma Limited',
    3303:	'Celebrity Biopharma ltd. C&F',
    3305:	'Celebrity Biopharma ltd. C&F',
    3306:	'Celebrity Biopharma ltd. C&F',
    3307:	'Celebrity Biopharma ltd. C&F',
    3309:	'Celebrity Biopharma ltd. C&F',
    3310:	'Celebrity Biopharma ltd. C&F',
    3311:	'Celebrity Biopharma ltd. C&F',
    3314:	'Celebrity Biopharma ltd. C&F',
    3315:	'Celebrity Biopharma ltd. C&F',
    3316:	'Celebrity Biopharma ltd. C&F',
    3318:	'Celebrity Biopharma ltd. C&F',
    3319:	'Celebrity Biopharma ltd. C&F',
    3320:	'Celebrity Biopharma ltd. C&F'
}


class Sales(BaseModel):
    invoiceNumber: int = Field(..., alias='Invoice No.')
    invoiceDate: str = Field(..., alias='Invoice Date')
    companyCode: int = Field(..., alias='Company Code')
    companyName: str = Field(..., alias='Company Name')
    division: str = Field(..., alias='Division')
    divisionDescription: str = Field(..., alias='Division Description')
    plant: int = Field(..., alias='Plant')
    plantName: str = Field(..., alias='Plant Name')
    billToStateCode: str = Field(..., alias='Bill To State Code')
    billToStateName: str = Field(..., alias='Bill to State Name')
    billingType: str = Field(..., alias='Billing Type')
    shipToDistrict: str = Field(..., alias='Ship To District')
    shipToCity: str = Field(..., alias='Ship To City')
    itemCode: int = Field(..., alias='Item Code')
    itemDescription: str = Field(..., alias='Item Description')
    hsnCode: int = Field(..., alias='HSN Code')
    batchNumber: str = Field(..., alias='Batch Number')
    saleUnitUOM: str = Field(..., alias='Sale Unit (UOM)')
    batchQuantity: str = Field(..., alias='Batch Quantity')
    netAmount: str = Field(..., alias='Net Amount')
    totalAmtAfterTax: str = Field(..., alias='Total Amt After Tax')
    grandTotal: str = Field(..., alias='Grand Total')
    distributionChannel: int = Field(..., alias='Distribution Channel')
    salesEmpolyee: str = Field(..., alias='Sales Empolyee')
    hqCode: str = Field(..., alias='HQ Code')



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

class SalesResponse(BaseModel):
    salesData: Optional[Union[str, List[Dict], Dict]] = None
    year: str
    zone: str
    employeeCode:  Optional[Union[str, Dict, List]] = None
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

# sales_json = json.dumps(dataclasses.asdict(sales))
# print('sales_json =', sales_json)  # '{"id": 123, "name": "James"}'
#
# sales_dict = json.loads(sales_json)
# sales = parse_obj_as(Sales, sales_dict)
# print('sales =', sales)  # User(id=123, name='James')
