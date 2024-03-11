from pydantic.dataclasses import dataclass
# from pydantic.tools import parse_obj_as
from pydantic import BaseModel, Field, parse_obj_as
import json
from pydantic import BaseModel, Field

companyDict = {
    1000: "Aish Connect"
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
