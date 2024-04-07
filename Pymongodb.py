"""
Copyright (c) 2024 - Bizware International
"""
from pymongo import MongoClient
# from com.bizware.config import settings
import urllib.parse
import pandas as pd
import model.Sales as sales
import model.CustomerAgeing as ageing
# importing date class from datetime module
from datetime import date, datetime, timedelta
import calendar
# import motor
from bson import json_util
import json

# creating the date object of today's date
todays_date = date.today()
# todays_date = date(2023, 9, 1)
current_year = todays_date.year
current_month = todays_date.month
# current_month_text = todays_date.strftime("%B") # March
current_month_text = todays_date.strftime("%b")
current_day = todays_date.day
year_entire = todays_date.strftime("%Y")


# # printing todays date
# print("Current date: ", todays_date)
#
# # fetching the current year, month and day of today
# print("Current year:", current_year)
# print("Current month:", current_month)
# print("Current month :", current_month_text)
# print("Current day:", current_day)


# get previous month
def getPreviousMonth():
    my_variable = todays_date
    # my_variable = "2024-05-10"
    d1 = datetime.strptime(str(my_variable), '%Y-%m-%d')
    days = d1.day
    # use timedelta to subtract n+1 days from current datetime object
    d2 = d1 - timedelta(days=days + 1)
    # get month of d2
    # print(d2.month)
    # get month name
    # previous_month_text = calendar.month_name[d2.month]   # March
    previousMonthText = calendar.month_abbr[d2.month]  # Mar
    # print('Previous Month :', previousMonthText)
    return d2.month, previousMonthText


previous_month, previous_month_text = getPreviousMonth()

# Provide the mongodb atlas url to connect python to mongodb using pymongo
# CONNECTION_STRING = f"mongodb+srv://{settings.database_username}:{settings.database_password}@{settings.database_port}/{settings.database_name}"
# CONNECTION_STRING = "mongodb+srv://bizware:<password>@bizwarecluster.rraehrb.mongodb.net/?retryWrites=true&w=majority&appName=BizwareCluster"
# password = urllib.parse.quote_plus('bizware@1234')
# CONNECTION_STRING = f"mongodb+srv://bizware:{password}@bizwarecluster.rraehrb.mongodb.net"

# local db connection
CONNECTION_STRING = f"mongodb://localhost:27017/"

# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
client = MongoClient(CONNECTION_STRING)


# client = motor.AsyncIOMotorClient(CONNECTION_STRING)


def get_session():
    db = client()
    try:
        yield db
    finally:
        db.close()


def get_database():
    # Create/Return the database
    return client['aishconnect']


def createTableUniqueIndex(collectionName):
    # collectionName.createIndex({'invoiceNumber': 1}, {unique= true})
    index = collectionName.create_index([('invoiceNumber', 1)], unique=True)
    print("Created Index for {} and index {}".format(collectionName, index))


def loadData(collection_name, datafile):
    data = pd.read_csv(datafile)
    datadict = data.to_dict(orient='records')
    # print('datadict -', datadict)
    collection_name.insert_many(datadict)


# Invno.,BillingTyp,CompCode,Plant,PlantName,HeadOffice,InvDate,TaxInvNo.,ActDocNo.,Bill2Party,Bil2PrtNme,Bil2PrtPAN,PartyGSTNo,BillStatCd,BillStatNm,Bill2Distc,Ship2Party,ShpPrtyNm,Shp2PAN,
# Shp2GSTNo,Shp2StatCd,ShpStatNme,Shp2Dist,Shp2City,PayToParty,Pay2PrtNme,MatGroup1,MatGrp1Des,LineItemNo,ItemCode,ItemDes,HSNCode,BatchNo,UOM,BatchQnt,FreeSale,MfgDate,ExpDate,ExcRate,MRP,PTR,SalesRate,
# SalesAmnt,Discnt%,DiscntAmnt,NetAmt,PlateChrgs,Freight,TaxableAmt,IGSTRate,IGSTAmount,CGSTRate,CGSTAmount,SGSTRate,SGSTAmount,TAmtAftTax,TCSBase,TCSAmount,RoundOff,GrandTotal,PrtOrderNo,PatOrdDte,VehclNo,
# LRNo,TrnsprtNam,PymntTerm,NoofBoxes,GrsWightof,DistrnChnl,Division,DivDiscri,SalesOffic,SlsOffDes,SalesGroup,SalsGrpDes,MatGrp,MatGrpDesc,IRNNo,AcknowleNo,EwayBillNo,ShipmentNo,ShipCost,FreightAmn,SerEntrySh,
#     FreightPON,SalesEmply,HQCode,HospName


# Load sales data
def loadSalesData(collection_name, datafile):
    data = pd.read_csv(datafile, encoding='cp1252')
    salesColumns = data[
        ['CompCode', 'Invno.', 'InvDate', 'BillingTyp', 'Plant', 'PlantName', 'Division',
         'DivDiscri', 'Bill2Party', 'Bil2PrtNme', 'BillStatCd', 'BillStatNm', 'Bill2Distc', 'Ship2Party', 'ShpPrtyNm',
         'ItemCode', 'ItemDes', 'HSNCode', 'BatchNo', 'UOM', 'BatchQnt', 'NetAmt',
         'TAmtAftTax', 'GrandTotal', 'DistrnChnl', 'SalesEmply', 'HQCode']]

    salesColumns['CompanyName'] = salesColumns['CompCode'].map(sales.companyDict)
    salesColumns[
        ['CompCode', 'Division', 'Ship2Party', 'Plant', 'BillStatCd', 'ItemCode', 'HSNCode', 'DistrnChnl',
         'Bill2Party']] = \
        salesColumns[['CompCode', 'Division', 'Ship2Party', 'Plant', 'BillStatCd', 'ItemCode', 'HSNCode',
                      'DistrnChnl', 'Bill2Party']].fillna(0)
    salesColumns = salesColumns.fillna('')
    salesColumns = salesColumns.to_dict(orient='records')
    salesDataList = []
    for sale in salesColumns:
        salesObj = sales.setSalesData(sale)
        salesDataList.append(salesObj)
    # print('Inserting salesDataList to MongoDB -', salesDataList)
    f = open("salesData.txt", "w")
    f.close()
    # collection_name.insert_many(salesDataList)
    # print('Inserted salesDataList Data to collection - {}'.format(collection_name))
    return salesDataList


def getCurrentMonthSales(salesDf):
    currentMonthSales = salesDf[(salesDf["invoiceYear"].astype(int) == current_year) & (
            salesDf["invoiceMonth"] == calendar.month_abbr[current_month])].groupby(
        ['invoiceMonth', 'invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    currentMonthSalesList = []
    for key, val in currentMonthSales.items():
        currentMonthSalesDict = {"invoiceMonth": key[0],
                                 "invoiceYear": key[1],
                                 "grandTotal": val
                                 }
        currentMonthSalesList.append(currentMonthSalesDict)
    return currentMonthSalesList


def getPreviousMonthSales(salesDf):
    previousMonthSales = salesDf[(salesDf["invoiceYear"].astype(int) == current_year) & (
            salesDf["invoiceMonth"] == calendar.month_abbr[previous_month])].groupby(
        ['invoiceMonth', 'invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    previousMonthSalesList = []
    for key, val in previousMonthSales.items():
        previousMonthSalesDict = {"invoiceMonth": key[0],
                                  "invoiceYear": key[1],
                                  "grandTotal": val
                                  }
        previousMonthSalesList.append(previousMonthSalesDict)
    return previousMonthSalesList


def getCurrentYearSales(salesDf):
    currentMonthSales = salesDf[salesDf["invoiceYear"].astype(int) == current_year].groupby(
        ['invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    currentYearSalesList = []
    for key, val in currentMonthSales.items():
        currentYearSalesDict = {"invoiceYear": key[0],
                                "grandTotal": val
                                }
        currentYearSalesList.append(currentYearSalesDict)
    return currentYearSalesList


def getPreviousYearSales(salesDf):
    previousYear = current_year - 1
    previousYearSales = salesDf[salesDf["invoiceYear"].astype(int) == previousYear].groupby(
        ['invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    previousYearSalesList = []
    for key, val in previousYearSales.items():
        previousYearSalesDict = {"invoiceYear": key[0],
                                 "grandTotal": val
                                 }
        previousYearSalesList.append(previousYearSalesDict)
    return previousYearSalesList


def getCurrentMonthPreviousYearSales(salesDf):
    currentMonthPreviousYearSales = salesDf[(salesDf["invoiceYear"].astype(int) == current_year - 1) & (
            salesDf["invoiceMonth"] == calendar.month_abbr[current_month])].groupby(
        ['invoiceMonth', 'invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    currentMonthPreviousYearSalesList = []
    for key, val in currentMonthPreviousYearSales.items():
        currentMonthPreviousYearSalesDict = {"invoiceMonth": key[0],
                                             "invoiceYear": key[1],
                                             "grandTotal": val
                                             }
        currentMonthPreviousYearSalesList.append(currentMonthPreviousYearSalesDict)
    return currentMonthPreviousYearSalesList


def getIndicatorCurrentMonthYearVsLastMonthYear(salesDf):
    currentMonthSales = 0
    currentMonthSalesDict = {}
    if len(getCurrentMonthSales(salesDf)) > 0:
        currentMonthSalesDict = getCurrentMonthSales(salesDf)[0]
        currentMonthSales = currentMonthSalesDict.get('grandTotal')

    previousMonthSales = 0
    previousMonthSalesDict = {}
    if len(getPreviousMonthSales(salesDf)) > 0:
        previousMonthSalesDict = getPreviousMonthSales(salesDf)[0]
        previousMonthSales = previousMonthSalesDict.get('grandTotal')

    rateChangeInPercent = round((currentMonthSales - previousMonthSales) * 100 / previousMonthSales, 3)

    currentYearSales = 0
    currentYearSalesDict = {}
    if len(getCurrentYearSales(salesDf)) > 0:
        currentYearSalesDict = getCurrentYearSales(salesDf)[0]
        currentYearSales = currentYearSalesDict.get('grandTotal')

    previousYearSales = 0
    previousYearSalesDict = {}
    if len(getPreviousYearSales(salesDf)) > 0:
        previousYearSalesDict = getPreviousYearSales(salesDf)[0]
        previousYearSales = previousYearSalesDict.get('grandTotal')

    rateChangeInPercentYearly = round((currentYearSales - previousYearSales) * 100 / previousYearSales, 3)

    currentMonthPreviousYearSales = 0
    currentMonthPreviousYearSalesDict = {}
    if len(getCurrentMonthPreviousYearSales(salesDf)) > 0:
        currentMonthPreviousYearSalesDict = getCurrentMonthPreviousYearSales(salesDf)[0]
        currentMonthPreviousYearSales = currentMonthPreviousYearSalesDict.get('grandTotal')

    rateChangeInPercentCurrentMonthLastYear = round((currentMonthSales - currentMonthPreviousYearSales) * 100 / currentMonthPreviousYearSales, 3)
    # rateOfChangeInPercent = round((((previousMonthSales - currentMonthSales) / previousMonthSales) * 100), 3)
    indicatorList = []
    indicator = {
        "indicatorName": "Monthly Sale",
        "period": f"{current_month_text} (this month) vs {previous_month_text} (last month)",
        "currentMonthSales": currentMonthSalesDict,
        "previousMonthSales": previousMonthSalesDict,
        "rateChange": rateChangeInPercent,
        "currentYearSales": currentMonthSalesDict,
        "previousYearSales": previousYearSalesDict,
        "rateChangeYear": rateChangeInPercentYearly,
        "currentMonthPreviousYearSales": currentMonthPreviousYearSales,
        "rateChangeInPercentCurrentMonthLastYear": rateChangeInPercentCurrentMonthLastYear
    }
    indicatorList.append(indicator)
    return indicatorList


def getTotalSale(currentMonthYearVsLastMonthYear):
    totalSales = {"MoM": currentMonthYearVsLastMonthYear["currentMonthSales"]['grandTotal'],
                  "MoMPct": currentMonthYearVsLastMonthYear["rateChange"],
                  "YoY": currentMonthYearVsLastMonthYear["currentYearSales"]['grandTotal'],
                  "YoYPct": currentMonthYearVsLastMonthYear["rateChangeYear"]}
    return totalSales


def getSalesTarget():
    salesTarget = {"MoM": "80.94",
                   "MoMPct": "43",
                   "YoY": "90.96",
                   "YoYPct": "43"}
    return salesTarget


def getTargetAchievement():
    targetAchievement = {"MoM": "90.99",
                         "MoMPct": "43",
                         "YoY": "99.96",
                         "YoYPct": "43"}
    return targetAchievement


def getSalesLastYear(currentMonthYearVsLastMonthYear):
    salesLastYear = {"MoM": currentMonthYearVsLastMonthYear["currentMonthPreviousYearSales"],
                     "MoMPct": currentMonthYearVsLastMonthYear["rateChangeInPercentCurrentMonthLastYear"],
                     "YoY": currentMonthYearVsLastMonthYear["previousYearSales"]['grandTotal'],
                     "YoYPct": currentMonthYearVsLastMonthYear["rateChangeYear"]}
    return salesLastYear


def getAccountReceivables():
    accountReceivables = {"accountReceivablesVal": "90.99",
                          "accountReceivablesPct": "43"}
    return accountReceivables


def getOverdueReceivables():
    overdueReceivables = {"overdueReceivablesVal": "90.99",
                          "overdueReceivablesPct": "43"}
    return overdueReceivables


def getTopCustomers(salesDf):
    topCustomers = []
    # topCustomersGroupByBillToPartyGrandTotalSum = salesDf.groupby(['billToParty', 'billToPartName'])['grandTotal'].sum()
    # topCustomersGroupByBillToPartyGrandTotalSum = salesDf.groupby(['billToPartName'])['grandTotal'].sum().sort_values(ascending=False).head(5).to_dict()
    topCustomersGroupByBillToPartyGrandTotalSum = salesDf.groupby(['billToParty', 'billToPartName'])[
        'grandTotal'].sum().sort_values(ascending=False).head(5).to_dict()
    topCustomersList = []
    for key, val in topCustomersGroupByBillToPartyGrandTotalSum.items():
        topCustomersDict = {"customerCode": key[0],
                            "customerName": key[1],
                            "grandTotal": val
                            }
        topCustomersList.append(topCustomersDict)
    print("Get Grand Total sum of the grouped data by billToParty, billToPartName : ", topCustomersList)
    return topCustomersList


def getTopProducts(salesDf):
    topProducts = []
    # topProductsGroupByItemDesGrandTotalSum = salesDf.groupby(['itemCode', 'itemDescription'])['grandTotal'].sum()
    topProductsGroupByItemDesGrandTotalSum = salesDf.groupby(['itemCode', 'itemDescription'])[
        'grandTotal'].sum().sort_values(ascending=False).head(5).to_dict()
    topProductsList = []
    for key, val in topProductsGroupByItemDesGrandTotalSum.items():
        topProductsDict = {"divisionCode": key[0],
                           "divisionName": key[1],
                           "grandTotal": val
                           }
        topProductsList.append(topProductsDict)
    print("Get Grand Total sum of the grouped data by itemCode, itemDescription : ", topProductsList)
    return topProductsList


def getTopDivisions(salesDf):
    topDivisions = []
    # topDivisionsGroupByItemDesGrandTotalSum = salesDf.groupby(['division', 'divisionDescription'])['grandTotal'].sum()
    topDivisionsGroupByItemDesGrandTotalSum = salesDf.groupby(['division', 'divisionDescription'])[
        'grandTotal'].sum().sort_values(ascending=False).head(5).to_dict()
    topDivisionsList = []
    for key, val in topDivisionsGroupByItemDesGrandTotalSum.items():
        topDivisionDict = {"divisionCode": key[0],
                           "divisionName": key[1],
                           "grandTotal": val
                           }
        topDivisionsList.append(topDivisionDict)
    print("Get Grand Total sum of the grouped data by division, divisionDescription : ", topDivisionsList)
    return topDivisionsList


def getTop5Performers(salesDf):
    # top5PerformersGroupBySalesEmpolyeeGrandTotalSum = salesDf.groupby('salesEmpolyee')['grandTotal'].sum()
    top5PerformersGroupBySalesEmpolyeeGrandTotalSum = salesDf.groupby('salesEmpolyee')['grandTotal'].sum().sort_values(
        ascending=False).head(5).to_dict()
    print("Get Grand Total sum of the grouped data by salesEmpolyee : ",
          top5PerformersGroupBySalesEmpolyeeGrandTotalSum)
    return top5PerformersGroupBySalesEmpolyeeGrandTotalSum


def getSaleDataByYearMonthCompanyCode(request):
    # get database obj
    dbname = get_database()

    # Retrieve a collection named "sales_data" from database
    collection_name = dbname["sales_data"]
    itemDetails = collection_name.find()
    # st_date = date(2023, 1, 1)
    # end_date = date(2024, 1, 1)
    # itemDetails = collection_name.find({
    #     'invoiceDate': {
    #         '$and': [
    #             {'$gte': ['$st_date', st_date],
    #              },
    #             {
    #                 '$lte': ['$end_date', end_date],
    #             }
    #         ]
    #     }
    # })
    # salesDf = pd.json_normalize(itemDetails)
    itemList = []
    for item in itemDetails:
        # This does not give a very readable output
        itemList.append(item)
    # itemList = list(itemDetails)
    salesDf = pd.DataFrame(itemList)
    salesDf['grandTotal'] = salesDf['grandTotal'].str.replace(',', '').astype('float64')
    # salesDf['invoiceDate'] = salesDf['invoiceDate'].str.replace("/", "-")
    salesDf['invoiceMonth'] = salesDf['invoiceDate'].str.split("-", expand=True)[1].apply({
        lambda x: calendar.month_abbr[int(x)]
    })
    salesDf['invoiceYear'] = salesDf['invoiceDate'].str.split("-", expand=True)[2]
    currentMonthYearVsLastMonthYearStats = getIndicatorCurrentMonthYearVsLastMonthYear(salesDf)
    salesDataDict = {"salesData": json.loads(json_util.dumps(itemList)),
                     "totalSales": getTotalSale(currentMonthYearVsLastMonthYearStats[0]),
                     "salesTarget": getSalesTarget(),
                     "targetAchievement": getTargetAchievement(),
                     "salesLastYear": getSalesLastYear(currentMonthYearVsLastMonthYearStats[0]),
                     "accountReceivables": getAccountReceivables(),
                     "overdueReceivables": getOverdueReceivables(),
                     "topCustomers": getTopCustomers(salesDf),  # list
                     "topProducts": getTopProducts(salesDf),  # list
                     "topDivisions": getTopDivisions(salesDf),  # list
                     "top5Performers": getTop5Performers(salesDf),  # list
                     "indicator": currentMonthYearVsLastMonthYearStats  # list
                     }
    return salesDataDict


def getData(collectionName):
    itemList = []
    itemDetails = collectionName.find()
    for item in itemDetails:
        # This does not give a very readable output
        itemList.append(item)
    # itemList = list(itemDetails)
    return itemList


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    dbname = get_database()

    # Retrieve a collection named "salesdata" from database
    sales_collection_name = dbname["sales_data"]

    # createTableUniqueIndex(collection_name)

    saleDatafile = 'zsdlogNew.csv'
    loadSalesData(sales_collection_name, saleDatafile)
    # # loadData(collection_name, r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\Sales_Report_Non
    # # SAP_22nd_Feb.csv')
    #
    ageingDataFile = 'Custageingallcompanycode.csv'
    customerAgeingList, customerAgeingReportDataList = ageing.customerAgeingFileReaderAndLoader(ageingDataFile)

    ageing_master_collection_name = dbname["ageing_master_data"]
    # ageing.customerAgeingDataLoader(ageing_master_collection_name, customerAgeingList)
    #
    ageing_collection_name = dbname["ageing_data"]
    # ageing.customerAgeingDataLoader(ageing_collection_name, customerAgeingReportDataList)

    # item_details = ageing_collection_name.find()
    # for item in item_details:
    #     # This does not give a very readable output
    #     print(item)

    # item_3 = {
    #     "item_name": "Bread",
    #     "quantity": 2,
    #     "ingredients": "all-purpose flour",
    #     "expiry_date": expiry
    # }
    # collection_name.insert_one(item_3)
    getSaleDataByYearMonthCompanyCode('request')
