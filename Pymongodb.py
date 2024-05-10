"""
Copyright (c) 2024 - Bizware International
"""
from pydantic import BaseModel
from pymongo import MongoClient
# from com.bizware.config import settings
import urllib.parse
import pandas as pd
import model.Sales as sales
import model.CustomerAgeing as ageing
import model.SalesTarget as salesTarget
import model.SecondarySales as secondarySales
# importing date class from datetime module
from datetime import date, datetime, timedelta
import calendar
# import motor
from bson import json_util
import json
from time import time
import getopt, sys
import logging

# creating the logger object
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

# creating the date object of today's date
todays_date = date.today()
current_year = todays_date.year
current_month = todays_date.month
# current_month_text = todays_date.strftime("%B") # March
current_month_text = todays_date.strftime("%b")
current_day = todays_date.day
year_entire = todays_date.strftime("%Y")

# # printing today's date
# print("Current date: ", todays_date)
#
# # fetching the current year, month and day of today
# print("Current year:", current_year)
# print("Current month:", current_month)
# print("Current month :", current_month_text)
# print("Current day:", current_day)


# Remove 1st argument from the
# list of command line arguments
argumentList = sys.argv[1:]

# Options
options = "sat:"

# Long options
long_options = ["sales", "ageing", "target"]

try:
    # Parsing argument
    arguments, values = getopt.getopt(argumentList, options, long_options)

    # checking each argument
    for currentArgument, currentValue in arguments:

        if currentArgument in ("-s", "--sales"):
            logging.info("Loading sales data")

        elif currentArgument in ("-a", "--ageing"):
            logging.info("Loading Ageing data")

        elif currentArgument in ("-t", "--target"):
            logging.info(("Loading target data (%s)") % (currentValue))

except getopt.error as err:
    # output error, and return with an error code
    logging.error(str(err))


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
    logging.info("Created Index for {} and index {}".format(collectionName, index))


def loadData(collection_name, datafile):
    data = pd.read_csv(datafile)
    datadict = data.to_dict(orient='records')
    # logging.info('datadict -', datadict)
    collection_name.insert_many(datadict)


# Invno.,BillingTyp,CompCode,Plant,PlantName,HeadOffice,InvDate,TaxInvNo.,ActDocNo.,Bill2Party,Bil2PrtNme,Bil2PrtPAN,PartyGSTNo,BillStatCd,BillStatNm,Bill2Distc,Ship2Party,ShpPrtyNm,Shp2PAN,
# Shp2GSTNo,Shp2StatCd,ShpStatNme,Shp2Dist,Shp2City,PayToParty,Pay2PrtNme,MatGroup1,MatGrp1Des,LineItemNo,ItemCode,ItemDes,HSNCode,BatchNo,UOM,BatchQnt,FreeSale,MfgDate,ExpDate,ExcRate,MRP,PTR,SalesRate,
# SalesAmnt,Discnt%,DiscntAmnt,NetAmt,PlateChrgs,Freight,TaxableAmt,IGSTRate,IGSTAmount,CGSTRate,CGSTAmount,SGSTRate,SGSTAmount,TAmtAftTax,TCSBase,TCSAmount,RoundOff,GrandTotal,PrtOrderNo,PatOrdDte,VehclNo,
# LRNo,TrnsprtNam,PymntTerm,NoofBoxes,GrsWightof,DistrnChnl,Division,DivDiscri,SalesOffic,SlsOffDes,SalesGroup,SalsGrpDes,MatGrp,MatGrpDesc,IRNNo,AcknowleNo,EwayBillNo,ShipmentNo,ShipCost,FreightAmn,SerEntrySh,
#     FreightPON,SalesEmply,HQCode,HospName


def sales_update_or_insert_record(collection, salesData):
    for data in salesData:
        # Check if data exists in MongoDB
        existing_data = collection.find_one(
            {'your_key': data['your_key'], 'key2': data['key2']})  # Replace 'your_key' with the key to check
        if existing_data:
            # Update the existing record
            collection.update_one({'your_key': data['your_key']}, {'$set': data})
            print("Record updated successfully.")
        else:
            # Insert a new record
            collection.insert_one(data)
            print("Record inserted successfully.")


def sales_ageing_update_or_insert_record(collection, data):
    # Check if data exists in MongoDB
    existing_data = collection.find_one(
        {'your_key': data['your_key'], 'key2': data['key2']})  # Replace 'your_key' with the key to check
    if existing_data:
        # Update the existing record
        collection.update_one({'your_key': data['your_key']}, {'$set': data})
        print("Record updated successfully.")
    else:
        # Insert a new record
        collection.insert_one(data)
        print("Record inserted successfully.")


def sales_target_update_or_insert_record(collection, data):
    # Check if data exists in MongoDB
    existing_data = collection.find_one(
        {'your_key': data['your_key'], 'key2': data['key2']})  # Replace 'your_key' with the key to check
    if existing_data:
        # Update the existing record
        collection.update_one({'your_key': data['your_key']}, {'$set': data})
        print("Record updated successfully.")
    else:
        # Insert a new record
        collection.insert_one(data)
        print("Record inserted successfully.")


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
    # logging.info('Inserting salesDataList to MongoDB -', salesDataList)
    f = open("salesData.txt", "w")
    f.close()
    collection_name.insert_many(salesDataList)
    logging.info('Inserted salesDataList Data to collection - {}'.format(collection_name))
    return salesDataList


# Load sales data
def loadSalesDataWithDelimiter(collection_name, datafile):
    data = pd.read_csv(datafile, encoding='cp1252', delimiter=';')
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
    # logging.info('Inserting salesDataList to MongoDB -', salesDataList)
    f = open("salesData.txt", "w")
    f.close()
    # collection_name.insert_many(salesDataList)
    logging.info('Inserted salesDataList Data to collection - {}'.format(collection_name))
    return salesDataList


def getCurrentMonthSales(salesDf):
    startTime_currentMonthSales = time()
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
    endTime_currentMonthSales = time()
    logging.info(
        "Time taken for getCurrentMonthSales() {}".format(endTime_currentMonthSales - startTime_currentMonthSales))
    return currentMonthSalesList


def getPreviousMonthSales(salesDf):
    startTime_PreviousMonthSales = time()
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
    endTime_PreviousMonthSales = time()
    logging.info(
        "Time taken for getPreviousMonthSales() {}".format(endTime_PreviousMonthSales - startTime_PreviousMonthSales))
    return previousMonthSalesList


def getCurrentYearSales(salesDf):
    startTime_CurrentYearSales = time()
    currentMonthSales = salesDf[salesDf["invoiceYear"].astype(int) == current_year].groupby(
        ['invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    currentYearSalesList = []
    for key, val in currentMonthSales.items():
        currentYearSalesDict = {"invoiceYear": key,
                                "grandTotal": val
                                }
        currentYearSalesList.append(currentYearSalesDict)
    endTime_CurrentYearSales = time()
    logging.info(
        "Time taken for getCurrentYearSales() {}".format(endTime_CurrentYearSales - startTime_CurrentYearSales))
    return currentYearSalesList


def getPreviousYearSales(salesDf):
    startTime_PreviousYearSales = time()
    previousYear = current_year - 1
    previousYearSales = salesDf[salesDf["invoiceYear"].astype(int) == previousYear].groupby(
        ['invoiceYear'])['grandTotal'].sum().sort_values().to_dict()
    previousYearSalesList = []
    for key, val in previousYearSales.items():
        previousYearSalesDict = {"invoiceYear": key,
                                 "grandTotal": val
                                 }
        previousYearSalesList.append(previousYearSalesDict)
    endTime_PreviousYearSales = time()
    logging.info(
        "Time taken for getPreviousYearSales() {}".format(endTime_PreviousYearSales - startTime_PreviousYearSales))
    return previousYearSalesList


def getCurrentMonthPreviousYearSales(salesDf):
    startTime_CurrentMonthPreviousYearSales = time()
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
    endTime_CurrentMonthPreviousYearSales = time()
    logging.info("Time taken for getCurrentMonthPreviousYearSales() {}".format(
        endTime_CurrentMonthPreviousYearSales - startTime_CurrentMonthPreviousYearSales))
    return currentMonthPreviousYearSalesList


def getIndicatorCurrentMonthYearVsLastMonthYear(salesDf):
    startTime_IndicatorCurrentMonthYearVsLastMonthYear = time()
    currentMonthSales = 0
    currentMonthSalesDict = {}
    currentMonthSalesData = getCurrentMonthSales(salesDf)
    if len(currentMonthSalesData) > 0:
        currentMonthSalesDict = currentMonthSalesData[0]
        currentMonthSales = currentMonthSalesDict.get('grandTotal')

    previousMonthSales = 0
    previousMonthSalesDict = {}
    previousMonthSalesData = getPreviousMonthSales(salesDf)
    if len(previousMonthSalesData) > 0:
        previousMonthSalesDict = previousMonthSalesData[0]
        previousMonthSales = previousMonthSalesDict.get('grandTotal')

    rateChangeInPercent = round((currentMonthSales - previousMonthSales) * 100 / previousMonthSales, 3)

    currentYearSales = 0
    currentYearSalesDict = {}
    currentYearSalesData = getCurrentYearSales(salesDf)
    if len(currentYearSalesData) > 0:
        currentYearSalesDict = currentYearSalesData[0]
        currentYearSales = currentYearSalesDict.get('grandTotal')

    previousYearSales = 0
    previousYearSalesDict = {}
    previousYearSalesData = getPreviousYearSales(salesDf)
    if len(previousYearSalesData) > 0:
        previousYearSalesDict = previousYearSalesData[0]
        previousYearSales = previousYearSalesDict.get('grandTotal')

    rateChangeInPercentYearly = round((currentYearSales - previousYearSales) * 100 / previousYearSales, 3)

    currentMonthPreviousYearSales = 0
    currentMonthPreviousYearSalesDict = {}
    currentMonthPreviousYearSalesData = getCurrentMonthPreviousYearSales(salesDf)
    if len(currentMonthPreviousYearSalesData) > 0:
        currentMonthPreviousYearSalesDict = currentMonthPreviousYearSalesData[0]
        currentMonthPreviousYearSales = currentMonthPreviousYearSalesDict.get('grandTotal')

    rateChangeInPercentCurrentMonthLastYear = round(
        (currentMonthSales - currentMonthPreviousYearSales) * 100 / currentMonthPreviousYearSales, 3)
    # rateOfChangeInPercent = round((((previousMonthSales - currentMonthSales) / previousMonthSales) * 100), 3)
    indicatorList = []
    indicator = {
        "indicatorName": "Monthly Sale",
        "period": f"{current_month_text} (this month) vs {previous_month_text} (last month)",
        "currentMonthSales": currentMonthSalesDict,
        "previousMonthSales": previousMonthSalesDict,
        "rateChange": rateChangeInPercent,
        "currentYearSales": currentYearSalesDict,
        "previousYearSales": previousYearSalesDict,
        "rateChangeYear": rateChangeInPercentYearly,
        "currentMonthPreviousYearSales": currentMonthPreviousYearSales,
        "rateChangeInPercentCurrentMonthLastYear": rateChangeInPercentCurrentMonthLastYear
    }
    indicatorList.append(indicator)
    endTime_IndicatorCurrentMonthYearVsLastMonthYear = time()
    logging.info("Time taken for getIndicatorCurrentMonthYearVsLastMonthYear() {}".format(
        endTime_IndicatorCurrentMonthYearVsLastMonthYear - startTime_IndicatorCurrentMonthYearVsLastMonthYear))
    return indicatorList


def getTotalSale(currentMonthYearVsLastMonthYear):
    startTime_getTotalSale = time()
    mom = ""
    if "currentMonthSales" in currentMonthYearVsLastMonthYear:
        if 'grandTotal' in currentMonthYearVsLastMonthYear["currentMonthSales"]:
            mom = currentMonthYearVsLastMonthYear["currentMonthSales"]['grandTotal']
    totalSales = {"MoM": mom,
                  "MoMPct": currentMonthYearVsLastMonthYear["rateChange"],
                  "YoY": currentMonthYearVsLastMonthYear["currentYearSales"]['grandTotal'],
                  "YoYPct": currentMonthYearVsLastMonthYear["rateChangeYear"]}
    endTime_getTotalSale = time()
    logging.info("Time taken for getTotalSale() {}".format(endTime_getTotalSale - startTime_getTotalSale))
    return totalSales


def getSalesTarget():
    startTime_getSalesTarget = time()
    salesTarget = {"MoM": "0",
                   "MoMPct": "0",
                   "YoY": "0",
                   "YoYPct": "0"}
    endTime_getSalesTarget = time()
    logging.info("Time taken for getSalesTarget() {}".format(endTime_getSalesTarget - startTime_getSalesTarget))
    return salesTarget


def getTargetAchievement():
    startTime_getTargetAchievement = time()
    targetAchievement = {"MoM": "0",
                         "MoMPct": "0",
                         "YoY": "0",
                         "YoYPct": "0"}
    endTime_getTargetAchievement = time()
    logging.info("Time taken for getTargetAchievement() {}".format(
        endTime_getTargetAchievement - startTime_getTargetAchievement))
    return targetAchievement


def getSalesLastYear(currentMonthYearVsLastMonthYear):
    startTime_getSalesLastYear = time()
    salesLastYear = {"MoM": currentMonthYearVsLastMonthYear["currentMonthPreviousYearSales"],
                     "MoMPct": currentMonthYearVsLastMonthYear["rateChangeInPercentCurrentMonthLastYear"],
                     "YoY": currentMonthYearVsLastMonthYear["previousYearSales"]['grandTotal'],
                     "YoYPct": currentMonthYearVsLastMonthYear["rateChangeYear"]}
    endTime_getSalesLastYear = time()
    logging.info("Time taken for getSalesLastYear() {}".format(endTime_getSalesLastYear - startTime_getSalesLastYear))
    return salesLastYear


def getAccountReceivables(ageingDf):
    startTime_getAccountReceivables = time()

    # ageingDf['Amount Receivables'] = ageingDf['Due Amount'] + ageingDf['Overdue Amount']

    # ageingDf['Overdue Receivables(cr)'] = ageingDf['Overdue Amount']
    # ageingOverdueReceivablesPct = (ageingDf['Overdue Receivables'].sum() / ageingDf['Account Receivables'].sum()) * 100

    # accountReceivables = {"accountReceivablesVal": ageingDf['Amount Receivables'].sum(),
    accountReceivables = {"accountReceivablesVal": "0",
                          "accountReceivablesPct": "0"}
    endTime_getAccountReceivables = time()
    logging.info("Time taken for getAccountReceivables() {}".format(
        endTime_getAccountReceivables - startTime_getAccountReceivables))
    return accountReceivables


def getOverdueReceivables(ageingDf):
    startTime_getOverdueReceivables = time()

    # amountReceivables = ageingDf['Due Amount'] + ageingDf['Overdue Amount']

    # ageingDf['Overdue Receivables(cr)'] = ageingDf['Overdue Amount']

    # ageingOverdueReceivablesPct = (ageingDf['Overdue Amount'].sum() / amountReceivables.sum()) * 100

    # overdueReceivables = {"overdueReceivablesVal": ageingDf['Overdue Amount'].sum(),
    #                       "overdueReceivablesPct": ageingOverdueReceivablesPct}
    # 
    overdueReceivables = {"overdueReceivablesVal": "0",
                          "overdueReceivablesPct": "0"}
    endTime_getOverdueReceivables = time()
    logging.info("Time taken for getOverdueReceivables() {}".format(
        endTime_getOverdueReceivables - startTime_getOverdueReceivables))
    return overdueReceivables


def getTopCustomers(salesDf):
    startTime_getTopCustomers = time()
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
    # logging.info("Get Grand Total sum of the grouped data by billToParty, billToPartName : ", topCustomersList)
    endTime_getTopCustomers = time()
    logging.info("Time taken for getTopCustomers() {}".format(endTime_getTopCustomers - startTime_getTopCustomers))
    return topCustomersList


def getTopProducts(salesDf):
    startTime_getTopProducts = time()
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
    logging.info("Get Grand Total sum of the grouped data by itemCode, itemDescription : " + topProductsList.__str__())
    endTime_getTopProducts = time()
    logging.info("Time taken for getTopProducts() {}".format(endTime_getTopProducts - startTime_getTopProducts))
    return topProductsList


def getTopDivisions(salesDf):
    startTime_getTopDivisions = time()
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
    logging.info(
        "Get Grand Total sum of the grouped data by division, divisionDescription : {}".format(topDivisionsList))
    endTime_getTopDivisions = time()
    logging.info("Time taken for getTopDivisions() {}".format(endTime_getTopDivisions - startTime_getTopDivisions))
    return topDivisionsList


def getTop5Performers(salesDf):
    startTime_getTop5Performers = time()
    # top5PerformersGroupBySalesEmpolyeeGrandTotalSum = salesDf.groupby('salesEmpolyee')['grandTotal'].sum()
    top5PerformersGroupBySalesEmployeeGrandTotalSum = salesDf.groupby('salesEmpolyee')['grandTotal'].sum().sort_values(
        ascending=False).head(10).to_dict()
    logging.info("Get Grand Total sum of the grouped data by salesEmpolyee : ",
                 top5PerformersGroupBySalesEmployeeGrandTotalSum)
    endTime_getTop5Performers = time()
    logging.info(
        "Time taken for getTop5Performers() {}".format(endTime_getTop5Performers - startTime_getTop5Performers))
    return top5PerformersGroupBySalesEmployeeGrandTotalSum


def getSalesDataCurrentAndPreviousYear(salesDf):
    previous_year = current_year - 1
    salesDfNew = salesDf[(salesDf.invoiceYear.isin([previous_year, current_year]))]
    # return salesDfNew
    return salesDfNew.drop(['_id'], axis=1)


def getSalesZoneDataBySalesTarget(dbname, salesTargetDf):
    salesDf = getSaleDataAndTransform(dbname)
    # salesTargetDf
    # salesDf = salesDf.loc[(salesDf['invoiceMonth'] == calendar.month_abbr[salesTargetDf['Month'][0]]) & (salesDf['invoiceYear'] == str(salesTargetDf['Year'][0])) & (salesDf['salesEmpolyee']!="")]
    salesWithTargetDf = pd.merge(salesDf, salesTargetDf, left_on=['invoiceYear', 'invoiceMonth', 'salesEmpolyee'],
                                 right_on=['Year', 'targetMonth', 'EmployeeCode'])
    salesWithTargetByYearMonthZone = salesWithTargetDf.groupby(['Year', 'Month', 'ZONE'])['grandTotal'].sum().to_dict()
    salesWithTargetByYearMonthZoneList = []
    for key, val in salesWithTargetByYearMonthZone.items():
        salesWithTargetByYearMonthZoneDict = {"Year": key[0],
                                              "Month": key[1],
                                              "Zone": key[2],
                                              "grandTotal": val,
                                              }
        salesWithTargetByYearMonthZoneList.append(salesWithTargetByYearMonthZoneDict)
    return salesWithTargetByYearMonthZoneList


def getSalesTargetDataByZone():
    # Get the database
    dbname = get_database()
    # Retrieve a collection named "sales_target_data" from database
    collectionName = dbname["sales_target_data"]
    salesTargetData = collectionName.find()
    salesTargetDataDf = pd.DataFrame(salesTargetData)
    salesTargetDf = salesTargetDataDf.drop(['_id'], axis=1)
    salesTargetDf = salesTargetDf[['Month', 'Year', 'ZONE', 'EmployeeCode', 'EmployeeName',
                                   'EmployeeDesignation', 'HODEmpCode', 'HODName', 'Country',
                                   'RegionState', 'HQCode', 'City', 'SAPRegion', 'Division', 'MonthlySalesTarget']]
    salesTargetDf['targetMonth'] = salesTargetDf['Month'].apply({lambda x: calendar.month_abbr[int(x)]})
    # Get Sales Target By Year
    salesTargetByYear = salesTargetDf.groupby(['Year'])['MonthlySalesTarget'].sum().to_dict()
    salesTargetByYearList = []
    for key, val in salesTargetByYear.items():
        salesTargetByYearDict = {"Year": key,
                                 "YearlySalesTarget": val
                                 }
        salesTargetByYearList.append(salesTargetByYearDict)
    # Get Sales Target By Year and Month
    salesTargetByYearMonth = salesTargetDf.groupby(['Year', 'Month'])['MonthlySalesTarget'].sum().to_dict()
    salesTargetByYearMonthList = []
    for key, val in salesTargetByYearMonth.items():
        salesTargetByYearMonthDict = {"Year": key[0],
                                      "Month": key[1],
                                      "MonthlySalesTarget": val
                                      }
        salesTargetByYearMonthList.append(salesTargetByYearMonthDict)
    salesTargetByYearMonthZone = salesTargetDf.groupby(['Year', 'Month', 'ZONE'])['MonthlySalesTarget'].sum().to_dict()
    salesTargetByZoneList = []
    for key, val in salesTargetByYearMonthZone.items():
        salesTargetByZoneDict = {"Year": key[0],
                                 "Month": key[1],
                                 "Zone": key[2],
                                 "MonthlySalesTarget": val,
                                 }
        salesTargetByZoneList.append(salesTargetByZoneDict)
    salesDataByZoneList = getSalesZoneDataBySalesTarget(dbname, salesTargetDf)
    salesTargetDataResponse = {
        'salesTarget': salesTargetDf.to_dict(orient="records"),
        'salesTargetByYear': salesTargetByYearList,
        'salesTargetByYearMonth': salesTargetByYearMonthList,
        'salesTargetByYearMonthZone': salesTargetByZoneList,
        'salesDataByYearMonthZone': salesDataByZoneList
    }
    return salesTargetDataResponse


def getSaleDataAndTransform(dbname):
    # Retrieve a collection named "sales_data" from database
    collection_name = dbname["sales_data"]
    itemDetails = collection_name.find()
    # itemDetails = collection_name.find({"year": year, "month": month, "companyCode": companyCode})

    # itemList = list(itemDetails)
    salesDf = pd.DataFrame(itemDetails)
    salesDf['grandTotal'] = salesDf['grandTotal'].str.replace(',', '').astype('float64')
    # logging.info("salesDf['invoiceDate'] -"+ salesDf['invoiceDate'])
    # salesDf['invoiceDate'] = salesDf['invoiceDate'].str.replace("/", "-")
    salesDf['invoiceMonth'] = salesDf['invoiceDate'].str.split("-", expand=True)[1].apply({
        lambda x: calendar.month_abbr[int(x)]
    })
    salesDf['invoiceYear'] = salesDf['invoiceDate'].str.split("-", expand=True)[2].astype('int64')

    return salesDf


def getSaleDataByYearMonthCompanyCode(request):
    startTime_getSaleDataByYearMonthCompanyCode = time()
    print("request - ", request, "type(request) - ", type(request))
    # {"year": "2024", "month": "march", "companyCode": "c2002"}
    try:
        year = request.year
        month = request.month
        companyCode = request.companyCode
    except:
        year = request['year']
        month = request['month']
        companyCode = request['companyCode']

    # get database obj
    dbname = get_database()

    salesDf = getSaleDataAndTransform(dbname)

    salesDfCurrentAndPreviousYear = getSalesDataCurrentAndPreviousYear(salesDf)
    startTime_getIndicatorCurrentMonthYearVsLastMonthYear = time()
    currentMonthYearVsLastMonthYearStats = getIndicatorCurrentMonthYearVsLastMonthYear(salesDf)
    endTime_getIndicatorCurrentMonthYearVsLastMonthYear = time()
    logging.info(
        "Time taken for getIndicatorCurrentMonthYearVsLastMonthYear() inside getSaleDataByYearMonthCompanyCode() {}".format(
            endTime_getIndicatorCurrentMonthYearVsLastMonthYear - startTime_getIndicatorCurrentMonthYearVsLastMonthYear))
    # Ageing data
    ageingCollectionName = dbname["ageing_data"]
    ageingItemDetails = ageingCollectionName.find()
    # ageingItemList = list(ageingItemDetails)
    ageingDf = pd.DataFrame(ageingItemDetails)
    # balanceDue = getAgeingStats(ageingDf)

    salesDataDict = {
        "salesData": salesDfCurrentAndPreviousYear.to_dict('records'),
        # "salesData": json.loads(json_util.dumps(salesDfCurrentAndPreviousYear)),
        "totalSales": getTotalSale(currentMonthYearVsLastMonthYearStats[0]),
        "salesTarget": getSalesTarget(),
        "targetAchievement": getTargetAchievement(),
        "salesLastYear": getSalesLastYear(currentMonthYearVsLastMonthYearStats[0]),
        "accountReceivables": getAccountReceivables(ageingDf),
        "overdueReceivables": getOverdueReceivables(ageingDf),
        "topCustomers": getTopCustomers(salesDfCurrentAndPreviousYear),  # list
        "topProducts": getTopProducts(salesDfCurrentAndPreviousYear),  # list
        "topDivisions": getTopDivisions(salesDfCurrentAndPreviousYear),  # list
        "top5Performers": getTop5Performers(salesDfCurrentAndPreviousYear),  # list
        "indicator": currentMonthYearVsLastMonthYearStats  # list
    }
    endTime_getSaleDataByYearMonthCompanyCode = time()
    logging.info("Time taken for getSaleDataByYearMonthCompanyCode() {}".format(
        endTime_getSaleDataByYearMonthCompanyCode - startTime_getSaleDataByYearMonthCompanyCode))
    return salesDataDict


def getData(collectionName):
    itemList = []
    itemDetails = collectionName.find()
    for item in itemDetails:
        # This does not give a very readable output
        itemList.append(item)
    # itemList = list(itemDetails)
    return itemList


def getAgeingData(collectionName):
    itemDetails = collectionName.find()
    # itemList = list(itemDetails)
    ageingDf = pd.DataFrame(itemDetails)
    ageingDf = ageingDf.drop(['_id'], axis=1)
    ageingDf['dueMonth'] = ageingDf['dueDate'].str.split("-", expand=True)[1].apply({
        lambda x: calendar.month_abbr[int(x)] if x is not None else x
    })
    ageingDf['dueYear'] = ageingDf['dueDate'].str.split("-", expand=True)[2].fillna(0).astype('int64')
    ageingDf['Amount Receivables'] = ageingDf['Due Amount'] + ageingDf['Overdue Amount']

    ageingDf['Overdue Receivables(cr)'] = ageingDf['Overdue Amount']

    ageingOverdueReceivablesPct = (ageingDf['Overdue Receivables'].sum() / ageingDf['Account Receivables'].sum()) * 100

    ageingDataDict = {
        "ageingData": ageingDf.to_dict('records'),
        "amountReceivables": ageingDf['Amount Receivables'].sum(),
        "overdueReceivables": ageingDf['Overdue Receivables(cr)'].sum(),
        "overdueReceivablesPct": ageingOverdueReceivablesPct
    }
    return ageingDataDict


def salesTargetUpload(salesTargetDataFile):
    # salesTargetDataFile = 'SalesEmployeeTargetData.csv'
    salesTargetDataList = salesTarget.salesTargetFileReaderAndLoader(salesTargetDataFile)
    sales_target_collection_name = dbname["sales_target_data"]
    salesTarget.salesTargetDataLoader(sales_target_collection_name, salesTargetDataList)


def salesTargetUploadedData(salesTargetDataFile):
    # salesTargetDataFile = 'SalesEmployeeTargetData.csv'
    dbname = get_database()
    sales_target_collection_name = dbname["sales_target_data"]
    salesTargetDataList = salesTarget.salesTargetReaderAndLoader(salesTargetDataFile)
    salesTarget.salesTargetDataLoader(sales_target_collection_name, salesTargetDataList)


def SecondarySalesUploadedData(secondarySalesDataFile):
    # salesTargetDataFile = 'SalesEmployeeTargetData.csv'
    dbname = get_database()
    secondary_sales_collection_name = dbname["secondary_sales_data"]
    secondarySalesDataList = secondarySales.secondarySalesReaderAndLoader(secondarySalesDataFile)
    secondarySales.salesTargetDataLoader(secondary_sales_collection_name, secondarySalesDataList)


def getSecondarySalesDataByZone():
    # Get the database
    dbname = get_database()
    # Retrieve a collection named "sales_target_data" from database
    collectionName = dbname["secondary_sales_data"]
    secondarySalesData = collectionName.find()
    secondarySalesDataDf = pd.DataFrame(secondarySalesData)
    secondarySalesDf = secondarySalesDataDf.drop(['_id'], axis=1)
    secondarySalesDf['secondarySalesMonth'] = secondarySalesDf['Month'].apply({lambda x: calendar.month_abbr[int(x)]})

    secondarySalesDataResponse = {
        'secondarySales': secondarySalesDf.to_dict(orient="records")
    }
    return secondarySalesDataResponse


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    dbname = get_database()

    # Retrieve a collection named "salesdata" from database
    sales_collection_name = dbname["sales_data"]

    # createTableUniqueIndex(collection_name)

    saleDatafile = 'Aishwarya_Sales_Report_03-05-24till09-05-2024.csv'
    loadSalesData(sales_collection_name, saleDatafile)
    # # loadData(collection_name, r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\Sales_Report_Non
    # # SAP_22nd_Feb.csv')
    #
    # ageingDataFile = 'Custageingallcompanycode.csv'
    # customerAgeingList, customerAgeingReportDataList = ageing.customerAgeingFileReaderAndLoader(ageingDataFile)

    # ageing_master_collection_name = dbname["ageing_master_data"]
    # ageing.customerAgeingDataLoader(ageing_master_collection_name, customerAgeingList)
    #
    # ageing_collection_name = dbname["ageing_data"]
    # ageing.customerAgeingDataLoader(ageing_collection_name, customerAgeingReportDataList)

    # Sales Target
    # salesTargetDataFile = 'SalesEmployeeTargetData.csv'
    # salesTargetDataList = salesTarget.salesTargetFileReaderAndLoader(salesTargetDataFile)
    # sales_target_collection_name = dbname["sales_target_data"]
    # salesTarget.salesTargetDataLoader(sales_target_collection_name, salesTargetDataList)

    # item_details = ageing_collection_name.find()
    # for item in item_details:
    #     # This does not give a very readable output
    #     logging.info(item)

    # item_3 = {
    #     "item_name": "Bread",
    #     "quantity": 2,
    #     "ingredients": "all-purpose flour",
    #     "expiry_date": expiry
    # }
    # collection_name.insert_one(item_3)
    # getSaleDataByYearMonthCompanyCode({"year": "2024", "month": "march", "companyCode": "c2002"})
    # getSalesTargetDataByZone()
