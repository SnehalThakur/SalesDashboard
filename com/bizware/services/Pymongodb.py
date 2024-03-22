"""
Copyright (c) 2024 - Bizware International
"""

from pymongo import MongoClient
# from com.bizware.config import settings
import urllib.parse
import pandas as pd
import com.bizware.model.Sales as sales
import com.bizware.model.CustomerAgeing as ageing

# Provide the mongodb atlas url to connect python to mongodb using pymongo
# MYSQL_DATABASE_URL = f"mysql://{settings.database_username}:{settings.database_password}@{settings.database_port}/{settings.database_name}"

# CONNECTION_STRING = f"mongodb+srv://{settings.database_username}:{settings.database_password}@{settings.database_port}/{settings.database_name}"
# CONNECTION_STRING = "mongodb+srv://bizware:<password>@bizwarecluster.rraehrb.mongodb.net/?retryWrites=true&w=majority&appName=BizwareCluster"
password = urllib.parse.quote_plus('bizware@1234')
CONNECTION_STRING = f"mongodb+srv://bizware:{password}@bizwarecluster.rraehrb.mongodb.net"

# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
client = MongoClient(CONNECTION_STRING)


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
    print('datadict -', datadict)
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
         'DivDiscri', 'BillStatCd', 'BillStatNm', 'Bill2Distc', 'Ship2Party', 'ShpPrtyNm',
         'ItemCode', 'ItemDes', 'HSNCode', 'BatchNo', 'UOM', 'BatchQnt', 'NetAmt',
         'TAmtAftTax', 'GrandTotal', 'DistrnChnl', 'SalesEmply', 'HQCode']]

    salesColumns['CompanyName'] = salesColumns['CompCode'].map(sales.companyDict)
    salesColumns[
        ['CompCode', 'Division', 'Ship2Party', 'Plant', 'BillStatCd', 'ItemCode', 'HSNCode', 'DistrnChnl']] = \
        salesColumns[['CompCode', 'Division', 'Ship2Party', 'Plant', 'BillStatCd', 'ItemCode', 'HSNCode',
                      'DistrnChnl']].fillna(0)
    salesColumns = salesColumns.fillna('')
    salesColumns = salesColumns.to_dict(orient='records')
    salesDataList = []
    for sale in salesColumns:
        salesObj = sales.setSalesData(sale)
        salesDataList.append(salesObj)
    print('Inserting salesDataList to MongoDB -', salesDataList)
    f = open("salesData.txt", "w")
    f.write(str(salesDataList))
    f.close()
    # collection_name.insert_many(salesDataList)
    print('Inserted salesDataList Data to collection - {}'.format(collection_name))
    return salesDataList


def getTotalSale():
    totalSales = {"MoM": "50.99",
                  "MoMPct": "43",
                  "YoY": "99.96",
                  "YoYPct": "43"}
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


def getSalesLastYear():
    salesLastYear = {"MoM": "50.99",
                     "MoMPct": "43",
                     "YoY": "99.96",
                     "YoYPct": "43"}
    return salesLastYear


def getAccountReceivables():
    accountReceivables = {"accountReceivablesVal": "90.99",
                          "accountReceivablesPct": "43"}
    return accountReceivables


def getOverdueReceivables():
    overdueReceivables = {"overdueReceivablesVal": "90.99",
                          "overdueReceivablesPct": "43"}
    return overdueReceivables


def getTopProducts():
    topProducts = []
    return topProducts


def getTopDivisions():
    topDivisions = []
    return topDivisions


def getTop5Performers():
    top5Performers = []
    return top5Performers


def getSaleDataByYearMonthCompanyCode(request):
    itemList = []
    # get database obj
    dbname = get_database()

    # Retrieve a collection named "sales_data" from database
    collection_name = dbname["sales_data"]
    itemDetails = collection_name.find()
    for item in itemDetails:
        # This does not give a very readable output
        itemList.append(item)

    salesDataDict = {"salesData": itemList,
                     "totalSales": getTotalSale(),
                     "salesTarget": getSalesTarget(),
                     "targetAchievement": getTargetAchievement(),
                     "salesLastYear": getSalesLastYear(),
                     "accountReceivables": getAccountReceivables(),
                     "overdueReceivables": getOverdueReceivables(),
                     "topProducts": getTopProducts(),  # list
                     "topDivisions": getTopDivisions(),  # list
                     "top5Performers": getTop5Performers()  # list
                     }
    return salesDataDict


def getData(collectionName):
    itemList = []
    itemDetails = collectionName.find()
    for item in itemDetails:
        # This does not give a very readable output
        itemList.append(item)
    return itemList


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    dbname = get_database()

    # Retrieve a collection named "salesdata" from database
    sales_collection_name = dbname["sales_data"]

    # createTableUniqueIndex(collection_name)

    saleDatafile = r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\ZSDLOGNNN.csv'
    loadSalesData(sales_collection_name, saleDatafile)
    # loadData(collection_name, r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\Sales_Report_Non
    # SAP_22nd_Feb.csv')

    ageingDataFile = r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\ZSDFICUSTAGENN.csv'
    customerAgeingList, customerAgeingReportDataList = ageing.customerAgeingFileReaderAndLoader(ageingDataFile)


    # ageing_master_collection_name = dbname["ageing_master_data"]
    # ageing.customerAgeingDataLoader(ageing_master_collection_name, customerAgeingList)
    #
    # ageing_collection_name = dbname["ageing_data"]
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
