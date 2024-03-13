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


# Load sales data
def loadSalesData(collection_name, datafile):
    data = pd.read_csv(datafile, encoding='cp1252')
    salesColumns = data[
        ['Company Code', 'Invoice No.', 'Invoice Date', 'Billing Type', 'Plant', 'Plant Name', 'Division',
         'Division Description', 'Bill To State Code', 'Bill to State Name', 'Ship To District', 'Ship To City',
         'Item Code', 'Item Description', 'HSN Code', 'Batch Number', 'Sale Unit (UOM)', 'Batch Quantity', 'Net Amount',
         'Total Amt After Tax', 'Grand Total', 'Distribution Channel', 'Sales Empolyee', 'HQ Code']]
    salesColumns['Company Name'] = salesColumns['Company Code'].map(sales.companyDict)
    salesColumns = salesColumns.fillna('')
    salesColumns = salesColumns.to_dict(orient='records')
    salesDataList = []
    for sale in salesColumns:
        salesObj = sales.setSalesData(sale)
        salesDataList.append(salesObj)
    print('Inserting salesDataList to MongoDB -', salesDataList)
    collection_name.insert_many(salesDataList)
    print('Inserted salesDataList Data')
    return salesDataList


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

    saleDatafile = r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\ZSD_LOG_08-03-2024.csv'
    loadSalesData(sales_collection_name, saleDatafile)
    # loadData(collection_name, r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\Sales_Report_Non SAP_22nd_Feb.csv')

    ageingDataFile = r'C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\data\CustomerAgening11.03.2024.csv'
    customerAgeingList, customerAgeingReportDataList = ageing.customerAgeingFileReaderAndLoader(ageingDataFile)

    ageing_master_collection_name = dbname["ageing_master_data"]
    # ageing.customerAgeingDataLoader(ageing_master_collection_name, customerAgeingList)

    ageing_collection_name = dbname["ageing_data"]
    # ageing.customerAgeingDataLoader(ageing_collection_name, customerAgeingReportDataList)
    item_details = ageing_collection_name.find()
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
