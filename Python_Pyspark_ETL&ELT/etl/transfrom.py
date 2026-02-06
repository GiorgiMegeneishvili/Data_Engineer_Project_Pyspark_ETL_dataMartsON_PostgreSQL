# transform.py
from pyspark.sql.functions import col, current_timestamp

def transform_SalesTerritory_Data(df):
    df_to_insert = df.select(
        col("TerritoryID").alias("territory_id"),
        col("Name").alias("name"),
        col("CountryRegionCode").alias("country_region_code"),
        col("Group").alias("group"),
        col("SalesYTD").alias("sales_ytd"),
        col("SalesLastYear").alias("sales_last_year"),
        col("CostYTD").alias("cost_ytd"),
        col("CostLastYear").alias("cost_last_year"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )
    return df_to_insert


def transform_Product_Data(df):
    df_to_insert = df.select(
        col("ProductID").alias("product_id"),
        col("Name").alias("name"),
        col("ProductNumber").alias("product_number"),
        col("MakeFlag").cast("boolean").alias("make_flag"),
        col("FinishedGoodsFlag").cast("boolean").alias("finished_goods_flag"),
        col("Color").alias("color"),
        col("SafetyStockLevel").alias("safety_stock_level"),
        col("ReorderPoint").alias("reorder_point"),
        col("StandardCost").alias("standard_cost"),
        col("ListPrice").alias("list_price"),
        col("Size").alias("size"),
        col("SizeUnitMeasureCode").alias("size_unit_measure_code"),
        col("WeightUnitMeasureCode").alias("weight_unit_measure_code"),
        col("Weight").alias("weight"),
        col("DaysToManufacture").alias("days_to_manufacture"),
        col("ProductLine").alias("product_line"),
        col("Class").alias("class"),
        col("Style").alias("style"),
        col("ProductSubcategoryID").alias("product_subcategory_id"),
        col("SellStartDate").cast("date").alias("sell_start_date"),
        col("SellEndDate").cast("date").alias("sell_end_date"),
        col("DiscontinuedDate").cast("date").alias("discontinued_date"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )

    return df_to_insert


def transform_ProductSubcategory_Data(df):
    df_to_insert = df.select(
        col("ProductSubcategoryID").alias("product_subcategory_id"),
        col("ProductCategoryID").alias("product_category_id"),
        col("Name").alias("name"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )

    return df_to_insert

def transform_ProductCategory_Data(df):
    df_to_insert = df.select(
        col("ProductCategoryID").alias("product_category_id"),
        col("Name").alias("name"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )

    return df_to_insert

def transform_SalesCustomer_Data(df):
    df_to_insert = df.select(
        col("CustomerID").alias("customer_id"),
        col("PersonID").alias("person_id"),
        col("StoreID").alias("store_id"),
        col("TerritoryID").alias("territory_id"),
        col("ModifiedDate").cast("date").alias("modified_date")
        # inserted_date DB DEFAULT-ის საშუალებით ავსებს
    )

    return df_to_insert

def transform_Person_Data(df):
    df_to_insert = df.select(
        col("BusinessEntityID").alias("business_entity_id"),
        col("PersonType").alias("person_type"),
        col("NameStyle").cast("boolean").alias("name_style"),
        col("Title").alias("title"),
        col("FirstName").alias("first_name"),
        col("MiddleName").alias("middle_name"),
        col("LastName").alias("last_name"),
        col("Suffix").alias("suffix"),
        col("EmailPromotion").alias("email_promotion"),
        col("ModifiedDate").cast("date").alias("modified_date")
        # inserted_date DB DEFAULT-ის საშუალებით ავსებს
    )

    return df_to_insert


def transform_PersonEmail_Data(df):
    df_to_insert = df.select(
        col("BusinessEntityID").alias("business_entity_id"),
        col("EmailAddressID").alias("email_address_id"),
        col("EmailAddress").alias("email_address"),
        col("ModifiedDate").cast("date").alias("modified_date")
        # inserted_date DB DEFAULT-ის საშუალებით ავსებს
    )

    return df_to_insert

def transform_PersonBusinessEntityAddress_Data(df):
    df_to_insert = df.select(
        col("BusinessEntityID").alias("business_entity_id"),
        col("AddressID").alias("address_id"),
        col("AddressTypeID").alias("address_type_id"),
        col("ModifiedDate").cast("date").alias("modified_date")
        # inserted_date DB DEFAULT-ის საშუალებით ავსებს
    )

    return df_to_insert


def transform_PersonAddress_Data(df):
    df_to_insert = df.select(
        col("AddressID").alias("address_id"),
        col("AddressLine1").alias("address_line1"),
        col("AddressLine2").alias("address_line2"),
        col("City").alias("city"),
        col("StateProvinceID").alias("state_province_id"),
        col("PostalCode").alias("postal_code"),
        col("SpatialLocation").alias("spatial_location"),
        col("ModifiedDate").cast("date").alias("modified_date")
        # inserted_date DB DEFAULT-ის საშუალებით ავსებს
    )

    return df_to_insert

def transform_StateProvince_Data(df):
    df_to_insert = df.select(
        col("StateProvinceID").alias("state_province_id"),
        col("StateProvinceCode").alias("state_province_code"),
        col("CountryRegionCode").alias("country_region_code"),
        col("IsOnlyStateProvinceFlag").cast("boolean").alias("is_only_state_province_flag"),
        col("Name").alias("name"),
        col("TerritoryID").alias("territory_id"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )

    return df_to_insert

def transform_CountryRegion_Data(df):
    df_to_insert = df.select(
        col("CountryRegionCode").alias("country_region_code"),
        col("Name").alias("name"),
        col("ModifiedDate").cast("date").alias("modified_date")
        # inserted_date DB DEFAULT-ის საშუალებით ავსებს, საჭირო არ არის Spark-ში
    )

    return df_to_insert

def transform_SalesOrderHeader_Data(df):
    df_to_insert = df.select(
        col("SalesOrderID").alias("sales_order_id"),
        col("RevisionNumber").alias("revision_number"),
        col("OrderDate").cast("date").alias("order_date"),
        col("DueDate").cast("date").alias("due_date"),
        col("ShipDate").cast("date").alias("ship_date"),
        col("Status").alias("status"),
        col("OnlineOrderFlag").cast("boolean").alias("online_order_flag"),
        col("SalesOrderNumber").alias("sales_order_number"),
        col("PurchaseOrderNumber").alias("purchase_order_number"),
        col("AccountNumber").alias("account_number"),
        col("CustomerID").alias("customer_id"),
        col("SalesPersonID").alias("salesperson_id"),
        col("TerritoryID").alias("territory_id"),
        col("BillToAddressID").alias("bill_to_address_id"),
        col("ShipToAddressID").alias("ship_to_address_id"),
        col("ShipMethodID").alias("ship_method_id"),
        col("CreditCardID").alias("credit_card_id"),
        col("CreditCardApprovalCode").alias("credit_card_approval_code"),
        col("SubTotal").alias("sub_total"),
        col("TaxAmt").alias("tax_amt"),
        col("Freight").alias("freight"),
        col("TotalDue").alias("total_due"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )

    return df_to_insert


def transform_SalesOrderDetail_Data(df):
    df_to_insert = df.select(
        col("SalesOrderID").alias("sales_order_id"),
        col("SalesOrderDetailID").alias("sales_order_detail_id"),
        col("CarrierTrackingNumber").alias("carrier_tracking_number"),
        col("OrderQty").alias("order_qty"),
        col("ProductID").alias("product_id"),
        col("SpecialOfferID").alias("special_offer_id"),
        col("UnitPrice").alias("unit_price"),
        col("UnitPriceDiscount").alias("unit_price_discount"),
        col("LineTotal").alias("line_total"),
        col("ModifiedDate").cast("date").alias("modified_date")
    )

    return df_to_insert
