#from etl.etl import extract, load, transfrom
from Utils.postgres_procedures import call_postgres_procedure
from etl import  extract, load, transfrom


def bronze_layer():
    tables_config = [
        {
            "source": "Sales.SalesTerritory",
            "transform": transfrom.transform_SalesTerritory_Data,
            "target": "bronze_l.sales_salesterritory"
        },
        {
            "source": "Production.Product",
            "transform": transfrom.transform_Product_Data,
            "target": "bronze_l.production_product"
        },
        {
            "source": "Production.ProductSubcategory",
            "transform": transfrom.transform_ProductSubcategory_Data,
            "target": "bronze_l.production_productsubcategory"
        },
        {
            "source": "Production.ProductCategory",
            "transform": transfrom.transform_ProductCategory_Data,
            "target": "bronze_l.production_productcategory"
        },
        {
            "source": "Sales.Customer",
            "transform": transfrom.transform_SalesCustomer_Data,
            "target": "bronze_l.sales_customer"
        },
        {
            "source": "Person.Person",
            "transform": transfrom.transform_Person_Data,
            "target": "bronze_l.person_person"
        },
        {
            "source": "Person.EmailAddress",
            "transform": transfrom.transform_PersonEmail_Data,
            "target": "bronze_l.person_emailaddress"
        },
        {
            "source": "Person.BusinessEntityAddress",
            "transform": transfrom.transform_PersonBusinessEntityAddress_Data,
            "target": "bronze_l.person_business_entityaddress"
        },
        {
            "source": "Person.Address",
            "transform": transfrom.transform_PersonAddress_Data,
            "target": "bronze_l.person_address"
        },
        {
            "source": "Person.StateProvince",
            "transform": transfrom.transform_StateProvince_Data,
            "target": "bronze_l.person_stateprovince"
        },
        {
            "source": "Person.CountryRegion",
            "transform": transfrom.transform_CountryRegion_Data,
            "target": "bronze_l.person_country_region"
        },
        {
            "source": "Sales.SalesOrderHeader",
            "transform": transfrom.transform_SalesOrderHeader_Data,
            "target": "bronze_l.sales_salesorderheader"
        },
        {
            "source": "Sales.SalesOrderDetail",
            "transform": transfrom.transform_SalesOrderDetail_Data,
            "target": "bronze_l.sales_salesorderdetail"
        }
    ]

    for table in tables_config:
        df = extract.input_table(table["source"])
        df = table["transform"](df)
        load.writing_data_to_postgresql(df, table["target"])





def silver_layer():
    procedures = [
        "silver_l.silver_layer_data_mart_insert_data"
    ]

    for proc in procedures:
        print(f"▶ Silver  Layer Procedure Running {proc}")
        call_postgres_procedure(proc)
        print(f"✔ Silver  Layer Procedure Finished {proc}")


def gold_layer():
    procedures = [
        "gold_l.gold_layer_view"
    ]

    for proc in procedures:
        print(f"▶ Glod Layer Procedure Running {proc}")
        call_postgres_procedure(proc)
        print(f"✔ Glod Layer Procedure Finished {proc}")
