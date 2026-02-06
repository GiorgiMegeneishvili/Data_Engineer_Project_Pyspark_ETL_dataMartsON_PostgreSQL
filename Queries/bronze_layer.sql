
---- Creation Of Sivler Tables in stored Procedure

CREATE OR REPLACE PROCEDURE bronze_l.bronze_layer_Table_creation()
LANGUAGE plpgsql
AS $$
BEGIN
		
		DROP TABLE  IF EXISTS bronze_l.sales_salesterritory;
		DROP TABLE  IF EXISTS bronze_l.production_product;
		DROP TABLE  IF EXISTS bronze_l.production_productsubcategory;
		DROP TABLE  IF EXISTS bronze_l.production_productcategory;
		DROP TABLE  IF EXISTS bronze_l.sales_customer;
		DROP TABLE  IF EXISTS bronze_l.person_person;
		DROP TABLE  IF EXISTS bronze_l.person_emailaddress;
		DROP TABLE  IF EXISTS bronze_l.person_business_entityaddress;
		DROP TABLE  IF EXISTS bronze_l.person_address;
		DROP TABLE  IF EXISTS bronze_l.person_stateprovince;
		DROP TABLE  IF EXISTS bronze_l.person_country_region;
		DROP TABLE  IF EXISTS bronze_l.sales_salesorderheader;
		DROP TABLE  IF EXISTS bronze_l.sales_salesorderdetail;
		
		
		
		CREATE TABLE bronze_l.sales_salesterritory (
		    territory_id        INT PRIMARY KEY,
		    name                VARCHAR(100),
		    country_region_code VARCHAR(10),
		    "group"             VARCHAR(50),
		    sales_ytd           NUMERIC(14,2),
		    sales_last_year     NUMERIC(14,2),
		    cost_ytd            NUMERIC(14,2),
		    cost_last_year      NUMERIC(14,2),
		    modified_date       DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		
		CREATE TABLE bronze_l.production_product (
		    product_id              INT PRIMARY KEY,
		    name                    VARCHAR(200),
		    product_number          VARCHAR(50),
		    make_flag               BOOLEAN,
		    finished_goods_flag     BOOLEAN,
		    color                   VARCHAR(50),
		    safety_stock_level      INT,
		    reorder_point           INT,
		    standard_cost           NUMERIC(12,2),
		    list_price              NUMERIC(12,2),
		    size                    VARCHAR(20),
		    size_unit_measure_code  VARCHAR(10),
		    weight_unit_measure_code VARCHAR(10),
		    weight                  NUMERIC(8,2),
		    days_to_manufacture     INT,
		    product_line            VARCHAR(10),
		    class                   VARCHAR(10),
		    style                   VARCHAR(10),
		    product_subcategory_id  INT,
		    sell_start_date         DATE,
		    sell_end_date           DATE,
		    discontinued_date       DATE,
		    modified_date           DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		
		CREATE TABLE bronze_l.production_productsubcategory (
		    product_subcategory_id INT PRIMARY KEY,
		    product_category_id    INT,
		    name                   VARCHAR(200),
		    modified_date          DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		
		CREATE TABLE bronze_l.production_productcategory (
		    product_category_id INT PRIMARY KEY,
		    name                VARCHAR(200),
		    modified_date       DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE bronze_l.sales_customer (
		    customer_id INT PRIMARY KEY,
		    person_id   INT,
		    store_id    INT,
		    territory_id INT,
		    modified_date DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE bronze_l.person_person (
		    business_entity_id INT PRIMARY KEY,
		    person_type        CHAR(2),
		    name_style         BOOLEAN,
		    title              VARCHAR(20),
		    first_name         VARCHAR(100),
		    middle_name        VARCHAR(100),
		    last_name          VARCHAR(100),
		    suffix             VARCHAR(20),
		    email_promotion    INT,
		    modified_date      DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		
		
		CREATE TABLE bronze_l.person_emailaddress (
		    business_entity_id INT,
		    email_address_id   INT,
		    email_address      VARCHAR(255),
		    modified_date      DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		
		CREATE TABLE bronze_l.person_business_entityaddress (
		    business_entity_id INT,
		    address_id         INT,
		    address_type_id    INT,
		    modified_date      DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		
		CREATE TABLE bronze_l.person_address (
		    address_id        INT PRIMARY KEY,
		    address_line1     VARCHAR(200),
		    address_line2     VARCHAR(200),
		    city              VARCHAR(100),
		    state_province_id INT,
		    postal_code       VARCHAR(20),
		    spatial_location  VARCHAR(200),
		    modified_date     DATE,
			inserted_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		

		
		
		
		CREATE TABLE bronze_l.person_stateprovince (
		    state_province_id   INT PRIMARY KEY,
		    state_province_code VARCHAR(10),
		    country_region_code VARCHAR(10),
		    is_only_state_province_flag BOOLEAN,
		    name                VARCHAR(100),
		    territory_id        INT,
		    modified_date       DATE
		);
		
		

		
		CREATE TABLE bronze_l.person_country_region (
		    country_region_code VARCHAR(10) PRIMARY KEY,
		    name                VARCHAR(100),
		    modified_date       DATE
		);
		
		

		
		CREATE TABLE bronze_l.sales_salesorderheader (
		    sales_order_id        INT PRIMARY KEY,
		    revision_number       INT,
		    order_date            DATE,
		    due_date              DATE,
		    ship_date             DATE,
		    status                INT,
		    online_order_flag     BOOLEAN,
		    sales_order_number    VARCHAR(25),
		    purchase_order_number VARCHAR(25),
		    account_number        VARCHAR(25),
		    customer_id           INT,
		    salesperson_id        INT,
		    territory_id          INT,
		    bill_to_address_id    INT,
		    ship_to_address_id    INT,
		    ship_method_id        INT,
		    credit_card_id        INT,
		    credit_card_approval_code VARCHAR(25),
		    sub_total             NUMERIC(14,2),
		    tax_amt               NUMERIC(14,2),
		    freight               NUMERIC(14,2),
		    total_due             NUMERIC(14,2),
		    modified_date         DATE
		);
		
		
		CREATE TABLE bronze_l.sales_salesorderdetail (
		    sales_order_id       INT,
		    sales_order_detail_id INT,
		    carrier_tracking_number VARCHAR(25),
		    order_qty            INT,
		    product_id           INT,
		    special_offer_id     INT,
		    unit_price           NUMERIC(12,2),
		    unit_price_discount  NUMERIC(5,2),
		    line_total           NUMERIC(14,2),
		    modified_date        DATE
		);
		


END;
$$;



---Creatinn of Bronze layer tables
call bronze_l.bronze_layer_Table_creation();
