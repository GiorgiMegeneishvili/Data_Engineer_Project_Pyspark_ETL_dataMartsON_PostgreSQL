

--- creation View on Gold layer
--create schema gold_l;
--- Sales Overview

CREATE OR REPLACE PROCEDURE gold_l.gold_layer_view()
LANGUAGE plpgsql
AS $$
BEGIN
		
		CREATE OR REPLACE VIEW gold_l.vw_sales_overview AS
		SELECT
		    d.fulldate,
		    d.yearnumber,
		    d.monthnumber,
		    d.monthname,
		    p.productname,
		    p.category,
		    p.subcategory,
		    c.fullname AS customer_nsilver_layer_data_mart_insert_dataame,
		    t.region,
		    t.country,
		    f.orderqty,
		    f.unitprice,
		    f.salesamount
		FROM silver_l.factsales f
		JOIN silver_l.dimdate d       ON f.datekey = d.datekey
		JOIN silver_l.dimproduct p    ON f.productkey = p.productkey
		JOIN silver_l.dimcustomer c   ON f.customerkey = c.customerkey
		JOIN silver_l.dimterritory t  ON f.territorykey = t.territorykey;
		
		
		-- Monthly Sales Summary
		CREATE OR REPLACE VIEW gold_l.vw_sales_monthly AS
		SELECT
		    d.yearnumber,
		    d.monthnumber,
		    d.monthname,
		    SUM(f.orderqty)        AS total_quantity,
		    SUM(f.salesamount)    AS total_sales
		FROM silver_l.factsales f
		JOIN silver_l.dimdate d ON f.datekey = d.datekey
		GROUP BY
		    d.yearnumber,
		    d.monthnumber,
		    d.monthname;
		
		
		-- Sales by Product
		CREATE OR REPLACE VIEW gold_l.vw_sales_by_product AS
		SELECT
		    p.productname,
		    p.category,
		    p.subcategory,
		    SUM(f.orderqty)     AS total_quantity,
		    SUM(f.salesamount) AS total_sales
		FROM silver_l.factsales f
		JOIN silver_l.dimproduct p ON f.productkey = p.productkey
		GROUP BY
		    p.productname,
		    p.category,
		    p.subcategory;
		
		
		-- Sales by Territory
		CREATE OR REPLACE VIEW gold_l.vw_sales_by_territory AS
		SELECT
		    t.region,
		    t.country,
		    SUM(f.salesamount) AS total_sales,
		    SUM(f.orderqty)    AS total_quantity
		FROM silver_l.factsales f
		JOIN silver_l.dimterritory t ON f.territorykey = t.territorykey
		GROUP BY
		    t.region,
		    t.country;
		
		
		-- Customer Sales Performance
		CREATE OR REPLACE VIEW gold_l.vw_customer_sales AS
		SELECT
		    c.fullname,
		    c.email,
		    c.country,
		    COUNT(DISTINCT f.datekey) AS active_days,
		    SUM(f.orderqty)           AS total_quantity,
		    SUM(f.salesamount)        AS total_sales
		FROM silver_l.factsales f
		JOIN silver_l.dimcustomer c ON f.customerkey = c.customerkey
		GROUP BY
		    c.fullname,
		    c.email,
		    c.country;

END;
$$;
--Executing Gold Layer 
call gold_l.gold_layer_view();
--select * from gold_l.vw_customer_sales;
