/*
 inserting data from bronze Layer to silver layer

*/


CREATE OR REPLACE PROCEDURE silver_l.silver_layer_data_mart_insert_data()
LANGUAGE plpgsql
AS $$
BEGIN
		TRUNCATE TABLE silver_l.factsales CASCADE;
		TRUNCATE TABLE silver_l.dimcustomer CASCADE;
		TRUNCATE TABLE silver_l.dimproduct CASCADE;
		TRUNCATE TABLE silver_l.dimterritory CASCADE;
		TRUNCATE TABLE silver_l.dimdate CASCADE;


		INSERT INTO silver_l.dimdate
		(
		    datekey,
		    fulldate,
		    yearnumber,
		    quarternumber,
		    monthnumber,
		    monthname,
		    daynumber,
		    dayname,
		    isweekend
		)
		SELECT
		    TO_CHAR(d, 'YYYYMMDD')::INT       AS datekey,
		    d                                AS fulldate,
		    EXTRACT(YEAR FROM d)::SMALLINT   AS yearnumber,
		    EXTRACT(QUARTER FROM d)::SMALLINT AS quarternumber,
		    EXTRACT(MONTH FROM d)::SMALLINT  AS monthnumber,
		    TRIM(TO_CHAR(d, 'Month'))        AS monthname,
		    EXTRACT(DAY FROM d)::SMALLINT    AS daynumber,
		    TRIM(TO_CHAR(d, 'Day'))          AS dayname,
		    CASE 
		        WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE
		        ELSE FALSE
		    END                              AS isweekend
		FROM generate_series(
		        DATE '2010-01-01',
		        DATE '2035-12-31',
		        INTERVAL '1 day'
		     ) AS d;
		
		INSERT INTO silver_l.dimterritory
		(territoryid, region, country, "Group")
		SELECT
		    territory_id,
		    name,
		    country_region_code,
		    "group"
		FROM bronze_l.sales_salesterritory;
		
		INSERT INTO silver_l.dimproduct
		(productid, productname, productnumber, category, subcategory, color, size)
		SELECT
		    p.product_id,
		    p.name,
		    p.product_number,
		    pc.name AS category,
		    ps.name AS subcategory,
		    p.color,
		    p.size
		FROM bronze_l.production_product p
		LEFT JOIN bronze_l.production_productsubcategory ps
		    ON p.product_subcategory_id = ps.product_subcategory_id
		LEFT JOIN bronze_l.production_productcategory pc
		    ON ps.product_category_id = pc.product_category_id;
		
		INSERT INTO silver_l.dimcustomer
		(customerid, fullname, email, city, country)
		SELECT
		    c.customer_id,
		    CONCAT(p.first_name, ' ', p.last_name) AS fullname,
		    ea.email_address,
		    a.city,
		    cr.name AS country
		FROM bronze_l.sales_customer c
		JOIN bronze_l.person_person p
		    ON c.person_id = p.business_entity_id
		LEFT JOIN bronze_l.person_emailaddress ea
		    ON p.business_entity_id = ea.business_entity_id
		LEFT JOIN bronze_l.person_business_entityaddress bea
		    ON p.business_entity_id = bea.business_entity_id
		LEFT JOIN bronze_l.person_address a
		    ON bea.address_id = a.address_id
		LEFT JOIN bronze_l.person_stateprovince sp
		    ON a.state_province_id = sp.state_province_id
		LEFT JOIN bronze_l.person_country_region cr
		    ON sp.country_region_code = cr.country_region_code;
		
	
		
		INSERT INTO silver_l.factsales
		(datekey, productkey, customerkey, territorykey, orderqty, unitprice, salesamount)
		SELECT
		    TO_CHAR(soh.order_date, 'YYYYMMDD')::INT AS datekey,
		    dp.productkey,
		    dc.customerkey,
		    dt.territorykey,
		    sod.order_qty,
		    sod.unit_price,
		    sod.line_total
		FROM bronze_l.sales_salesorderdetail sod
		JOIN bronze_l.sales_salesorderheader soh
		    ON sod.sales_order_id = soh.sales_order_id
		JOIN silver_l.dimproduct dp
		    ON dp.productid = sod.product_id
		JOIN silver_l.dimcustomer dc
		    ON dc.customerid = soh.customer_id
		JOIN silver_l.dimterritory dt
		    ON dt.territoryid = soh.territory_id;
END;
$$;

--- INSERITNG INTO TABLES DATA FROM BRONZE LAYER
CAll silver_l.silver_layer_data_mart_insert_data();



 select * from silver_l.dimdate;
 select * from silver_l.dimterritory;
 select * from silver_l.dimproduct;
 select * from silver_l.dimcustomer;
 select * from silver_l.factsales;




