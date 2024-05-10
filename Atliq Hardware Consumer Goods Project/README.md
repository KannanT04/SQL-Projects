# Atliq Hardware Business Analysis

## Table of contents
|Sr.| Description |Tools|
|---|-------------|-----|
|**1**|[**Data Exploration**](#1-data-exploration-using-python-pandas)|Python / Pandas|
|**2**|[**Finance Analytics**](#2-finance-analytics)|MySQL|
|2.1|[Generate a report of individual product sales for Croma India](#task-1-generate-a-report-of-individual-product-sales-for-croma-india)||
|2.2|[Aggregated monthly gross sales report for Croma India](#task-2-aggregated-monthly-gross-sales-report-for-croma)||
|2.3|[Generate a yearly report for Croma India](#task-3-generate-a-yearly-report-for-croma-india)||
|2.4|[Stored procedure to get monthly gross sales report](#task-4-stored-procedure-to-get-monthly-gross-sales-report)||
|2.5|[Stored procedure to determine the market badge](#task-5-stored-procedure-to-determine-the-market-badge)||
|**3**|[**Top Customers, Products, Markets**](#3-top-customers-products-markets)|MySQL, Excel|
|3.1|[Create a generated column `fiscal_year`](#task-1-create-a-generated-column-fiscal_year)||
|3.2|[Create `database view`s](#task-2-create-database-views)||
|3.3|[Query top 5 markets by net sales in fy 2021](#task-3-query-top-5-markets-by-net-sales-in-fy-2021)||
|3.4|[Top 5 customers by net_sales for fy 2021](#task-4-top-5-customers-by-net_sales-for-fy-2021)||
|3.5|[Top 5 products by net_sales for fy 2021](#task-5-top-5-products-by-net_sales-for-fy-2021)||
|3.6|[Bar Chart report for fy 2021 for top 10 markets & pie chart for % net sales](#task-6-bar-chart-report-for-fy-2021-for-top-10-markets--pie-chart-for--net-sales)||
|3.7|[Region-wise breakdown of net sales by customers](#task-7-region-wise-breakdown-of-net-sales-by-customers)||
|3.8|[Retrieve the top 2 markets in every region by their gross sales amount in FY 2021](#task-8-retrieve-the-top-2-markets-in-every-region-by-their-gross-sales-amount-in-fy-2021)||
|**4**|[**Ad-hoc Queries, Data Visualization & Business Insights**](#4-ad-hoc-queries-data-visualization--business-insights-using-python-pandas)|Python / Panads|
|4.1|[Get the total sold quantity for each fiscal year & Show it through visualisation](#task-1-get-the-total-sold-quantity-for-each-fiscal-year--show-it-through-visualisation)||
|4.2|[Query the quarters of 2021 by sold quantity and plot line graph](#task-2-query-the-quarters-of-2021-by-sold-quantity-and-plot-line-graph)||
|4.3|[Generate a report with Top 5 products in each division by sold quantity](#task-3-generate-a-report-with-top-5-products-in-each-division-by-sold-quantity)||
|4.4|[Which channel brings more gross sales in the year 2021 and plot the pie chart](#task-4-which-channel-brings-more-gross-sales-in-the-year-2021-and-plot-the-pie-chart)||
```






```

## 1. Data Exploration using Python Pandas

**Connect to database and read dim tables**
```python
# import necessary libraries
from sqlalchemy import create_engine
import pandas as pd

# Connect with MySql database
engine = create_engine("mysql+pymysql://root:root@localhost:3306/gdb0041")
conn = engine.connect()

# Read in dimension tables
dim_customer = pd.read_sql("dim_customer", conn)
dim_product = pd.read_sql("dim_product", conn)

# Sample rows from dim_customer
dim_customer.sample(3)
```
>||customer_code|customer|platform|channel	market|	sub_zone|	region|
>|-|-|-|-|-|-|-|
>|109|90012040|Fnac-Darty|Brick & Mortar|Retailer|Germany|NE|EU|
>|197|90023024|Sage|Brick & Mortar|Retailer|Canada	|NA	|NA|
>|121|90014136|Reliance Digital|Brick & Mortar|	Retailer|Netherlands|NE|EU|

**No of unique customers and countries(markets)**
```python
# No of unique customers and countries
for column in ["customer", "market"]:
    print(f"No of unique {column}s: {dim_customer[column].nunique()}")
```
> No of unique customers: 75

> No of unique markets: 27

**What are different platforms, channels, sub_zones, regions**
```python
# different platforms, channels, sub_zones, regions
for column in ["platform", "channel", "sub_zone", "region"]:
    print(f"{column}s -> {dim_customer[column].unique()}")
```
> platforms -> ['Brick & Mortar' 'E-Commerce']

> channels -> ['Direct' 'Distributor' 'Retailer']

> sub_zones -> ['India' 'ROA' 'ANZ' 'SE' 'NE' 'NA' 'LATAM']

> regions -> ['APAC' 'EU' 'NA' 'LATAM']

```python
# sample rows from dim_product table
dim_product = pd.read_sql("dim_product", conn)
dim_product.sample(3)
```
>|	|product_code	|division	|segment	|category	|product	|variant|
>|-|-|-|-|-|-|-|
>|218	|A4319110304	|PC	|Notebook	|Personal Laptop	|AQ Velocity	|Plus Grey|
>|380	|A6818160202	|N & S	|Storage	|USB Flash Drives	|AQ Pen Drive DRC	|Plus|
>|292	|A5318110104	|PC	|Notebook	|Gaming Laptop	|AQ Gamer 1	|Plus Firey Red|

**What are different divisions, segments and catogories**
```python
# Divisions, segments and category
for column in ["division", "segment", "category"]:
    print(f"{column} ({dim_product[column].nunique()}) -> {dim_product[column].unique()}")
```
> division (3) -> ['P & A' 'PC' 'N & S']

> segment (6) -> ['Peripherals' 'Accessories' 'Notebook' 'Desktop' 'Storage' 'Networking']

> category (14) -> ['Internal HDD' 'Graphic Card' 'Processors' 'MotherBoard' 'Mouse'
 'Keyboard' 'Batteries' 'Personal Laptop' 'Business Laptop'
 'Gaming Laptop' 'Personal Desktop' 'External Solid State Drives'
 'USB Flash Drives' 'Wi fi extender']

```python
# no of products per category
dim_product[["category", "product"]].groupby("category").count().sort_values("product", ascending=False)
```
>|category	|product|
>|-|-|	
>|Personal Laptop	|61|
>|Keyboard	|48|
>|Mouse	|48|
>|Business Laptop	|44|
>|Gaming Laptop	|40|
>|Graphic Card	|36|
>|Batteries	|20|
>|MotherBoard	|20|
>|Processors	|18|
>|Personal Desktop	|16|
>|External Solid State Drives	|15|
>|USB Flash Drives	|12|
>|Internal HDD	|10|
>|Wi fi extender	|9|

**Read in all the fact tables and give column names and no of rows for each table**
```python
# Read in fact tables in database
fact_sales_monthly = pd.read_sql("fact_sales_monthly", conn)
fact_forecast_monthly = pd.read_sql("fact_forecast_monthly", conn)
fact_freight_cost = pd.read_sql("fact_freight_cost", conn)
fact_gross_price = pd.read_sql("fact_gross_price", conn)
fact_manufacturing_cost = pd.read_sql("fact_manufacturing_cost", conn)
fact_pre_invoice_deductions = pd.read_sql("fact_pre_invoice_deductions", conn)
fact_post_invoice_deductions = pd.read_sql("fact_post_invoice_deductions", conn)

# Dictionary of tables and tables names
dict_of_tables = {"fact_sales_monthly": fact_sales_monthly, "fact_forecast_monthly": fact_forecast_monthly, 
                 "fact_freight_cost": fact_freight_cost, "fact_gross_price": fact_gross_price, 
                 "fact_manufacturing_cost": fact_manufacturing_cost, "fact_pre_invoice_deductions": fact_pre_invoice_deductions, 
                 "fact_post_invoice_deductions": fact_post_invoice_deductions}

# A function to print column names and no of rows
def give_columns_nrows(df, name):
    print(name)
    print(f"columns -> {list(df.columns)}")
    print(len(df))
    print("---------------------------------------------------\n")

for key, value in dict_of_tables.items():
    give_columns_nrows(value, key)
```
```
fact_sales_monthly
columns -> ['date', 'product_code', 'customer_code', 'sold_quantity']
1425706
---------------------------------------------------

fact_forecast_monthly
columns -> ['date', 'fiscal_year', 'product_code', 'customer_code', 'forecast_quantity']
1885941
---------------------------------------------------

fact_freight_cost
columns -> ['market', 'fiscal_year', 'freight_pct', 'other_cost_pct']
135
---------------------------------------------------

fact_gross_price
columns -> ['product_code', 'fiscal_year', 'gross_price']
1182
---------------------------------------------------

fact_manufacturing_cost
columns -> ['product_code', 'cost_year', 'manufacturing_cost']
1182
---------------------------------------------------

fact_pre_invoice_deductions
columns -> ['customer_code', 'fiscal_year', 'pre_invoice_discount_pct']
1045
---------------------------------------------------

fact_post_invoice_deductions
columns -> ['customer_code', 'product_code', 'date', 'discounts_pct', 'other_deductions_pct']
2063076
---------------------------------------------------
```

## 2. Finance Analytics
### Task 1: Generate a report of individual product sales for Croma India
Generate a report of individual product sales (aggregated on a monthly basis at the product code level) for Croma India customer for FY=2021. Atliq's fiscal year starts in September. The report should have the following fields.
1. Month
2. Product Name
3. Variant
4. Sold Quantity
5. Gross Price per Item
6. Gross Price total

```sql
-- User defined function to get fiscal year
CREATE FUNCTION `get_fiscal_year` (calendar_date DATE)
	RETURNS INTEGER
DETERMINISTIC
BEGIN
	DECLARE fiscal_year INT;
    SET fiscal_year = YEAR(DATE_ADD(calendar_date, INTERVAL 4 MONTH));
	RETURN fiscal_year;
END

-- filter fact_monthly_sales by customer_id of croma india
WITH cte AS(
    SELECT customer_code 
    FROM dim_customer
    WHERE customer LIKE '%croma%' AND market LIKE '%india%'
)
SELECT 
	MONTH(s.date) AS month, p.product, p.variant, s.sold_quantity, 
    ROUND(g.gross_price, 2) AS gross_price, 
    ROUND(s.sold_quantity*g.gross_price, 2) AS gross_price_total
FROM fact_sales_monthly s JOIN dim_product p
ON s.product_code = p.product_code
JOIN fact_gross_price g
ON 	g.product_code = s.product_code AND
g.fiscal_year = get_fiscal_year(s.date)
WHERE
	customer_code = (SELECT * FROM cte) AND
    get_fiscal_year(date) = 2021
ORDER BY date ASC;
```

>|month| product| variant| sold_quantity| gross_price| gross_price_total|
>|-|-|-|-|-|-|
>|9| AQ Dracula HDD – 3.5 Inch SATA 6 Gb/s 5400 RPM 256 MB Cache| Standard| 202| 19.06| 3849.57|
>|9| AQ Dracula HDD – 3.5 Inch SATA 6 Gb/s 5400 RPM 256 MB Cache| Plus| 162| 21.46| 3475.95|
>|9| AQ Dracula HDD – 3.5 Inch SATA 6 Gb/s 5400 RPM 256 MB Cache| Premium| 193| 21.78| 4203.44|
>|...|...|...|...|...|...|
