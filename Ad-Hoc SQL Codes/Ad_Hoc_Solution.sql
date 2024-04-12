#1.Provide the list of markets in which customer "Atliq Exclusive" operates its business in the APAC region
SELECT DISTINCT(market)
FROM dim_customer
WHERE customer='Atliq Exclusive' AND region='APAC';

#2.What is the percentage of unique product increase in 2021 vs. 2020? The final output contains these fields, unique_products_2020 unique_products_2021 percentage_chg
SELECT unique_products_2020,unique_products_2021,ROUND((unique_products_2021-unique_products_2020)*100/(unique_products_2020),2) AS PCT
FROM (
(SELECT COUNT(DISTINCT(product_code)) AS unique_products_2020
FROM fact_sales_monthly 
WHERE fiscal_year=2020) x,
(SELECT COUNT(DISTINCT(product_code)) AS unique_products_2021
FROM fact_sales_monthly
WHERE fiscal_year=2021)y
);

#3.Provide a report with all the unique product counts for each segment and sort them in descending order of product counts.
SELECT segment,COUNT(DISTINCT(product_code)) AS product_count
FROM dim_product
GROUP BY segment
ORDER BY product_count DESC;

#4.Follow-up: Which segment had the most increase in unique products in 2021 vs 2020? The final output contains these fields, segment product_count_2020 product_count_2021 difference
WITH x AS (SELECT segment,COUNT(DISTINCT(p.product_code)) product_count_2020
FROM dim_product p
JOIN fact_sales_monthly s
ON p.product_code= s.product_code
WHERE fiscal_year=2020
GROUP BY segment),
y AS (SELECT segment,COUNT(DISTINCT(p.product_code)) product_count_2021
FROM dim_product p
JOIN fact_sales_monthly s
ON p.product_code= s.product_code
WHERE fiscal_year=2021
GROUP BY segment)
SELECT x.segment,x.product_count_2020,y.product_count_2021,
(y.product_count_2021-x.product_count_2020) AS difference
FROM x
JOIN y
ON x.segment=y.segment
ORDER BY difference DESC;

#5.Get the products that have the highest and lowest manufacturing costs. The final output should contain these fields, product_code product manufacturing_cost
WITH x as(SELECT * FROM dim_product),
     y AS (SELECT * FROM fact_manufacturing_cost
   WHERE manufacturing_cost IN (
   (SELECT MAX(manufacturing_cost) FROM fact_manufacturing_cost),
   (SELECT MIN(manufacturing_cost) FROM fact_manufacturing_cost)))
   SELECT x.product_code,x.product,ROUND(y.manufacturing_cost,2) AS manufacturing_cost
   FROM x
   JOIN y
   ON x.product_code=y.product_code
   ORDER BY manufacturing_cost DESC;
   
#6.Generate a report which contains the top 5 customers who received an average high pre_invoice_discount_pct for the fiscal year 2021 and in the Indian market. The final output contains these fields, customer_code customer average_discount_percentage
SELECT c.customer_code,customer,ROUND(AVG(pre_invoice_discount_pct)*100,2) AS avg_discount_pct
FROM dim_customer c
JOIN fact_pre_invoice_deductions pre
ON c.customer_code=pre.customer_code
WHERE fiscal_year=2021 AND market = 'India'
GROUP BY customer_code,customer
ORDER BY avg_discount_pct DESC
LIMIT 5;

#7.Get the complete report of the Gross sales amount for the customer “Atliq Exclusive” for each month . This analysis helps to get an idea of low and high-performing months and take strategic decisions. The final report contains these columns: Month Year Gross sales Amount
SELECT MONTHNAME(fs.date) AS month,fs.fiscal_year,
ROUND(SUM(g.gross_price*fs.sold_quantity)/1000000,2) AS gross_sales_amount 
FROM fact_sales_monthly fs
JOIN dim_customer c 
ON fs.customer_code=c.customer_code
JOIN fact_gross_price g
ON fs.product_code=g.product_code
WHERE c.customer='Atliq Exclusive'
GROUP BY month,fs.fiscal_year
ORDER BY fs.fiscal_year;

#8.In which quarter of 2020, got the maximum total_sold_quantity? The final output contains these fields sorted by the total_sold_quantity, Quarter total_sold_quantity
SELECT 
CASE 
   WHEN MONTH(date) IN (9,10,11) THEN 'Q1'
   WHEN MONTH(date) IN (12,1,2) THEN 'Q2'
   WHEN MONTH(date) IN (3,4,5) THEN 'Q3'
   ELSE 'Q4'
   END AS qtr,
   ROUND(SUM(sold_quantity)/1000000,2) AS total_sold_quantity_mln
   FROM fact_sales_monthly
   WHERE fiscal_year=2020
   GROUP BY qtr
   ORDER BY total_sold_quantity_mln DESC;

#9.Which channel helped to bring more gross sales in the fiscal year 2021 and the percentage of contribution? The final output contains these fields, channel gross_sales_mln percentage
WITH cte1 AS (SELECT channel, ROUND(SUM(sold_quantity*gross_price)/1000000,2) AS gross_sales_mln
FROM dim_customer c
JOIN fact_sales_monthly s
ON c.customer_code=s.customer_code
JOIN fact_gross_price g
ON g.product_code=s.product_code AND
g.fiscal_year=s.fiscal_year
WHERE g.fiscal_year=2021
GROUP BY channel
ORDER BY gross_sales_mln DESC)

SELECT channel,gross_sales_mln * 100/SUM(gross_sales_mln) OVER() as percentage
FROM cte1
GROUP BY channel;

#10.Get the Top 3 products in each division that have a high total_sold_quantity in the fiscal_year 2021? The final output contains these fields, division product_code codebasics.io
#product total_sold_quantity rank_order
WITH cte2 AS (SELECT division,p.product_code,product,SUM(sold_quantity) AS total_sold_quantity
FROM dim_product p
JOIN fact_sales_monthly s
ON p.product_code=s.product_code
WHERE fiscal_year=2021
GROUP BY division,product,p.product_code),

cte3 AS (SELECT *,
DENSE_RANK() OVER(PARTITION BY division ORDER BY total_sold_quantity DESC) AS drnk
FROM cte2)

SELECT * FROM cte3 WHERE drnk <=3;