# Databricks notebook source
# MAGIC %md
# MAGIC # Initializing Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics.customer_segments limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Assigning variables in python

# COMMAND ----------

# package imports
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from delta.tables import DeltaTable

# COMMAND ----------

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# today = datetime(2024,5,4)
today = date.today()
# first_day = today.replace(day=1)

eigtheen_months = today - relativedelta(months = 18)
CUTTOFF_DATE = eigtheen_months.strftime("%Y%m")

print("\t cutoff date: {}".format(CUTTOFF_DATE))

# COMMAND ----------

# MAGIC %md
# MAGIC # Final customer segment migration

# COMMAND ----------

# Formatting sql query string with variable
sql_query = f"""
WITH cust AS (
    SELECT DISTINCT customer_id
    FROM analytics.customer_segments
    WHERE key = 'rfm'
      AND country = 'uae'
      AND channel = 'pos'
      AND month_year >= '{CUTTOFF_DATE}'
),
months AS (
    SELECT DISTINCT month_year
    FROM analytics.customer_segments
    WHERE key = 'rfm'
      AND country = 'uae'
      AND channel = 'pos'
      AND month_year >= '{CUTTOFF_DATE}'
),
base AS (
    SELECT *
    FROM cust
    CROSS JOIN months
),
seg AS (
    SELECT customer_id, month_year, segment
    FROM analytics.customer_segments
    WHERE key = 'rfm'
      AND country = 'uae'
      AND channel = 'pos'
      AND month_year >= '{CUTTOFF_DATE}'
),
final AS (
    SELECT 
        a.customer_id, 
        a.month_year AS mon_year,
        b.month_year,
        b.segment
    FROM base a
    LEFT JOIN seg b
    ON a.customer_id = b.customer_id
    AND a.month_year = b.month_year
),
seg_mig AS (
    SELECT a.mon_year as month_year1,
        b.mon_year as month_year2,
        a.segment AS segment1,
        b.segment AS segment2,
        COUNT(DISTINCT a.customer_id) AS customers
    FROM final a
    JOIN final b
    ON a.customer_id = b.customer_id
    GROUP BY a.mon_year, b.mon_year, a.segment, b.segment
)
SELECT month_year1,
        month_year2,
        case when segment1 is null then 'NA' else segment1 end as segment1,
        case when segment2 is null then 'NA' else segment2 end as segment2,
        customers,
        CONCAT(
            CASE SUBSTRING(CAST(month_year1 AS STRING), 5, 2)
                WHEN '01' THEN 'JAN'
                WHEN '02' THEN 'FEB'
                WHEN '03' THEN 'MAR'
                WHEN '04' THEN 'APR'
                WHEN '05' THEN 'MAY'
                WHEN '06' THEN 'JUN'
                WHEN '07' THEN 'JUL'
                WHEN '08' THEN 'AUG'
                WHEN '09' THEN 'SEP'
                WHEN '10' THEN 'OCT'
                WHEN '11' THEN 'NOV'
                WHEN '12' THEN 'DEC'
            END, 
            '-', 
            SUBSTRING(CAST(month_year1 AS STRING), 1, 4)
        ) AS mon_year1,
        CONCAT(
            CASE SUBSTRING(CAST(month_year2 AS STRING), 5, 2)
                WHEN '01' THEN 'JAN'
                WHEN '02' THEN 'FEB'
                WHEN '03' THEN 'MAR'
                WHEN '04' THEN 'APR'
                WHEN '05' THEN 'MAY'
                WHEN '06' THEN 'JUN'
                WHEN '07' THEN 'JUL'
                WHEN '08' THEN 'AUG'
                WHEN '09' THEN 'SEP'
                WHEN '10' THEN 'OCT'
                WHEN '11' THEN 'NOV'
                WHEN '12' THEN 'DEC'
            END, 
            '-', 
            SUBSTRING(CAST(month_year2 AS STRING), 1, 4)
        ) AS mon_year2
FROM seg_mig

"""

sdf = spark.sql(sql_query)
df_final = sdf.pandas_api()
sdf_final = df_final.to_spark()
sdf_final.write.mode("overwrite").saveAsTable("dashboard.tbl_uae_pos_rfm_migration")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dashboard.tbl_uae_pos_rfm_migration;

# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import *
sdf.filter((col("segment1") != "NA") & (col("segment2") != "NA")).display()

# COMMAND ----------

sdf.write.format('delta').mode('overWrite').saveAsTable("sandbox.rfm_segment_migration")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sandbox.rfm_segment_migration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with trans_data 
# MAGIC AS (
# MAGIC         select 
# MAGIC         DISTINCT transaction_id, 
# MAGIC         business_day, 
# MAGIC         transaction_begin_datetime, 
# MAGIC         store_id, 
# MAGIC         billed_amount,
# MAGIC         gift_sale, 
# MAGIC         mobile, 
# MAGIC         loyalty_account_id,
# MAGIC         t1.redemption_loyalty_id,
# MAGIC         t1.returned_loyalty_id,
# MAGIC         loyalty_points, 
# MAGIC         loyalty_points_returned,
# MAGIC         redeemed_points,
# MAGIC         redeemed_amount, 
# MAGIC         --voucher_sale, 
# MAGIC         transaction_type_id
# MAGIC         from gold.qatar_pos_transactions t1
# MAGIC         where t1.business_day = "2024-06-03"
# MAGIC         and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
# MAGIC         --and t1.mobile is not null
# MAGIC         -- and t1.transaction_type_id not in ('RT','RR')
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC business_day business_date, 
# MAGIC case 
# MAGIC   when date_format(transaction_begin_datetime, 'HH:mm:ss') >= '00:00:00' and date_format(transaction_begin_datetime, 'HH:mm:ss') <= '08:00:00' then "00 to 08 hours" 
# MAGIC   when date_format(transaction_begin_datetime, 'HH:mm:ss') > '08:00:00' and date_format(transaction_begin_datetime, 'HH:mm:ss') <= '12:00:00' then "08 to 12 hours" 
# MAGIC   when date_format(transaction_begin_datetime, 'HH:mm:ss') > '12:00:00' and date_format(transaction_begin_datetime, 'HH:mm:ss') <= '16:00:00' then "12 to 16 hours" 
# MAGIC   when date_format(transaction_begin_datetime, 'HH:mm:ss') > '16:00:00' and date_format(transaction_begin_datetime, 'HH:mm:ss') <= '20:00:00' then "16 to 20 hours" 
# MAGIC   else "20 to 00 hours" 
# MAGIC end as business_time,
# MAGIC store_id as store_id,
# MAGIC round(sum(billed_amount),2) as sales,
# MAGIC coalesce(round(sum(gift_sale),2),0) as gift_sales,
# MAGIC count(distinct transaction_id) as `transactions`,
# MAGIC count(distinct case when mobile IS NOT NULL  THEN transaction_id end) as mobile_linked_transactions,
# MAGIC count(distinct mobile) as linked_mobiles,
# MAGIC count(distinct case when billed_amount >= 100 AND loyalty_account_id IS NULL THEN transaction_id  end) as missed_oppurtunity,
# MAGIC count(distinct case when (loyalty_account_id is not null and loyalty_points > 0) then transaction_id end) as loyalty_transactions, -- Loyalty only when account_id is present and transaction loyalty points are not zero
# MAGIC count(distinct case when (loyalty_account_id is not null and loyalty_points > 0) then mobile end) as loyalty_mobiles,
# MAGIC coalesce(round(sum(case when (loyalty_account_id is not null and loyalty_points > 0) then round(billed_amount,2) end),2),0) as loyalty_sales,
# MAGIC coalesce(count(distinct case when (loyalty_account_id is not null and loyalty_points > 0) then loyalty_account_id end),0) as loyalty_customers,
# MAGIC -- count(distinct case when (loyalty_account_id is not null and loyalty_points > 0) and  (redeemed_points) > 0 then loyalty_account_id end) as redeemed_customers,
# MAGIC coalesce(sum(case when (loyalty_account_id is not null and loyalty_points > 0) then case when transaction_type_id in ('RT','RR') then loyalty_points*-1 else loyalty_points end end),0) as loyalty_points_rewarded, -- Ensure the points rewarded are subtracted when the sales undergoes a return
# MAGIC coalesce(sum(case when (returned_loyalty_id is not null) then loyalty_points_returned end),0) as loyalty_points_returned,
# MAGIC coalesce(sum(case when (redemption_loyalty_id is not null) then redeemed_points end),0) as loyalty_points_redeemed,
# MAGIC coalesce(sum(case when (redemption_loyalty_id is not null) then redeemed_amount end),0) as redeemed_points_value,
# MAGIC 0 as loyalty_points_refunded
# MAGIC from 
# MAGIC trans_data
# MAGIC group by business_date, business_time, store_id
# MAGIC order by business_date, business_time, store_id

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT customer_id
# MAGIC FROM analytics.customer_segments
# MAGIC WHERE key = 'rfm'
# MAGIC   AND country = 'uae'
# MAGIC   AND channel = 'pos'
# MAGIC   AND month_year BETWEEN '2024-03-01' AND '2024-04-30'
# MAGIC   AND segment = 'slipping loyalist'
# MAGIC

# COMMAND ----------


