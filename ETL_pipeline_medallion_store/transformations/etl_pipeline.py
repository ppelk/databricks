"""
ETL-pipeline: dirty store sales data into Medallion architecture.
Bronze: load raw CSV files from the source system into materialized views.
Silver: data quality checks (uniqueness, referential integrity, validity),
    derive new features, join tables.
Gold: business logic checks and the date dimension.
"""
from pyspark.sql.functions import *
from pyspark import pipelines as dp
import json

# Declare file path and file names (stored in config.json)
filepath = "/Workspace/Users/pepelk@utu.fi/databricks/ETL_pipeline_medallion_store/data"

with open("/Workspace/Users/pepelk@utu.fi/databricks/ETL_pipeline_medallion_store/etl/config.json") as config_file:
    files = json.load(config_file)["files"]


# Helper functions
def file_path_exists(filepath: str) -> bool:
    """
    Check if given filepath exists.
    Args: filepath: Filepath in string format.
    Returns: Boolean, True is file path exists, False if not.
    """
    try:
        spark.read.format("csv").load(filepath)
        return True
    except Exception:
        return False


def read_csv(filepath: str, filename: str) -> None:
    @dp.table(
        name=f"bronze.{filename.removesuffix('.csv')}",
        comment="Raw Data ingested from local CSV file",
        table_properties={
            "quality": "bronze"
        }
    )
    def read_file():
        df = spark.read.format("csv").option("InferSchema", True).option("Header", True).load(f"{filepath}/{filename}")
        df = df.withColumn("dw_load_ts", date_format(from_utc_timestamp(current_timestamp(), "Europe/Helsinki"), "yyyy-MM-dd HH:mm:ss").cast("date"))
        return df


# Read all files and save into materialized views
for file in files:
    bronze_source_path = f"{filepath}/{file}"
    if file_path_exists(bronze_source_path):
        read_csv(filepath, file)


@dp.table(
        name="silver.orders",
        comment="Cleansed silver-quality data",
        table_properties={
            "quality": "silver"
        }
    )
def silver_orders():
    orders = spark.table("bronze.orders")
    # Clean currency
    orders = orders.withColumn("currency", when(col("currency_raw").rlike("(?i)€|eur"), "EUR")\
        .when(col("currency_raw").rlike("(?i)us|\\$"), "USD")\
        .when(col("currency_raw").rlike("(?i)gbp|£"), "GBP"))
    orders = orders.drop(col("currency_raw"))
    # Clean status
    orders = orders.withColumn("status", when(col("status_raw").rlike("(?i)ship"), "shipped")\
        .when(col("status_raw").rlike("(?i)pend"), "pending")\
        .when(col("status_raw").rlike("(?i)canc"), "canceled")\
        .when(col("status_raw").rlike("(?i)deliv"), "delivered"))
    orders = orders.drop("status_raw")
    # Convert ordered_at_date into date
    orders = orders.withColumn("ordered_at", orders.ordered_at_text.try_cast("date"))
    orders = orders.drop(col("order_at_text"))
    return orders


@dp.table(
        name="silver.customers",
        comment="Cleansed silver-quality data",
        table_properties={
            "quality": "silver"
        }
    )
def silver_customers():
    customers = spark.table("bronze.customers")
    # Clean countries
    customers = customers.withColumn("country", when(customers.country_raw.rlike("(?i)de|ger"), "Germany")\
        .when(customers.country_raw.rlike("(?i)US|U\\.S|state"), "United States")\
        .when(customers.country_raw.rlike("(?i)fr"), "France"))
    customers = customers.drop(col("country_raw"))
    # Remove duplicate ids
    duplicate_ids = customers.groupBy(col("id")).count().where(col("count") > 1).select("id")
    customers = customers.join(duplicate_ids, on="id", how="left_anti")
    # Clean age
    customers = customers.withColumn("age", when(customers.age_raw <= 0, None).otherwise(customers.age_raw))
    customers = customers.drop(col("age_raw"))
    # Clean status
    customers = customers.withColumn("status", when(customers.status_raw.rlike("(?i)ina|clo"), "inactive")\
        .when(customers.status_raw.rlike("(?i)^act"), "active"))
    customers = customers.drop(col("status_raw"))
    # Convert credit into double
    customers = customers.withColumn("credit_limit", regexp_replace(regexp_replace(col("credit_limit_text"), r"\.", ""), ",", ".")\
        .cast("double"))
    customers = customers.drop(col("credit_limit_text"))
    # Convert signup date into date type
    customers = customers.withColumn("signup_date", customers.signup_date_text.try_cast("date"))
    customers = customers.drop(col("signup_date_text"))
    # Convert last order date into date type
    customers = customers.withColumn("last_order", try_to_date(customers.last_order_text, 'd/M/yyyy'))
    customers = customers.drop(col("last_order_text"))
    # Remove tags column
    customers = customers.drop(col("tags_raw"))
    # Add column total orders count
    order_counts = spark.table("silver.orders").groupBy("customer_code")\
        .agg(count("*").alias("orders_count"), sum("quantity").alias("total_quantity"))
    customers = customers.join(order_counts, how="left", on="customer_code").fillna({"orders_count": 0, "total_quantity": 0})
    return customers


@dp.table(
        name="gold.customers",
        comment="Gold business-logic validated data",
        table_properties={
            "quality": "gold"
        }
    )
def gold_customers():
    customers = spark.table("silver.customers")
    # Business logic checks: keep only rows passing every rule
    customers = customers.where(
        # Batch id is an integer number
        col("batch_id").isNotNull()
        # Create date within normal boundaries
        & col("dw_load_ts").between(to_date(lit("2015-01-01")), current_date())
        # Credit limit within reasonable boundaries
        & (col("credit_limit").isNull() | col("credit_limit").between(-100000, 100000))
        # Signup date before last order date
        & (col("signup_date").isNull() | col("last_order").isNull() | (col("signup_date") < col("last_order")))
        # Orders count not negative
        & (col("orders_count").isNull() | (col("orders_count") >= 0))
        # Total quantity not negative
        & (col("total_quantity").isNull() | (col("total_quantity") >= 0))
    )
    return customers


@dp.table(
    name="gold.dim_date",
    comment="dim_date table",
    table_properties={
        "quality": "gold"
    }
)
def gold_dim_date():
    dim_date = spark.sql("""
                         SELECT explode(sequence(
                             to_date("2015-01-01"),
                             current_date(),
                             interval 1 day
                         )) AS calendar_date
                        """)
    dim_date = dim_date.select(
        col("calendar_date"),
        year("calendar_date").alias("year"),
        quarter("calendar_date").alias("quarter"),
        month("calendar_date").alias("month"),
        date_format("calendar_date", "MMMM").alias("month_name"),
        weekofyear("calendar_date").alias("week_of_year"),
        dayofweek("calendar_date").alias("day_of_week"),
        date_format("calendar_date", "EEEE").alias("day_name"),
        dayofmonth("calendar_date").alias("day_of_month"),
        dayofyear("calendar_date").alias("day_of_year"),
        when(dayofweek("calendar_date").isin(1, 7), True).otherwise(False).alias("is_weekend")
    )
    return dim_date
