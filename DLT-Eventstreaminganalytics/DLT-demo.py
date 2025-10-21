# DLT Pipeline - Bronze and Silver Layers
# Generates streaming events automatically - no storage needed!

import dlt
from pyspark.sql.functions import *

# =============================================================================
# BRONZE LAYER
# =============================================================================

# Bronze Layer - Auto-generated streaming events
@dlt.table(
    name="bronze_events",
    comment="Streaming events generated automatically"
)
def bronze_events():
    """
    Generates events automatically - no file storage needed!
    Creates 10 events per second
    """
    return (
        spark.readStream
            .format("rate")
            .option("rowsPerSecond", 10)
            .load()
            .withColumn("event_id", expr("uuid()"))
            .withColumn("user_id", (rand() * 1000).cast("int") + 1)
            .withColumn("session_id", concat(lit("session_"), (rand() * 100).cast("int")))
            .withColumn("event_type", 
                expr("element_at(array('click','view','purchase','add_to_cart','search','logout'), cast(rand()*6+1 as int))"))
            .withColumn("event_timestamp", current_timestamp())
            .withColumn("page_url", concat(lit("/page/"), (rand() * 50).cast("int")))
            .withColumn("product_id", concat(lit("prod_"), (rand() * 200).cast("int")))
            .withColumn("quantity", (rand() * 5).cast("int") + 1)
            .withColumn("amount", (rand() * 500).cast("decimal(10,2)"))
            .withColumn("device_type", 
                expr("element_at(array('mobile','desktop','tablet'), cast(rand()*3+1 as int))"))
            .withColumn("country", 
                expr("element_at(array('US','UK','CA','DE','FR','IN','AU'), cast(rand()*7+1 as int))"))
            .withColumn("browser", 
                expr("element_at(array('Chrome','Firefox','Safari','Edge'), cast(rand()*4+1 as int))"))
            .withColumn("is_logged_in", (rand() > 0.3).cast("boolean"))
            .withColumn("referrer_source", 
                expr("element_at(array('google','facebook','direct','email','instagram'), cast(rand()*5+1 as int))"))
            .select("event_id", "user_id", "session_id", "event_type", "event_timestamp", 
                   "page_url", "product_id", "quantity", "amount", "device_type", 
                   "country", "browser", "is_logged_in", "referrer_source")
    )

# =============================================================================
# SILVER LAYER
# =============================================================================

@dlt.table(
    name="silver_events_cleaned",
    comment="Cleaned and validated events with data quality checks"
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id > 0")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
def silver_events_cleaned():
    """
    Clean and validate events from bronze layer
    Adds business logic and derived columns
    """
    return (
        dlt.read_stream("bronze_events")
    
            # Add date/time dimensions
            .withColumn("event_date", to_date(col("event_timestamp")))
            .withColumn("event_hour", hour(col("event_timestamp")))
            .withColumn("day_of_week", dayofweek(col("event_timestamp")))
            .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False))
            
            # Add business flags
            .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
            .withColumn("has_amount", when(col("amount").isNotNull(), 1).otherwise(0))
            .withColumn("total_value", col("quantity") * col("amount"))
            
            # Categorize amounts
            .withColumn("amount_category", 
                when(col("amount") < 50, "low")
                .when(col("amount") < 200, "medium")
                .otherwise("high"))
            
            # Processing metadata
            .withColumn("processing_time", current_timestamp())
    )

@dlt.table(
    name="silver_events_enriched",
    comment="Events enriched with session-level context"
)
def silver_events_enriched():
    """
    Add additional enrichment and session context
    """
    return (
        dlt.read_stream("silver_events_cleaned")
       
            # Add device category
            .withColumn("device_category",
                when(col("device_type") == "mobile", "Mobile")
                .when(col("device_type") == "tablet", "Mobile")
                .otherwise("Desktop"))
            
            # Traffic source category
            .withColumn("traffic_category",
                when(col("referrer_source").isin(["google", "facebook", "instagram"]), "Paid")
                .when(col("referrer_source") == "direct", "Direct")
                .otherwise("Other"))
            
            # User engagement score (simple calculation)
            .withColumn("engagement_score",
                (col("is_purchase") * 10) + 
                (when(col("event_type") == "add_to_cart", 5).otherwise(0)) +
                (when(col("event_type") == "search", 3).otherwise(0)) +
                (when(col("event_type") == "view", 1).otherwise(0)))
    )

# =============================================================================
# GOLD LAYER - Aggregations and Analytics
# =============================================================================

@dlt.table(
    name="gold_hourly_metrics",
    comment="Hourly aggregated metrics by event type and device"
)
def gold_hourly_metrics():
    """
    Aggregate events by hour, event type, and device category
    """
    return (
        dlt.read_stream("silver_events_enriched")
            
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "event_type",
                "device_category"
            )
            .agg(
                count("*").alias("event_count"),
                approx_count_distinct("user_id").alias("unique_users"),
                approx_count_distinct("session_id").alias("unique_sessions"),
                sum("is_purchase").alias("total_purchases"),
                sum("total_value").alias("total_revenue"),
                avg("total_value").alias("avg_order_value"),
                avg("engagement_score").alias("avg_engagement")
            )
            .select(
                col("window.start").alias("hour_start"),
                col("window.end").alias("hour_end"),
                "event_type",
                "device_category",
                "event_count",
                "unique_users",
                "unique_sessions",
                "total_purchases",
                round(col("total_revenue"), 2).alias("total_revenue"),
                round(col("avg_order_value"), 2).alias("avg_order_value"),
                round(col("avg_engagement"), 2).alias("avg_engagement")
            )
    )

@dlt.table(
    name="gold_country_performance",
    comment="Performance metrics by country"
)
def gold_country_performance():
    """
    Aggregate metrics by country for geographic analysis
    """
    return (
        dlt.read_stream("silver_events_enriched")
      
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "country"
            )
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("user_id").alias("unique_users"),
                sum("is_purchase").alias("purchases"),
                sum("total_value").alias("revenue"),
                avg("total_value").alias("avg_transaction"),
                (sum("is_purchase") / count("*") * 100).alias("conversion_rate")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "country",
                "total_events",
                "unique_users",
                "purchases",
                round(col("revenue"), 2).alias("revenue"),
                round(col("avg_transaction"), 2).alias("avg_transaction"),
                round(col("conversion_rate"), 2).alias("conversion_rate_pct")
            )
    )

@dlt.table(
    name="gold_user_behavior",
    comment="User-level behavior analysis"
)
def gold_user_behavior():
    """
    Analyze user behavior patterns
    """
    return (
        dlt.read_stream("silver_events_enriched")
          
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "user_id"
            )
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("session_id").alias("sessions"),
                approx_count_distinct("event_type").alias("unique_event_types"),
                sum("is_purchase").alias("purchases"),
                sum("total_value").alias("total_spent"),
                max("engagement_score").alias("max_engagement"),
                first("device_category").alias("primary_device"),
                first("country").alias("country")
            )
            .withColumn("avg_events_per_session", 
                round(col("total_events") / col("sessions"), 2))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "user_id",
                "total_events",
                "sessions",
                "unique_event_types",
                "purchases",
                round(col("total_spent"), 2).alias("total_spent"),
                "max_engagement",
                "primary_device",
                "country",
                "avg_events_per_session"
            )
    )

@dlt.table(
    name="gold_traffic_source_analysis",
    comment="Traffic source performance metrics"
)
def gold_traffic_source_analysis():
    """
    Analyze performance by traffic source and category
    """
    return (
        dlt.read_stream("silver_events_enriched")
           
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "referrer_source",
                "traffic_category"
            )
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("user_id").alias("unique_users"),
                sum("is_purchase").alias("purchases"),
                sum("total_value").alias("revenue"),
                avg("engagement_score").alias("avg_engagement"),
                (sum("is_purchase") / count("*") * 100).alias("conversion_rate")
            )
            .select(
                col("window.start").alias("hour_start"),
                col("window.end").alias("hour_end"),
                "referrer_source",
                "traffic_category",
                "total_events",
                "unique_users",
                "purchases",
                round(col("revenue"), 2).alias("revenue"),
                round(col("avg_engagement"), 2).alias("avg_engagement"),
                round(col("conversion_rate"), 2).alias("conversion_rate_pct")
            )
    )

@dlt.table(
    name="gold_product_performance",
    comment="Product-level sales metrics"
)
def gold_product_performance():
    """
    Track product performance and popularity
    """
    return (
        dlt.read_stream("silver_events_enriched")
            .filter(col("event_type").isin(["view", "add_to_cart", "purchase"]))
            
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "product_id", 
                "event_type"
            )
            .agg(
                count("*").alias("event_count"),
                approx_count_distinct("user_id").alias("unique_users"),
                sum("quantity").alias("total_quantity"),
                sum("total_value").alias("total_value")
            )
            .withColumn("avg_value_per_event", 
                round(col("total_value") / col("event_count"), 2))
            .select(
                col("window.start").alias("hour_start"),
                col("window.end").alias("hour_end"),
                "product_id",
                "event_type",
                "event_count",
                "unique_users",
                "total_quantity",
                round(col("total_value"), 2).alias("total_value"),
                "avg_value_per_event"
            )
    )