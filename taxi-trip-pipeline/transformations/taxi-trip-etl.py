# import delta live table and pyspark modules
import dlt
from pyspark.sql.functions import col, to_date, sum, avg, desc, to_timestamp, count, hour

# -------------------------------------------------------------------------------------------------------
# BRONZE - LAYER: Raw Data Ingestion
# -------------------------------------------------------------------------------------------------------
# Ingest data directly from the unity catalog table/location
# Define the bronze table using the dlt decorator
@dlt.table(
    name="taxi_trips_bronze",
    comment="Bronze table: Raw taxi trip dataset ingested directly from the workspace."
)
def taxi_trip_raw():
    '''Reading as a stream for DLT to process new data incrementally instead of processing the entire
     dataset'''
    return spark.readStream.table("workspace.taxi_trip.trips")

@dlt.table(
    name="zones_bronze",
    comment="Bronze table: Raw zones dataset ingested directly from the workspace."
)
def zones_raw():
    '''Reading as a stream for DLT to process new data incrementally instead of processing the entire
     dataset'''
    return spark.readStream.table("workspace.taxi_trip.zones")

# -------------------------------------------------------------------------------------------------------
# SILVER - LAYER: Cleansed & Prepared Data
# -------------------------------------------------------------------------------------------------------
# Applying data quality expectation, fix data types, and drop raw metadata cols.
# Define the silver table using the dlt table decorator
@dlt.table(
    name="taxi_trips_silver",
    comment="Silver table: Cleansed taxi trip data with proper data types and quality checks"
)
# DLT Expectations (Data Quality Checks)
# With the intention of maximizing the records that can inform the company, the quality check will
# be is placed on the total_amount column. With the assumption that records with null or negative amount 
# are invalid
# Define data quality rules using DLT decorators
@dlt.expect_or_drop("valid_trips", "total_amount IS NOT NULL AND total_amount >= 0")
def taxi_trip_prepared():
    return(
        dlt.read_stream("taxi_trips_bronze")
        # convert pickup and dropoff times to datetime
        .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"), "dd/MM/yy H:mm"))
        .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"), "dd/MM/yy H:mm"))
        # Selecting only the business relevant columns, leaving behind raw metadata
        .select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "PULocationID",
            "DOLocationID",
        )
    )

@dlt.table(
    name="zones_silver",
    comment="Silver table: Cleansed taxi trip data with proper data types and quality checks"
)
# DLT Expectations (Data Quality Checks)
# Define data quality rules using DLT decorators
@dlt.expect_or_drop("valid_zones", "LocationID IS NOT NULL") #drop zones missing a location ID
def zones_prepared():
    return (
        dlt.read_stream("zones_bronze")
        # Selecting only the business relevant columns
        .select(
            "LocationID",
            "ZoneName",
            "Latitude",
            "Longitude",
        )
    )

# -------------------------------------------------------------------------------------------------------
# GOLD - LAYER: Business-Level Aggregations
# -------------------------------------------------------------------------------------------------------
# Create summarized views for reporting
# demand summary for spatial demand and revenue optimization reports
@dlt.table(
    name="trips_gold_demand_summary",
    comment="Gold table: Aggregated total trips and amount by pick up location."
)
def trips_demand_summary():
    # combine trips and zones to get zone names
    # reading gold tables as static batches rather than streams to compute full aggregations
    df_trips = dlt.read("taxi_trips_silver")\
        .groupBy("PULocationID")\
            .agg(\
                count("*").alias("Total_Trips"),\
                sum("total_amount").alias("Total_Amount")\
                    )
    df_zones = dlt.read("zones_silver").alias("z")

    return(
        # standard PySpark DataFrame join
        df_trips.join(df_zones, df_trips.PULocationID == df_zones.LocationID, how="inner")
        .select(
            "ZoneName",
            "Latitude",
            "Longitude",
            "Total_Trips",
            "Total_Amount"
        )
    )

# temporal trends report
@dlt.table(
    name="trips_gold_temporal_trends",
    comment="Gold table: Aggregated total trips by time of day."
)
def trips_temporal_trends():
    # reading gold tables as static batches to compute full aggregations
    return(
        dlt.read("taxi_trips_silver")
        .withColumn("Hour_of_Day", hour(col("tpep_pickup_datetime")))
        .groupBy("Hour_of_Day")
        .agg(
            count("*").alias("Total_Trips")
        )
    )

# capacity planning report
@dlt.table(
    name="trips_gold_capacity_planning",
    comment="Gold table: Average passenger load per trip"
)
def capacity_planning():
    # reading gold tables as static batches to compute full aggregations
    return(
        dlt.read("taxi_trips_silver")
        .agg(
            sum("passenger_count").alias("Total_Passengers"),
            count("*").alias("Total_Trips")
        )
        .withColumn("Avg_Passenger_Load_per_Trip", col("Total_Passengers") / col("Total_Trips"))
    )
