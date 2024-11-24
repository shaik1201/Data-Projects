import json
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# Load the configuration file
with open('config.json', 'r') as config_file:
    CONFIG = json.load(config_file)


def transform_airport(airport_df):
    """Transform airport dataframe and return a cleaned dataframe."""
    rename_dict = {
        "_c0": "ID",
        "_c1": "Airport_Name",
        "_c2": "City",
        "_c3": "Country",
        "_c4": "IATA_Code",
        "_c5": "Airport_Code",
        "_c6": "Latitude",
        "_c7": "Longitude",
        "_c8": "Altitude",
        "_c9": "Timezone_Offset",
        "_c10": "DST",
        "_c11": "Timezone",
        "_c12": "Type",
        "_c13": "Source"
    }
    
    for old_name, new_name in rename_dict.items():
        airport_df = airport_df.withColumnRenamed(old_name, new_name)
        
    airport_df = airport_df.drop('Timezone_Offset', 'DST', 'Timezone', 'Type', 'Source')
    return airport_df


def transform_arrival_departure(arrivals_df, departures_df):
    """Transform arrivals and departures dataframes and clean them."""
    rename_dict = {
        "arrivalAirportCandidatesCount": "Arrival_Airport_Candidate_Count",
        "callsign": "Callsign",
        "departureAirportCandidatesCount": "Departure_Airport_Candidate_Count",
        "estArrivalAirport": "Arrival_Airport",
        "estArrivalAirportHorizDistance": "Estimated_Arrival_Airport_Horizontal_Distance",
        "estArrivalAirportVertDistance": "Estimated_Arrival_Airport_Vertical_Distance",
        "estDepartureAirport": "Departure_Airport",
        "estDepartureAirportHorizDistance": "Estimated_Departure_Airport_Horizontal_Distance",
        "estDepartureAirportVertDistance": "Estimated_Departure_Airport_Vertical_Distance",
        "firstSeen": "First_Seen_Timestamp",
        "icao24": "ICAO24_Code",
        "lastSeen": "Last_Seen_Timestamp"
    }
    
    for old_name, new_name in rename_dict.items():
        arrivals_df = arrivals_df.withColumnRenamed(old_name, new_name)
        departures_df = departures_df.withColumnRenamed(old_name, new_name)
        
    # Convert timestamps
    for df in [arrivals_df, departures_df]:
        df = df.withColumn("First_Seen_Timestamp", from_unixtime("First_Seen_Timestamp").cast("timestamp"))
        df = df.withColumn("Last_Seen_Timestamp", from_unixtime("Last_Seen_Timestamp").cast("timestamp"))
    
    # Drop unnecessary columns
    drop_columns = [
        'Arrival_Airport_Candidate_Count', 'Departure_Airport_Candidate_Count',
        'Estimated_Arrival_Airport_Horizontal_Distance', 'Estimated_Arrival_Airport_Vertical_Distance',
        'Estimated_Departure_Airport_Horizontal_Distance', 'Estimated_Departure_Airport_Vertical_Distance'
    ]
    arrivals_df = arrivals_df.drop(*drop_columns)
    departures_df = departures_df.drop(*drop_columns)
    
    return arrivals_df, departures_df


def analyze_data(spark, arrivals_df, departures_df, airport_df):
    """Perform analysis on the data and return the results."""
    arrivals_df.createOrReplaceTempView('arrival')
    departures_df.createOrReplaceTempView('departure')
    airport_df.createOrReplaceTempView('airport')
    
    arrival_airport = spark.sql(
        """
        SELECT Arrival_Airport, Departure_Airport, City, Country, Latitude, Longitude, First_Seen_Timestamp, Last_Seen_Timestamp
        FROM arrival LEFT JOIN airport
        ON arrival.Departure_Airport = airport.Airport_Code
        """
    )
    
    departure_airport = spark.sql(
        """
        SELECT Arrival_Airport, Departure_Airport, City, Country, Latitude, Longitude, First_Seen_Timestamp, Last_Seen_Timestamp
        FROM departure LEFT JOIN airport
        ON departure.Arrival_Airport = airport.Airport_Code
        """
    )
    
    arrival_airport.createOrReplaceTempView('arrival_airport')
    departure_airport.createOrReplaceTempView('departure_airport')
    
    num_flights_arriving = spark.sql("SELECT COUNT(*) AS Num_Flights FROM arrival_airport")
    num_flights_departuring = spark.sql("SELECT COUNT(*) AS Num_Flights FROM departure_airport")
    
    top_countries_arriving = spark.sql(
        """
        SELECT Country, COUNT(*) AS Num_Arrivals
        FROM arrival_airport
        GROUP BY Country
        ORDER BY Num_Arrivals DESC
        """
    )
    
    top_countries_departuring = spark.sql(
        """
        SELECT Country, COUNT(*) AS Num_Departures
        FROM departure_airport
        GROUP BY Country
        ORDER BY Num_Departures DESC
        """
    )
    
    return (arrival_airport, departure_airport, num_flights_arriving, 
            num_flights_departuring, top_countries_arriving, top_countries_departuring)


def save_to_gcs(df, folder_name, file_name):
    """Save DataFrame to GCS in Parquet format."""
    bucket_name = CONFIG['gcs']['bucket_name']
    output_path = f"gs://{bucket_name}/{folder_name}/{file_name}"
    df.write.mode("overwrite").parquet(output_path)


def config_spark():
    """Configure Spark with GCS integration."""
    credentials_location = CONFIG['gcs']['credentials_path']
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('FlightDataAnalysis') \
        .set("spark.jars", "/home/Shai-user/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    
    sc = SparkContext(conf=conf)
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    return SparkSession.builder.config(conf=sc.getConf()).getOrCreate()


def main():
    """Main function to execute the ETL process."""
    spark = config_spark()
    
    # Load datasets from GCS
    arrivals_df = spark.read.json(CONFIG['gcs']['bucket_name'] + '/arrival.json')
    departures_df = spark.read.json(CONFIG['gcs']['bucket_name'] + '/departure.json')
    airport_df = spark.read.csv(CONFIG['gcs']['bucket_name'] + '/airports.csv')
    
    # Transform data
    arrivals_df, departures_df = transform_arrival_departure(arrivals_df, departures_df)
    airport_df = transform_airport(airport_df)
    
    # Analyze data
    results = analyze_data(spark, arrivals_df, departures_df, airport_df)
    
    # Save results to GCS
    folder_name = 'analysis_outputs'
    save_to_gcs(results[0], folder_name, 'arrival_airport.parquet')
    save_to_gcs(results[1], folder_name, 'departure_airport.parquet')
    save_to_gcs(results[2], folder_name, 'num_flights_arriving.parquet')
    save_to_gcs(results[3], folder_name, 'num_flights_departuring.parquet')
    save_to_gcs(results[4], folder_name, 'top_countries_arriving.parquet')
    save_to_gcs(results[5], folder_name, 'top_countries_departuring.parquet')
    
    spark.stop()


if __name__ == "__main__":
    main()
