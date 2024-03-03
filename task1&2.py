from pyspark.sql import SparkSession
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark for CMS") \
    .config("spark.driver.extraClassPath", "lib/mysql-connector.jar") \
    .config("spark.executor.extraClassPath", "lib/mysql-connector.jar") \
    .getOrCreate()


def fetch_new_entries():
    print("Fetching new entries...")

    # Read data from MySQL using JDBC with a custom SQL query
    query = """
        SELECT * 
        FROM Claim 
        WHERE UpdatedAt >= (CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE)
    """

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/CMSDB") \
        .option("query", query) \
        .option("user", "root") \
        .option("password", "pragati") \
        .load()

    # Show some rows from the DataFrame
    df.show()


# Call the fetch_new_entries function
try:
    while True:
        fetch_new_entries()
        time.sleep(60)  # Sleep for 1 minutes (60 seconds)
except KeyboardInterrupt:
    print("Program stopped by user.")
