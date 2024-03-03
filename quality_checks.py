from pyspark.sql import SparkSession
import time
import csv
from datetime import datetime

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark for CMS") \
    .config("spark.driver.extraClassPath", "lib/mysql-connector.jar") \
    .config("spark.executor.extraClassPath", "lib/mysql-connector.jar") \
    .getOrCreate()


# Function to fetch new entries and perform data quality checks
def fetch_new_entries():
    print("Fetching new entries...")

    # Read data from MySQL using JDBC with a custom SQL query
    query = """
        SELECT 
            p.PolicyNumber, 
            p.PolicyType,
            p.AssuredAmount,
            p.StartDate, 
            p.EndDate, 
            c.ClaimID, 
            c.ClaimType, 
            c.ClaimAmount, 
            c.CreatedAt, 
            c.UpdatedAt
        FROM 
            Policy p
        LEFT JOIN 
            Claim c ON c.PolicyNumber = p.PolicyNumber
        WHERE c.UpdatedAt >= (CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE)
    """

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/CMSBI") \
        .option("query", query) \
        .option("user", "root") \
        .option("password", "pragati") \
        .load()

    # Data Quality Checks
    print("Running data quality checks...")

    errors = []

    # Error 1: Null values in the table
    for col in df.columns:
        null_check = df.filter(df[col].isNull())
        if null_check.count() > 0:
            errors.append((datetime.now(), f"Error 1: Null values in column '{col}'"))

    # Error 2: Ambiguous data
    ambiguous_check = df.groupBy("PolicyNumber").count().where("count > 1")
    if not ambiguous_check.isEmpty():
        errors.append((datetime.now(), "Error 2: Ambiguous data"))

    # Error 3: Policy type associated with policy number doesn't match the claim type generated wrt policy number
    mismatch_check = df.filter(df["PolicyType"] != df["ClaimType"])
    if not mismatch_check.isEmpty():
        errors.append(
            (datetime.now(), "Error 3: Policy type associated with policy number doesn't match the claim type"))

    # Error 4: Claim amount is more than assured amount
    amount_check = df.filter(df["ClaimAmount"] > df["AssuredAmount"])
    if not amount_check.isEmpty():
        errors.append((datetime.now(), "Error 4: Claim amount is more than assured amount"))

    # Append errors to CSV file
    with open('error_log.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        for error in errors:
            writer.writerow(error)


# Main execution loop
try:
    while True:
        fetch_new_entries()
        print('Completed quality checking.')
        time.sleep(60)  # Sleep for 1 minute (60 seconds)
except KeyboardInterrupt:
    print("Program stopped by user.")
