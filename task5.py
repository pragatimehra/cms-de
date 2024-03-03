from pyspark.sql import SparkSession
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark for CMS") \
    .config("spark.driver.extraClassPath", "lib/mysql-connector.jar") \
    .config("spark.executor.extraClassPath", "lib/mysql-connector.jar") \
    .getOrCreate()

print("Workflow Orchestra starting...")


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


def quality_checks_on_new_entries():
    print("Running data quality checks...")

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

    # Data Quality Checks

    # 1. Check for missing values in critical data elements
    critical_columns = ['ClaimID', 'PolicyNumber', 'ClaimType', 'ClaimAmount', 'Status', 'Reason']
    for column in critical_columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            print(f"WARNING: Found {null_count} null values in column {column}")

    # 2. Check for duplicate entries based on ClaimID -- already checked in table, ClaimID ia PK
    duplicate_count = df.groupBy('ClaimID').count().filter('count > 1').count()
    if duplicate_count > 0:
        print(f"WARNING: Found {duplicate_count} duplicate ClaimID entries")

    # 3. Check for invalid values in ClaimAmount (e.g., 0 or negative amounts)
    invalid_amount_count = df.filter(df['ClaimAmount'] <= 0).count()
    if invalid_amount_count > 0:
        print(f"WARNING: Found {invalid_amount_count} entries with invalid ClaimAmount")

    # 4. Check for inconsistent data (e.g., Status is 'Approved' but ClaimAmount is 0)
    inconsistent_data_count = df.filter((df['Status'] == 'Approved') & (df['ClaimAmount'] == 0)).count()
    if inconsistent_data_count > 0:
        print(
            f"WARNING: Found {inconsistent_data_count} entries with inconsistent data (Approved status but ClaimAmount is 0)")


def fetch_encry_policy_info():
    print("Fetching encrypted policy information...")

    # Read data from MYSQL using JDBC with a custom SQL query
    query = """
        SELECT 
            p.UserID,
            sha2(p.PolicyHolder, 256) AS EncryptedPolicyHolder,
            sha2(p.PolicyNumber, 256) AS EncryptedPolicyNumber,
            p.PolicyType,
            COUNT(c.ClaimID) AS TotalClaims,
            SUM(CASE WHEN c.Status = 'Pending' THEN 1 ELSE 0 END) AS OpenClaims,
            SUM(CASE WHEN c.Status = 'Pending' THEN 0 ELSE 1 END) AS ClosedClaims
        FROM 
            Policy p
        LEFT JOIN 
            Claim c ON p.PolicyNumber = c.PolicyNumber
        GROUP BY 
            p.UserID, p.PolicyHolder, p.PolicyType, p.PolicyNumber
    """

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/CMSDB") \
        .option("query", query) \
        .option("user", "root") \
        .option("password", "pragati") \
        .load()

    # Show Policy and associated Claims
    df.show()


# Call the routine
try:
    while True:
        fetch_new_entries()
        quality_checks_on_new_entries()
        print("Quality checks executed successfully.")
        fetch_encry_policy_info()
        time.sleep(60)  # Sleep for 1 minute (60 seconds)
except KeyboardInterrupt:
    print("Program stopped by user.")
