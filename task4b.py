from pyspark.sql import SparkSession
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark for CMS") \
    .config("spark.driver.extraClassPath", "lib/mysql-connector.jar") \
    .config("spark.executor.extraClassPath", "lib/mysql-connector.jar") \
    .getOrCreate()


def fetch_encry_policy_info():
    print("Fetching policy information...")

    # Read data from MYSQL using JDBC with a custom SQL query
    query = """
        SELECT 
            p.UserID,
            sha2(p.PolicyHolder, 256) AS EncryptedPolicyHolder,
            REPLACE('####', p.PolicyNumber, '####') AS MaskedPolicyNumber,
            p.PolicyType AS PolicyType,
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


# Call the fetch_policy_info function
try:
    while True:
        fetch_encry_policy_info()
        time.sleep(60)  # Sleep for 1 minute (60 seconds)
except KeyboardInterrupt:
    print("Program stopped by user.")
