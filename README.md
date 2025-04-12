Use Delta Lake in Azure Synapse Analytics

This lab demonstrates how to use Delta Lake with Apache Spark in Azure Synapse Analytics. Delta Lake provides transactional data storage on top of data lakes, enabling ACID transactions, schema enforcement, and time travel queries. With Delta Lake, you can build a Lakehouse architecture that integrates both batch and streaming data processing.
Lab Overview

This lab will guide you through the following steps:

    Provisioning an Azure Synapse Analytics Workspace

    Loading Data into Delta Lake

    Working with Delta Tables

    Creating External and Managed Catalog Tables

    Streaming Data with Delta Lake

    Querying Delta Tables Using a Serverless SQL Pool

    Deleting Azure Resources

Estimated Duration:

This exercise should take approximately 40 minutes to complete.
Prerequisites

    An Azure subscription with administrative access.

    Knowledge of Azure Synapse Analytics, Delta Lake, and Apache Spark.

    Access to the Azure Portal and Cloud Shell.

Steps to Complete the Lab
1. Provision an Azure Synapse Analytics Workspace
a. Sign in to the Azure portal

    Go to Azure Portal.

b. Open Cloud Shell

    Click the >_ button next to the search bar in the Azure portal to open Cloud Shell.

    Choose PowerShell as the shell environment and create storage if prompted.

c. Clone the Repository

rm -r dp-203 -f
git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203

d. Run Setup Script

    Navigate to the lab folder and run the setup script.

cd dp-203/Allfiles/labs/07
./setup.ps1

    Wait for the script to complete, which may take around 10 minutes.

2. Load and Explore Data in the Data Lake

    After the script completes, go to the Azure Synapse Studio in your browser.

    In Synapse Studio, open the Data page and check the Linked tab.

    Verify the Azure Data Lake Storage Gen2 is linked and contains a products.csv file under the files container.

a. Load Data into DataFrame

    Open a new notebook and load the data from products.csv into a DataFrame.

%%pyspark
df = spark.read.load('abfss://files@datalakexxxxxxx.dfs.core.windows.net/products/products.csv', format='csv', header=True)
display(df.limit(10))

3. Create Delta Tables
a. Write Data to Delta Table

    Save the loaded DataFrame to a Delta table.

delta_table_path = "/delta/products-delta"
df.write.format("delta").save(delta_table_path)

    Navigate to the files tab to view the new delta folder containing the products-delta table.

b. Update Delta Table

    Update the price of product 771 in the Delta table.

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.update(
    condition = "ProductID == 771",
    set = { "ListPrice": "ListPrice * 0.9" }
)

deltaTable.toDF().show(10)

c. Verify Delta Table Update

    Load the updated Delta table into a DataFrame and display the results.

new_df = spark.read.format("delta").load(delta_table_path)
new_df.show(10)

d. Use Time Travel for Versioned Data

    Query a previous version of the Delta table.

new_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
new_df.show(10)

4. Create Catalog Tables
a. Create an External Table

spark.sql("CREATE DATABASE AdventureWorks")
spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)

b. Query External Table

%%sql
USE AdventureWorks;
SELECT * FROM ProductsExternal;

c. Create a Managed Table

df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsManaged").show(truncate=False)

d. Query Managed Table

%%sql
USE AdventureWorks;
SELECT * FROM ProductsManaged;

e. Compare External and Managed Tables

    Use SQL to list the tables and drop them.

%%sql
USE AdventureWorks;
SHOW TABLES;

    Drop the tables:

%%sql
USE AdventureWorks;
DROP TABLE IF EXISTS ProductsExternal;
DROP TABLE IF EXISTS ProductsManaged;

5. Use Delta Tables for Streaming Data
a. Create a Streaming Data Source

    Create a folder and write simulated IoT data to a stream.

from notebookutils import mssparkutils
from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = '/data/'
mssparkutils.fs.mkdirs(inputPath)

jsonSchema = StructType([
    StructField("device", StringType(), False),
    StructField("status", StringType(), False)
])
iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

device_data = '''{"device":"Dev1","status":"ok"} ... '''
mssparkutils.fs.put(inputPath + "data.txt", device_data, True)

b. Write Streaming Data to Delta Table

delta_stream_table_path = '/delta/iotdevicedata'
checkpointpath = '/delta/checkpoint'
deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)

c. Read the Streaming Data

df = spark.read.format("delta").load(delta_stream_table_path)
display(df)

6. Query Delta Tables from a Serverless SQL Pool

    Use the Serverless SQL pool to query Delta tables directly via SQL.

a. Query Data Using SQL

SELECT TOP 100 * FROM OPENROWSET(BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/delta/products-delta/', FORMAT = 'DELTA') AS [result];

7. Delete Azure Resources

    After completing the lab, delete the resources to avoid unnecessary costs:

        Go to Resource Groups in the Azure portal.

        Select the dp203-xxxxxxx resource group and delete it.

Conclusion

In this lab, you learned how to use Delta Lake in Azure Synapse Analytics to manage both batch and streaming data. You also explored creating Delta Tables, querying them, and integrating them with SQL Pools and Spark. Finally, you used time travel and streaming data features in Delta Lake.
