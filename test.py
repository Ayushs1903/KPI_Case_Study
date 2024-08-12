from pyspark.sql import SparkSession
# import os
# import sys
#
#
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import argparse

def main(case):
    # Initialize Spark session with more resources
    # spark = SparkSession.builder \
    #     .appName("ExampleApp") \
    #     .config("spark.executor.memory", "2g") \
    #     .config("spark.driver.memory", "2g") \
    #     .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    #     .getOrCreate()
    spark = SparkSession.builder \
        .appName("DeltaLakeExample") \
        .config("spark.local.dir", "C:\\Users\\singhays\\Downloads\\OTIF_case_study\\temp") \
        .getOrCreate()
        # .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

    print(f"args received {case}")
    # Sample data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]

    # Create DataFrame
    df = spark.createDataFrame(data, ["Name", "Age"])

    # Show DataFrame
    df.show()
    # df.write.format('delta').mode('overwrite').partitionBy("Age").save("\\testf\\test1")

    with open("src/main/tablesDDL/table.sql", "r") as f:
        content = f.read()
    print(content)

    t = spark.sql(content)
    q = spark.sql("select * from temp")
    q.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Testing")
    parser.add_argument("--case",type=str, required=True, help='Path to the input data')
    parser.add_argument('--output', type=str, required=False, help='Path to the output data')
    args = parser.parse_args()
    print(args)
    main(args.case)
