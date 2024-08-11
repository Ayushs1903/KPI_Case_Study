spark-submit ^
--conf spark.pyspark.driver.python=C:\Users\singhays\AppData\Local\Programs\Python\Python39\python.exe ^
--conf spark.pyspark.python=C:\Users\singhays\AppData\Local\Programs\Python\Python39\python.exe ^
--packages io.delta:delta-core_2.12:2.4.0 ^
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension ^
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog ^
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=C:\\Users\\singhays\\Projects\\CaseStudy\\log4j.properties ^
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=C:\\Users\\singhays\\Projects\\CaseStudy\\log4j.properties ^
C:\\Users\\singhays\\Projects\\CaseStudy\\src\\main\\pyspark\\main.py ^
--useCase datajob ^
--jobMode full