from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from Utils import Logger, ConfigLoader, FileImplicits, DataframeImplicits
from Context import parseArguments, Context


logger = Logger("main").getlogger()


class SparkSessionCreater:
    def __init__(self,conf):
        self.conf = conf
    def createSpark(self):
        logger.info(f"Started Building Spark with below parameters")
        builder = SparkSession.builder
        for key,value in self.conf.items():
            logger.info(f"{key} : {value}")
            builder.config(key, value)
        return builder.getOrCreate()

def main():
    logger.info(f"Application started")
    argsConfig = parseArguments()
    # print(FileImplicits.getAbsolutePath("\\src\\main\\resources\\config.json"))
    config = ConfigLoader.loadConf(FileImplicits.getAbsolutePath("\\src\\main\\resources\\config.json"))
    context = Context(argsConfig=argsConfig, config=config)
    # print(context.config)
    # print(context.config["sparkParameters"])
    # for k,v in context.config["sparkParameters"].items():
    #     print(k,v)
    # print(context.argsConfig)
    return context


if __name__=="__main__":
    context = main()
    spark = SparkSessionCreater(context.config["sparkParameters"]).createSpark()

    # df.write.format('delta').mode('overwrite').partitionBy("Age").save("C:\\Users\\singhays\\Projects\\CaseStudy\\temp\\data")
    # FileImplicits.deleteFileOrDirectory(context.config["sparkParameters"]["spark.local.dir"])
    o=DataframeImplicits.read(spark=spark, path="C:\\Users\\singhays\\Projects\\CaseStudy\\data\\OTIF_DATA_deivery_note.csv", format="csv", options={"header":"true","inferSchema":"true"})
    # o.show()
    # o.withColumn("date",to_date("Actual_Delivery_Date","dd-MM-yyyy")).show()
    # o.printSchema()
    # with open("C:\\Users\\singhays\\Projects\\CaseStudy\\src\\main\\tablesDDL\\Delivery.sql", "r") as f:
    #     content = f.read()
    # print(content)
    # df = spark.sql(content)
    # a = spark.sql("select * from delivery")
    # a.show()
    # o.withColumn("Actual_Delivery_Date",to_date("Actual_Delivery_Date","dd-MM-yyyy")).write.format("delta").mode('overwrite').partitionBy("Sales_Organization").save("C:\\Users\\singhays\\Projects\\CaseStudy\\tables\\delivery")
    # a = spark.sql("select * from delivery")
    # a.show()





