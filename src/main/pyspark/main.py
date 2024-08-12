from pyspark.sql import SparkSession
from Utils import Logger, ConfigLoader
from Context import parseArguments, Context
from DataJob import LoadData
from CalculateKPI import KPILoader


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

def contextCreator():
    argsConfig = parseArguments()
    config = ConfigLoader.loadConf(argsConfig.configPath)
    context = Context(argsConfig=argsConfig, config=config)
    logger.info(f"Context created")
    return context


def main():
    logger.info(f"Application started")
    context = contextCreator()
    spark = SparkSessionCreater(context.config["sparkParameters"]).createSpark()

    if context.argsConfig.useCase == "DataJob":
        LoadData(spark=spark, context=context).startDataJob()
    elif context.argsConfig.useCase == "CalculateKPI":
        KPILoader(spark=spark, context=context).startMetricCalculation()
    else:
        logger.info("JobMode should be DataJob or CalculateKPI")





if __name__=="__main__":
    main()
    # spark = SparkSessionCreater(context.config["sparkParameters"]).createSpark()

    # df.write.format('delta').mode('overwrite').partitionBy("Age").save("C:\\Users\\singhays\\Projects\\CaseStudy\\temp\\data")
    # FileImplicits.deleteFileOrDirectory(context.config["sparkParameters"]["spark.local.dir"])
    # o=DataframeImplicits.read(spark=spark, path="C:\\Users\\singhays\\Projects\\CaseStudy\\data\\OTIF_DATA_deivery_note.csv", format="csv", options={"header":"true","inferSchema":"true"})
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





