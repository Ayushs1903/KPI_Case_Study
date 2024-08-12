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

    try:
        if context.argsConfig.useCase == "DataJob":
            LoadData(spark=spark, context=context).startDataJob()
        elif context.argsConfig.useCase == "CalculateKPI":
            KPILoader(spark=spark, context=context).startMetricCalculation()
        else:
            logger.info("JobMode should be DataJob or CalculateKPI")
    finally:
        spark.stop()
        logger.info("Application finished")


if __name__=="__main__":
    main()
