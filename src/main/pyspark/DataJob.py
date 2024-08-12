from Utils import Logger, DataframeImplicits, TableImplicits
from pyspark.sql.functions import col, year, month, to_date, lit
from datetime import datetime, timedelta

logger = Logger("DataJob").getlogger()
class LoadData:
    def __init__(self,spark,context):
        self.spark = spark
        self.context = context

    def ordersTransformation(self, df):
        cols = df.columns
        cols.remove("Requested_Delivery_Date")
        return cols+[
            to_date(col("Requested_Delivery_Date"),"dd-MM-yyyy").alias("Requested_Delivery_Date"),
            year(to_date(col("Requested_Delivery_Date"),"dd-MM-yyyy")).alias("Year"),
            month(to_date(col("Requested_Delivery_Date"),"dd-MM-yyyy")).alias(("Month"))
        ]


    def loadOrders(self):
        df = DataframeImplicits.read(spark=self.spark,
                                     path=self.context.config["absolutePath"]+self.context.config["data"]["orders"],
                                     format=self.context.config["dataJob"]["inputOrders"]["format"],
                                     options=self.context.config["dataJob"]["inputOrders"]["options"])
        colsToSelect = self.ordersTransformation(df=df)
        df = df.select(*colsToSelect)

        if self.context.argsConfig.jobMode=="incremental":
            logger.info(f"Started orders incremental load")
            referenceDate = self.context.argsConfig.referenceDate
            numDays = self.context.argsConfig.numDays
            requestedDate = datetime.strptime(referenceDate, "%Y-%m-%d").date() - timedelta(days=numDays)
            logger.info(f"Requested to load from date: {requestedDate}")
            filterCondition = [(col("Year") > lit(requestedDate.year)) | ((col("Year") == lit(requestedDate.year)) & (col("Month") >= requestedDate.month))]
            df = df.filter(*filterCondition)
            logger.info(f"Loaded order records: {df.count()}")
        else:
            logger.info(f"Started orders full load")

            TableImplicits.deployTable(spark=self.spark,
                                       scriptPath= self.context.config["absolutePath"]+self.context.config["tableScripts"]["orders"])

        DataframeImplicits.write(spark=self.spark,
                                 df= df,
                                 path=self.context.config["absolutePath"]+self.context.config["tables"]["orders"],
                                 format=self.context.config["dataJob"]["outputOrders"]["format"],
                                 mode=self.context.config["dataJob"]["outputOrders"]["mode"],
                                 options=self.context.config["dataJob"]["outputOrders"]["options"])



    def deliveryTransformation(self, df):
        cols = df.columns
        cols.remove("Actual_Delivery_Date")
        return cols+[
            to_date(col("Actual_Delivery_Date"),"dd-MM-yyyy").alias("Actual_Delivery_Date"),
            year(to_date(col("Actual_Delivery_Date"),"dd-MM-yyyy")).alias("Year"),
            month(to_date(col("Actual_Delivery_Date"),"dd-MM-yyyy")).alias(("Month"))
        ]

    def loadDelivery(self):
        df = DataframeImplicits.read(spark=self.spark,
                                     path=self.context.config["absolutePath"]+self.context.config["data"]["delivery"],
                                     format=self.context.config["dataJob"]["inputDelivery"]["format"],
                                     options=self.context.config["dataJob"]["inputDelivery"]["options"])
        colsToSelect = self.deliveryTransformation(df=df)
        df = df.select(*colsToSelect)

        if self.context.argsConfig.jobMode == "incremental":
            logger.info(f"Started delivery incremental load")
            referenceDate = self.context.argsConfig.referenceDate
            numDays = self.context.argsConfig.numDays
            requestedDate = datetime.strptime(referenceDate, "%Y-%m-%d").date() - timedelta(days=numDays)
            logger.info(f"Requested to load from date: {requestedDate}")
            filterCondition = [(col("Year") > lit(requestedDate.year)) | ((col("Year") == lit(requestedDate.year)) & (col("Month") >= requestedDate.month))]
            df = df.filter(*filterCondition)
            logger.info(f"Loaded delivery records: {df.count()}")
        else:
            logger.info(f"Started delivery full load")
            TableImplicits.deployTable(spark=self.spark,
                                       scriptPath= self.context.config["absolutePath"]+self.context.config["tableScripts"]["delivery"])

        DataframeImplicits.write(spark=self.spark,
                                 df= df,
                                 path=self.context.config["absolutePath"]+self.context.config["tables"]["delivery"],
                                 format=self.context.config["dataJob"]["outputDelivery"]["format"],
                                 mode=self.context.config["dataJob"]["outputDelivery"]["mode"],
                                 options=self.context.config["dataJob"]["outputDelivery"]["options"])

    def startDataJob(self):
        self.loadOrders()
        self.loadDelivery()




