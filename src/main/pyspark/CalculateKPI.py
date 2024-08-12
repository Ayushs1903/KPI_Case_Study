from Utils import Logger, DataframeImplicits, TableImplicits
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, when, date_add, round, sum as fsum

logger = Logger("OTIF").getlogger()

class KPILoader:
    def __init__(self, spark, context):
        self.spark = spark
        self.context = context

    def readData(self):
        orders = DataframeImplicits.read(spark=self.spark,
                                         path=self.context.config["absolutePath"]+self.context.config["tables"]["orders"],
                                         format=self.context.config["dataJob"]["outputOrders"]["format"])
        logger.info(f"Orders table loaded successfully")

        delivery = DataframeImplicits.read(spark=self.spark,
                                         path=self.context.config["absolutePath"]+self.context.config["tables"]["delivery"],
                                         format=self.context.config["dataJob"]["outputDelivery"]["format"])
        logger.info(f"Delivery table loaded successfully")

        if self.context.argsConfig.jobMode == "incremental":
            referenceDate = self.context.argsConfig.referenceDate
            numDays = self.context.argsConfig.numDays
            requestedDate = datetime.strptime(referenceDate, "%Y-%m-%d").date() - timedelta(days=numDays)
            logger.info(f"Requested to load from date: {requestedDate}")
            filterCondition = [(col("Year") > lit(requestedDate.year)) | (col("Year") == lit(requestedDate.year) & col("Month") >= requestedDate.month)]
            orders = orders.filter(*filterCondition)
            delivery = delivery.filter(*filterCondition)

        colsToSelect = [col("orders.Sales_Order_Document"),
                        col("orders.Sales_Organization"),
                        col("orders.Article"),
                        col("orders.Size_Grid_Value"),
                        col("orders.Requested_Delivery_Date"),
                        col("orders.Sales_Order_Qty"),
                        col("orders.Year"),
                        col("orders.Month"),
                        col("delivery.Actual_Delivery_Date"),
                        col("delivery.Delivery_Qty"),
                        when(col("delivery.Actual_Delivery_Date").between(col("orders.Requested_Delivery_Date"), date_add(col("orders.Requested_Delivery_Date"), 2)), lit(1))
                        .otherwise(lit(0)).alias("KPI_Flag"),
                        round(col("delivery.Delivery_Qty")/col("orders.Sales_Order_Qty"), 2).alias("Qty_Percent")
                        ]

        return orders.alias("orders").join(delivery.alias("delivery"), "Sales_Order_Document", "inner").select(*colsToSelect)

    def calculateMetrics(self):
        logger.info(f"Started OTIF metric calculation")

        df = self.readData()
        logger.info(f"Successfully read joined data")

        df = df.filter(col("KPI_Flag")==lit(1))
        groupByCols = [col("Sales_Order_Document"),col("Sales_Organization"),col("Article"),col("Size_Grid_Value"),col("Year"),col("Month")]

        aggregated = df.groupBy(*groupByCols).agg(fsum(col("Qty_Percent")*lit(100)).alias("OTIF_KPI"))
        aggregated = aggregated.filter(col("OTIF_KPI")>lit(90))

        DataframeImplicits.write(spark=self.spark,
                                 df= aggregated,
                                 path=self.context.config["absolutePath"]+self.context.config["tables"]["OTIF"],
                                 format=self.context.config["OTIF"]["format"],
                                 mode=self.context.config["OTIF"]["mode"],
                                 options=self.context.config["OTIF"]["options"])

    def startMetricCalculation(self):
        if self.context.argsConfig.jobMode == "full":
            TableImplicits.deployTable(spark=self.spark,
                                       scriptPath=self.context.config["absolutePath"]+self.context.config["tableScripts"]["OTIF"])

        self.calculateMetrics()






