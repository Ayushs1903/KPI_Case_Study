import logging
import os
import shutil
import json



class Logger:
    def __init__(self, name, level=logging.INFO):
        self.format="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s : %(message)s"
        self.dateformat = "%Y-%m-%d"
        self.name=name
        self.level = level
    def getlogger(self):
        logging.basicConfig(format=self.format, datefmt=self.dateformat)
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        return logger


logger = Logger("Utility").getlogger()


class FileImplicits(object):
    @staticmethod
    def getRootPath():
        return "C:\\Users\\singhays\\Projects\\CaseStudy"
    @staticmethod
    def getAbsolutePath(relativePath):
        return FileImplicits.getRootPath()+relativePath
    @staticmethod
    def deleteFileOrDirectory(path):
        if os.path.isfile(path):
            os.remove(path)
            logger.info(f"Successfully deleted file path: {path}")
        elif os.path.isdir(path):
            shutil.rmtree(path)
            logger.info(f"Successfully deleted folder path: {path}")
        else:
            logger.info(f"No file or directory found: {path}")

class ConfigLoader(object):
    @staticmethod
    def loadConf(path):
        with open(path,"r") as conf:
            config = json.load(conf)
        return config


class DataframeImplicits(object):
    @staticmethod
    def read(spark, path, format, options={}):
        logger.info(f"Started reading path: {path}")
        return spark.read.format(format).options(**options).load(path)
    @staticmethod
    def write(spark, df, path, format, mode, options={}):
        logger.info(f"Started writing to path: {path}")
        df.write.format(format).mode(mode).options(**options).save(path)

class TableImplicits(object):
    @staticmethod
    def loadTable(path):
        with open(path,"r") as table:
            script = table.read()
        return script
    @staticmethod
    def deployTable(spark, scriptPath):
        script = TableImplicits.loadTable(scriptPath)
        logger.info(f"Deploying table \n {script}")
        table = spark.sql(script)
        table.show()
        logger.info(f"Successfully deployed table")


















