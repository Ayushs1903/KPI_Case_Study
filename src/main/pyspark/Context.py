import argparse
from datetime import date
from Utils import Logger

logger = Logger("Context").getlogger()


class ArgsConfig:
    def __init__(self, useCase, jobMode, referenceDate, numDays, configPath):
        self.useCase = useCase
        self.jobMode = jobMode
        self.referenceDate = referenceDate
        self.numDays = numDays
        self.configPath = configPath

    def __repr__(self):
        return (f"AppConfig(useCase={self.useCase}, "
                f"jobMode={self.jobMode}, "
                f"referenceDate={self.referenceDate}, "
                f"numDays={self.numDays})"
                )

class Context:
    def __init__(self, argsConfig, config):
        self.argsConfig = argsConfig
        self.config = config



def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--useCase', type=str, required=True, help='useCase')
    parser.add_argument('--jobMode', type=str, required=True, help='jobMode')
    parser.add_argument('--referenceDate', type=str, required=False, default=date.today().__str__(), help='referenceDate')
    parser.add_argument('--numDays', type=int, required=False, help='numDays')
    parser.add_argument('--configPath', type=str, required=True, help='configPath')
    args = parser.parse_args()
    argsConfig = ArgsConfig(useCase=args.useCase, jobMode=args.jobMode, referenceDate=args.referenceDate, numDays=args.numDays, configPath=args.configPath)
    logger.info(f"Successfully created Argsconfig: {argsConfig.__str__()}")
    return argsConfig






