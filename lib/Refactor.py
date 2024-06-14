import sys
from lib.logger import Log4j
import lib.Schemas as Schema


def read_file_csv(spark, file_name):
    logger = Log4j(spark)
    logger.info("Reading file " + file_name)
    if file_name == "test_data/accounts/account_samples.csv":
        file_schema = Schema.account_schema
    elif file_name == "test_data/parties/party_samples.csv":
        file_schema = Schema.party_schema
    elif file_name == "test_data/party_address/address_samples.csv":
        file_schema = Schema.address_schema
    else:
        logger.info("The file " + file_name + " does not match any of the files names for the expected schemas")
        print("The file " + file_name + " does not match any of the files names for the expected schemas")
        sys.exit(-1)
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(file_schema) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "yyyy-mm-dd") \
        .load(file_name)
