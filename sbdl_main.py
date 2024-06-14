import sys

from lib import Utils
from lib.logger import Log4j
from lib.Refactor import read_file_csv
# import lib.Schemas as Schema


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Finished creating Spark Session")

    accountDF = read_file_csv(spark,"test_data/accounts/account_samples.csv")
    partiesDF = read_file_csv(spark,"test_data/parties/party_samples.csv")
    addressDF = read_file_csv(spark,"test_data/party_address/address_samples.csv")
    # accountDF.show()
    # partiesDF.show()
    # addressDF.show()

    # accountDF.write.json(spark, "test_data/test_out/accounts", mode="overwrite")

    # data = {"eventHeader":{"eventIdentifier":"c361a145-d2fc-434e-a608-9688caa6d22e","eventType":"SBDL-Contract","majorSchemaVersion":1,"minorSchemaVersion":0,"eventDateTime":"2022-09-06T20:49:03+0530"},"keys":[{"keyField":"contractIdentifier","keyValue":"6982391060"}],"payload":{"contractIdentifier":{"operation":"INSERT","newValue":"6982391060"},"sourceSystemIdentifier":{"operation":"INSERT","newValue":"COH"},"contactStartDateTime":{"operation":"INSERT","newValue":"2018-03-24T13:56:45.000+05:30"},"contractTitle":{"operation":"INSERT","newValue":[{"contractTitleLineType":"lgl_ttl_ln_1","contractTitleLine":"Tiffany Riley"},{"contractTitleLineType":"lgl_ttl_ln_2","contractTitleLine":"Matthew Davies"}]},"taxIdentifier":{"operation":"INSERT","newValue":{"taxIdType":"EIN","taxId":"ZLCK91795330413525"}},"contractBranchCode":{"operation":"INSERT","newValue":"ACXMGBA5"},"contractCountry":{"operation":"INSERT","newValue":"Mexico"},"partyRelations":[{"partyIdentifier":{"operation":"INSERT","newValue":"9823462810"},"partyRelationshipType":{"operation":"INSERT","newValue":"F-N"},"partyRelationStartDateTime":{"operation":"INSERT","newValue":"2019-07-29T06:21:32.000+05:30"},"partyAddress":{"operation":"INSERT","newValue":{"addressLine1":"45229 Drake Route","addressLine2":"13306 Corey Point","addressCity":"Shanefort","addressPostalCode":"77163","addressCountry":"Canada","addressStartDate":"2019-02-26"}}}]}}
    # df = spark.createDataFrame([data], schema=Schema.results_schema)
    # df.show()
    # df.coalesce(1).write.json("test_data/test_out/results_test", mode="overwrite")
