import sys

from lib import Utils
from lib.logger import Log4j
from lib.Refactor import read_file_csv
from lib.DatabaseOperations import db_check_create, table_create_insert
# import lib.Schemas as Schema


if __name__ == '__main__':
    job_run_env, load_date = Utils.arg_check(sys.argv)

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Finished creating Spark Session")

    accountDF = read_file_csv(spark,"test_data/accounts/account_samples.csv")
    partiesDF = read_file_csv(spark,"test_data/parties/party_samples.csv")
    addressDF = read_file_csv(spark,"test_data/party_address/address_samples.csv")
    # accountDF.show()
    # partiesDF.show()
    # addressDF.show()

    # Testing writing a json file
    # accountDF.write.json(spark, "test_data/test_out/accounts", mode="overwrite")

    # Testing creating a json file with nested objects
    # data = {"eventHeader":{"eventIdentifier":"c361a145-d2fc-434e-a608-9688caa6d22e","eventType":"SBDL-Contract","majorSchemaVersion":1,"minorSchemaVersion":0,"eventDateTime":"2022-09-06T20:49:03+0530"},"keys":[{"keyField":"contractIdentifier","keyValue":"6982391060"}],"payload":{"contractIdentifier":{"operation":"INSERT","newValue":"6982391060"},"sourceSystemIdentifier":{"operation":"INSERT","newValue":"COH"},"contactStartDateTime":{"operation":"INSERT","newValue":"2018-03-24T13:56:45.000+05:30"},"contractTitle":{"operation":"INSERT","newValue":[{"contractTitleLineType":"lgl_ttl_ln_1","contractTitleLine":"Tiffany Riley"},{"contractTitleLineType":"lgl_ttl_ln_2","contractTitleLine":"Matthew Davies"}]},"taxIdentifier":{"operation":"INSERT","newValue":{"taxIdType":"EIN","taxId":"ZLCK91795330413525"}},"contractBranchCode":{"operation":"INSERT","newValue":"ACXMGBA5"},"contractCountry":{"operation":"INSERT","newValue":"Mexico"},"partyRelations":[{"partyIdentifier":{"operation":"INSERT","newValue":"9823462810"},"partyRelationshipType":{"operation":"INSERT","newValue":"F-N"},"partyRelationStartDateTime":{"operation":"INSERT","newValue":"2019-07-29T06:21:32.000+05:30"},"partyAddress":{"operation":"INSERT","newValue":{"addressLine1":"45229 Drake Route","addressLine2":"13306 Corey Point","addressCity":"Shanefort","addressPostalCode":"77163","addressCountry":"Canada","addressStartDate":"2019-02-26"}}}]}}
    # df = spark.createDataFrame([data], schema=Schema.results_schema)
    # df.show()
    # df.coalesce(1).write.json("test_data/test_out/results_test", mode="overwrite")

    # Testing checking if a DB exists
    # print(spark.catalog.databaseExists("SBDL"))

    # Checking if my DB exists, if not create it
    # In both cases, the current database is then set to my DB
    db_check_create(spark,"SBDL")
    table_create_insert(spark, "account", accountDF)
    table_create_insert(spark, "party", partiesDF)
    table_create_insert(spark, "address", addressDF)
    # newdf = spark.sql("select * from account")
    # newdf.show()

    join_expr1 = accountDF.account_id == partiesDF.account_id
    join_expr2 = addressDF.party_id == partiesDF.party_id
    outputDF = accountDF.join(partiesDF, join_expr1, "inner").join(addressDF, join_expr2, "inner").drop(partiesDF.account_id,partiesDF.party_id)
    outputDF.show()

