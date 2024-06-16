from lib.logger import Log4j
import lib.Schemas as Schema


def db_check_create(spark, database):
    logger = Log4j(spark)
    if not spark.catalog.databaseExists("SBDL"):
        logger.info("Database " + database + " does not exist, creating")
        spark.sql("CREATE DATABASE IF NOT EXISTS " + database)
    logger.info("Setting current database to " + database)
    spark.catalog.setCurrentDatabase(database)


def table_create_insert(spark, table, dataframe):
    logger = Log4j(spark)
    if not spark.catalog.tableExists(table):
        logger.info("Table " + table + " does not exist, creating")
        empty_df = spark.createDataFrame([], Schema.select_schema(table))
        empty_df.write.mode("overwrite").saveAsTable(table)
        # Fix this
        # constraint = Schema.choose_constraint(table)
        # logger.info("Adding unique constraint to column " + constraint + " for table " + table)
        # alter_sql = "ALTER TABLE " + table + " ADD CONSTRAINT table_key UNIQUE (" + constraint + ");"
        # spark.sql(alter_sql)
    logger.info("Inserting data into table " + table)
    dataframe.write.mode('append').saveAsTable(table)
