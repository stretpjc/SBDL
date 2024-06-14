from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, TimestampType, LongType, \
    DoubleType, ArrayType

# Input file schemas:
address_schema = StructType([
    StructField("load_date", DateType()),
    StructField("party_id", LongType()),
    StructField("address_line_1", StringType()),
    StructField("address_line_2", StringType()),
    StructField("city", StringType()),
    StructField("postal_code", IntegerType()),
    StructField("country_of_address", StringType()),
    StructField("address_start_date", DateType())
])

account_schema = StructType([
    StructField("load_date", DateType()),
    StructField("active_ind", IntegerType()),
    StructField("account_id", LongType()),
    StructField("source_sys", StringType()),
    StructField("account_start_date", TimestampType()),
    StructField("legal_title_1", StringType()),
    StructField("legal_title_2", StringType()),
    StructField("tax_id_type", StringType()),
    StructField("tax_id", StringType()),
    StructField("branch_code", StringType()),
    StructField("country", StringType())
])

party_schema = StructType([
    StructField("load_date", DateType()),
    StructField("account_id", DoubleType()),
    StructField("party_id", LongType()),
    StructField("relation_type", StringType()),
    StructField("relation_start_date", TimestampType())
])


#Output file schemas
event_header_schema = StructType([
    StructField("eventIdentifier", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("majorSchemaVersion", StringType(), True),
    StructField("minorSchemaVersion", StringType(), True),
    StructField("eventDateTime", StringType(), True)
])

key_schema = ArrayType(StructType([
    StructField("keyField", StringType(), True),
    StructField("keyValue", StringType(), True)
]))

contract_title_schema = ArrayType(StructType([
    StructField("contractTitleLineType", StringType(), True),
    StructField("contractTitleLine", StringType(), True)
]))

tax_identifier_schema = StructType([
    StructField("taxIdType", StringType(), True),
    StructField("taxId", StringType(), True)
])

party_address_schema = StructType([
    StructField("addressLine1", StringType(), True),
    StructField("addressLine2", StringType(), True),
    StructField("addressCity", StringType(), True),
    StructField("addressPostalCode", StringType(), True),
    StructField("addressCountry", StringType(), True),
    StructField("addressStartDate", StringType(), True)
])

party_relations_schema = ArrayType(StructType([
    StructField("partyIdentifier", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("partyRelationshipType", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("partyRelationStartDateTime", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("partyAddress", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", party_address_schema, True)
    ]), True)
]))

payload_schema = StructType([
    StructField("contractIdentifier", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("sourceSystemIdentifier", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("contactStartDateTime", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("contractTitle", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", contract_title_schema, True)
    ]), True),
    StructField("taxIdentifier", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", tax_identifier_schema, True)
    ]), True),
    StructField("contractBranchCode", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("contractCountry", StructType([
        StructField("operation", StringType(), True),
        StructField("newValue", StringType(), True)
    ]), True),
    StructField("partyRelations", party_relations_schema, True)
])

results_schema = StructType([
    StructField("eventHeader", event_header_schema, True),
    StructField("keys", key_schema, True),
    StructField("payload", payload_schema, True)
])