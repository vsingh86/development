import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node src_physician
src_physician_node1670886407357 = glueContext.create_dynamic_frame.from_catalog(
    database="src_db",
    table_name="caregauge_app_core_facility_physician",
    transformation_ctx="src_physician_node1670886407357",
)

# Script generated for node src_facility
src_facility_node1670886421443 = glueContext.create_dynamic_frame.from_catalog(
    database="src_db",
    table_name="caregauge_app_core_facility_facility",
    transformation_ctx="src_facility_node1670886421443",
)



# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1671041861177 = ApplyMapping.apply(
    frame=Join_node1670886433718,
    mappings=[
        ("ehr_id", "string", "ehr_id", "string"),
        ("npi", "string", "npi", "string"),
        ("description", "string", "facility_description", "string"),
        ("last_name", "string", "last_name", "string"),
        ("middle_name", "string", "middle_name", "string"),
        ("code", "string", "facility_code", "string"),
        ("first_name", "string", "first_name", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1671041861177",
)

# Script generated for node trg_physician
trg_physician_node1670886730159 = glueContext.write_dynamic_frame.from_catalog(
    frame=ChangeSchemaApplyMapping_node1671041861177,
    database="trg_db",
    table_name="analytics_poc_physician",
    transformation_ctx="trg_physician_node1670886730159",
)

job.commit()
