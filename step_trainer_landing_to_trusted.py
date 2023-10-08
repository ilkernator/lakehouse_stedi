import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Steptrainer Landing
S3SteptrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3SteptrainerLanding_node1",
)

# Script generated for node S3 Customer Curated
S3CustomerCurated_node1696767821459 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1696767821459",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696768148492 = ApplyMapping.apply(
    frame=S3CustomerCurated_node1696767821459,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696768148492",
)

# Script generated for node Join
Join_node1696767889440 = Join.apply(
    frame1=S3SteptrainerLanding_node1,
    frame2=RenamedkeysforJoin_node1696768148492,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1696767889440",
)

# Script generated for node Drop Fields
DropFields_node1696768213331 = DropFields.apply(
    frame=Join_node1696767889440,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
    ],
    transformation_ctx="DropFields_node1696768213331",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696768245161 = DynamicFrame.fromDF(
    DropFields_node1696768213331.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696768245161",
)

# Script generated for node S3 Steptrainer Trusted
S3SteptrainerTrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696768245161,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://aws-nanodegree-test-bucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3SteptrainerTrusted_node2",
)

job.commit()
