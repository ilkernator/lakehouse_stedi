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

# Script generated for node S3 - Acceleromater_Landing
S3Acceleromater_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3Acceleromater_Landing_node1",
)

# Script generated for node S3 - Customer Trusted
S3CustomerTrusted_node1695065867448 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/customer/security-zone/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerTrusted_node1695065867448",
)

# Script generated for node Join
Join_node1695065789853 = Join.apply(
    frame1=S3Acceleromater_Landing_node1,
    frame2=S3CustomerTrusted_node1695065867448,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1695065789853",
)

# Script generated for node Drop Fields
DropFields_node1695066327195 = DropFields.apply(
    frame=Join_node1695065789853,
    paths=["user", "timeStamp", "x", "y", "z", "email", "phone", "birthday"],
    transformation_ctx="DropFields_node1695066327195",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1695675010651 = DynamicFrame.fromDF(
    DropFields_node1695066327195.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1695675010651",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1695675010651,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://aws-nanodegree-test-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
