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

# Script generated for node S3 Accelerometer Trusted
S3AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3AccelerometerTrusted_node1",
)

# Script generated for node S3 Step Trainer Trusted
S3StepTrainerTrusted_node1696777406056 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3StepTrainerTrusted_node1696777406056",
)

# Script generated for node S3 Customer Curated
S3CustomerCurated_node1696777244169 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-nanodegree-test-bucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1696777244169",
)

# Script generated for node Left Join
S3CustomerCurated_node1696777244169DF = S3CustomerCurated_node1696777244169.toDF()
S3AccelerometerTrusted_node1DF = S3AccelerometerTrusted_node1.toDF()
LeftJoin_node1696777300744 = DynamicFrame.fromDF(
    S3CustomerCurated_node1696777244169DF.join(
        S3AccelerometerTrusted_node1DF,
        (
            S3CustomerCurated_node1696777244169DF["email"]
            == S3AccelerometerTrusted_node1DF["user"]
        ),
        "left",
    ),
    glueContext,
    "LeftJoin_node1696777300744",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696777556352 = ApplyMapping.apply(
    frame=S3StepTrainerTrusted_node1696777406056,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("distanceFromObject", "int", "right_distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696777556352",
)

# Script generated for node Drop Fields
DropFields_node1696777365499 = DropFields.apply(
    frame=LeftJoin_node1696777300744,
    paths=["user"],
    transformation_ctx="DropFields_node1696777365499",
)

# Script generated for node Inner Join
InnerJoin_node1696777454949 = Join.apply(
    frame1=DropFields_node1696777365499,
    frame2=RenamedkeysforJoin_node1696777556352,
    keys1=["serialNumber", "timeStamp"],
    keys2=["right_serialNumber", "sensorReadingTime"],
    transformation_ctx="InnerJoin_node1696777454949",
)

# Script generated for node Drop Fields
DropFields_node1696777646133 = DropFields.apply(
    frame=InnerJoin_node1696777454949,
    paths=["right_serialNumber", "timeStamp", "email", "phone", "birthday"],
    transformation_ctx="DropFields_node1696777646133",
)

# Script generated for node Rename Field
RenameField_node1696777685693 = RenameField.apply(
    frame=DropFields_node1696777646133,
    old_name="right_distanceFromObject",
    new_name="distanceFromObject",
    transformation_ctx="RenameField_node1696777685693",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696777710416 = DynamicFrame.fromDF(
    RenameField_node1696777685693.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696777710416",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696777710416,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://aws-nanodegree-test-bucket/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node2",
)

job.commit()
