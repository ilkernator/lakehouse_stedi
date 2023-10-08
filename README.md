# Project Overview
The STEDI Team has been developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps
- has a companion mobile app that collects customer data and interacts with the device sensors

Using AWS Glue, AWS S3, Python, and Spark, this project aims to extract 
the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution 
on AWS so that Data Scientists can train the learning model. This project was part of the 
Nanodegree Data Engineering Program offered by Udacity.

# Project Data
STEDI has three JSON data sources to use from the Step Trainer:
1. customer
2. step_trainer
3. accelerometer

The data was stored in respective S3 Buckets to leverage AWS services when building up the lakehouse solution.

# Files
- `customer_landing_to_trusted.py`: Spark Job sanitizing the customer data from the website (Landing Zone) and
only stores the customer records who agreed to share their data for research purposes (Trusted Zone) - creating a
Glue Table called customer_trusted

- `accelerometer_landing_to_trusted.py`: Spark Job sanitizing the accelerometer data from the Mobile App (Landing Zone)
and only stores accelerometer readings from customers who agreed to share their data for research purposes (Trusted Zone) -
creating a Glue Table called accelerometer_trusted

- `customer_trusted_to_curated.py`: Spark Job sanitizing the customer data (Trusted Zone) and created a Glue Table (Curated Zone)
that only includes customers who have accerlometer data and have agreed to share their data for research called customers_curated

- `step_trainer_landing_to_trusted.py`: Spark Job reading the step trainer IoT data stream and populating it to a Trusted Zone
Glue Table called step_trainer_trusted containing the step trainer records data for customers who have accelerometer data and
have agreed to share their data for research (customers_curated)

- `machine_learning_to_curated.py`: Spark Job creating an aggregated table that has each of the step trainer readings, and the
associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and
populaties a glue table called machine_learning_curated

- `customer_landing.sql`, `accelerometer_landing.sql`: DDLs for the creation of the raw tables for accelerometer and customer
data

- `accelerometer_landing.png`, `customer_landing.png`, `customer_trusted.png`: Screenshots giving an impression of the data
resulting from the Spark Jobs

# Note
The curated tables have been anonymized to not be subject to GDPR or other privacy regulations.
