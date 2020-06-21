# Project: Data Lake Project from Udacity's Data Engineer 

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building a Data Lake that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in order to improve the recommendation system which is the core of the sparkify's app.

## Files 

[dl.cfg](https://raw.githubusercontent.com/ricardoues/data-lake/master/dl.cfg)

[etl.py](https://raw.githubusercontent.com/ricardoues/data-lake/master/etl.py)

## Discussion 

Data lakes are core components in data science, according to databricks a data lake is centralized data repository that is capable of storing both structured data and non-structured data such as video or audio. Data lakes are often used to consolidate all of an organization's data in a central location, where it can be saved as is, without the need to impose a schema, it can be very convenient in advanced analytics application such as natural language processing or computer vision. The advantages of using Data Lakes are follows:  

* Low cost
* Flexibility 
* Scalability
* Allows storage of the raw data needed for machine learning and deep learning applications


The schema includes the following tables: 

* Fact Table
    * songplays - records in event data associated with song plays.
* Dimension Tables
    * users - users in the app 
    * songs - songs in music database 
    * artists - artists in music database 
    * time - timestamps of records 


The above star schema was designed in this way due to the massive amount of data in the Sparkify app. The advantanges of this schema are shown as follows:

* Easily understood
* Queries run faster since that there is no need to join more than two dataframes. 
* Enabling dimensional tables to be easily updated
* Enabling new facts to be easily added i.e. regularly or selectively

## How to run the code 

* Sign in for AWS services, go to [Amazon EMR Console](https://console.aws.amazon.com/elasticmapreduce/)
* Select "Clusters" in the menu on the left, and click the "Create cluster" button.
* Release: emr-5.20.0
* Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
* Instance type: m5.xlarge
* Number of instance: 6

I suggest to proceed with an EC2 key pair. Wait until the cluster has the following status: Waiting before moving on to the next step. After that connect to the master node using SSH and run the following commands in the terminal: 

```bash
sudo cp /etc/spark/conf/log4j.properties.template /etc/spark/conf/log4j.properties
sudo sed -i 's/log4j.rootCategory=INFO, console/log4j.rootCategory=ERROR,console/' /etc/spark/conf/log4j.properties
```

Go to /etc/spark/conf/ and run the following commands in the terminal:

```bash
sudo cp spark-env.sh spark-env.sh.bkp
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
```

Next, we install configparser and pandas python packages with the following command: 

```bash
sudo python3 -m  pip install configparser pandas
```

Finally, we will clone the repository and submit the spark script with the following commands: 

```bash
git clone https://github.com/ricardoues/data-lake.git
/usr/bin/spark-submit --verbose  --master yarn  etl.py 
```

The spark script takes around 2 hours to finish.


**Note**: You must switch your zone to us-west-2 because this might be a restriction access to the log files in S3 bucket.



## References 

https://databricks.com/discover/data-lakes/introduction
https://www.datasciencecentral.com/profiles/blogs/what-are-data-lakes
https://www.ibmbigdatahub.com/blog/charting-data-lake-using-data-models-schema-read-and-schema-write
