# Project: Data Lake Project from Udacity's Data Engineer 

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building a Data Lake that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in order to improve the recommendation system which is the core of the sparkify's app.

## Discussion 

Data lakes are core components in data science, according to databricks a data lake is centralized data repository that is capable of storing both structured data and non-structured data such as video or audio. Data lakes are often used to consolidate all of an organization's data in a central location, where it can be saved as is, without the need to impose a schema and  

* Statistics about songs, users, and artists
* Predict customer churn 
* Build recommendation systems 

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
* Queries run faster. 
* Enabling dimensional tables to be easily updated
* Enabling new facts to be easily added i.e. regularly or selectively


## References 

http://gkmc.utah.edu/ebis_class/2003s/Oracle/DMB26/A73318/schemas.htm

https://www.quora.com/What-does-a-star-schema-do-What-purpose-does-it-serve
