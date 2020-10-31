# Graphical Tool for Generating NoSQL Benchmarks (GT-GNB)
********************************
The GT-GNB fits as a NoSQL benchmark generation tool which was designed and developed to simulate and assess a multidimensional star data warehouse. 
It uses the SSB data model which is a star schema consisting of a single fact table and four dimension tables. 
Recall, this model manages the line order according to the dimensions of products, suppliers, customers and date
Several aspects are supported by this tool, when generating data, such as the format of the data and the volume of data generated.

I.	Description
The graphical tool for generating NoSQL benchmarks includes three components, namely; 

(1) the data generator (DBGen) which is based on the MapReduce paradigm to parallelize the processing and assign the calculation tasks over a cluster made up of several machines;

(2) the data loading tool (LoadDB) offering the possibility of loading the data generated in a NoSQL DBMS; 

(3) the query generator (QGen) which generates a set of queries allowing to perform evaluation tests.

The GT-GNB functionalities are summarized as follows.

II.	Setup Tutorial
This tool requires a software environment for its operation.

1. Install Java
   •	Download Java (the version 8 is recommended)
   •	Add the "JAVA_HOME" and "PATH" environment variables

2. Install Scala
  •	Download Scala
  •	Add the "SCALA_HOME" and "PATH" environment variables

3. Installation of Spark applications
  •	Download spark
  •	Download hadoop
  •	Add the "HADOOP_HOME" and "SPARK_HOME" and "PATH" environment variables
  •	Configure Hadoop

4. Intellij installation
  •	Download Intellij
  •	Install additional plugins

5. Installation MongoDB and Mongo Plugin
  •	Download MonogoDB
  •	Install Mongo Plugin in Intellij
  •	Configure Mongo Plugin in Intellij
