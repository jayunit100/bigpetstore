BigPetStore
============
Apache Bigtop/Hadoop Ecosystem Demo
-----------------------------------
This software is created to demonstrate Apache Bigtop for processing
big data sets.

Architecture
------------
The application consists of the following modules

* generator: generates raw data on the dfs
* clustering: Apache Mahout demo code for processing the data using Itembased Collaborative Filtering
* Pig: demo code for processing the data using Apache Pig
* Hive: demo code for processing the data using Apache Hive demo code
* Crunch: demo code for processing the data using Apache Crunch

Setup
-----

there are no box specific set-up requirements for the main build
however to run the hive profile follow the instructions below

###Setup for hive code
####Basically need to get hive-contrib-0.12.0.jar and hive-contrib-0.12.0.jar on the hadoop classpath at runtime

here is one possible  way to do this  
1) download and unpack hadoop-2.2.0.tar.gz then create a hive home directory and HIVE_HOME environment variable  
  
wget  http://apache.cbox.biz/hadoop/common/stable2/hadoop-2.2.0.tar.gz  
tar -xvf hadoop-2.2.0.tar.gz  
mv hadoop-2.2.0 hadoop  
export HADOOP_HOME=/{path to home dir}/hadoop

2) download and unpack  hive-0.12.0.tar.gz then put the following jars to the hadoop lib directory

wget  http://apache.cbox.biz/hive/hive-0.12.0.tar.gz  
tar -xvf hive-0.12.0.tar.gz  
mv hive-0.12.0 hive  
cp /{path to home dir}/hive/lib/hive-contrib-0.12.0.jar /{path to home dir}/hadoop/lib/
cp /{path to home dir}/hive/lib/hive-serde-0.12.0.jar /{path to home dir}/hadoop/lib/


Build Instructions
------------------

the hive code requires a HADOOP_HOME variable to be set

mvn clean package will build the bigpetstore jar

due to classpath and dependency issues it is necessary to run the Hive and Pig classes
as encapsulated processes
this is achieved by separate integration tests running in separate maven profiles

Run Intergration tests with

  * Pig profile: mvn clean verify -P pig
  * Hive profile: mvn clean verify -P hive


High level summary
------------------


The bigpetstore project exemplifies the hadoop ecosystem for newcomers, and also for benchmarking and
comparing functional space of tools.

The end goal is to run many different implementations of each phase
using different tools, thus exemplifying overlap of tools in the hadoop ecosystem, and allowing people to benchmark/compare tools
using a common framework and easily understood use case


How it works (To Do)
--------------------

* Phase 1: Generating pet store data:

The first step is to generate a raw data set.  This is done by the "GeneratePetStoreTransactionsInputFormat":

The first MapReduce job in the pipeline runs a simple job which takes this input format and forwards
its output.  The result is a list of "transactions".  Each transaction is a tuple of the format

  *{state,name,date,price,product}.*


* Phase 2: Processing the data

The next phase of the application processes the data to create basic aggregations.
For example with both pig and hive these could easily include

  *Number of transactions by state* or
  *Most valuable customer by state* or
  *Most popular items by state*


* Phase 3: Clustering the states by all feilds

  Now, say we want to cluster the states, so as to put different states into different buying categories
  for our marketing team to deal with differently.

* Phase 4: Visualizing the Data in D3.


For Eclipse Users
-----------------


1) run "mvn eclipse:eclipse" to create an IDE loadable project.

2) open .classpath and add
    `<classpathentry kind="src" path="src/integration/java" including="**/*.java"/>`

3) import the project into eclipse





