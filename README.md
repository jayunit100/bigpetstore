BigPetStore: Apache Bigtop/Hadoop Ecosystem Demo
===============================================
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

Build Instructions
------------------

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

  *Number of transactions by state*
  *Most valuable customer by state*
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





    