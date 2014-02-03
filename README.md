This is a big pet store
=======================
Build Instructions for maven profiles

  - Run Intergration tests with
  - Pig profile clean verify -P pig
  - Hive profile clean verify -P hive



* it is not  a real pet store

* it can only be understood by using hadoop

* it will be the shining beacon on the NoSQL hill


High level summary
==================


The bigpetstore project exemplifies the hadoop ecosystem for newcomers, and also for benchmarking and comparing functional space of tools.

MAPREDUCE

1) First, we generate a input data set of variable size of pet store transactions... each with location, name, price, etc.. (exact details
are changing and in the code).

PIG/HIVE/PANGOOL/CASCALOG

2) Then we use pig and hive to do identical transformations on that data to create meaningfull aggregations that
a data analyst or scientist might use for analyzing trends, picking products, assigning regional managers, etc etc..

MAHOUT/VowpalWabiit/....

3) Then, we use machine learning (i.e. mahout) to associate customers with new products, and cluster products by similarty for
designing layouts for new stores to maximize purchasing

HBASE/SOLR/...

4) Finally, we sink results of the transaction generator into NoSQL or NewSQL stores to demonstrate high throughput databases
supported via the hadoop ecosystem.

** Each of the above phases can be accomplished in many ways, and the end goal is to run many different implementations of each phase
using different tools, thus exemplifying overlap of tools in the hadoop ecosystem, and allowing people to benchmark/compare tools
using a common framework and easily understood use case **


How it works (not all done yet... let this serve as requirements for now)
=========================================================================

- Phase 1: Generating pet store data:

The first step is to generate a raw data set.  This is done by the "GeneratePetStoreTransactionsInputFormat":

BigPetStore,storeCode_AK,1	watt,john,Fri Jan 16 06:52:34 EST 1970,11.59,pet deterrent
BigPetStore,storeCode_AK,2	walbright,jim,Wed Dec 31 16:36:24 EST 1969,94.25,cat food
BigPetStore,storeCode_AK,3	jones,paul,T.....................................


The first MapReduce job in the pipeline runs a simple job which takes this input format and forwards
its output.  The result is a list of "transactions".  Each transaction is a tuple of the format

{state,name,date,price,product}.

- Phase 2: Processing the data

The next phase of the application processes the data to create basic aggregations.

  - Number of transactions by state
  - Most valuable customer by state
  - Most popular items by state

This is done in both pig and hive.  The output is a JSON file:

{
  "AZ":{
	"transactions":3,
        "best_customer":"",
	"best_item":""
	}
  ...
}

- Phase 3: Clustering the states by all feilds

  Based on the aggregations above, we have 50 states, each of which has
	- a # of transactions
	- a most popular item

  Now, say we want to cluster the states, so as to put different states into different buying categories
  for our marketing team to deal with differently.

  We run a k-means (k=4) over all states using a distance metric which uses transactions
  and most popular item.  The end result is grouping of our states into different store "types".

- Phase 4: Visualizing the Data in D3.

  Again, we can take the aggregated JSON above and do more with it :  This time, we will visualize it in D3.
  To do this, we need to output longitudinal / lattitude on to a map of the US, and embed the json values
  into heat coloring and tool tips.


Understanding the code
=======================

To understand this project, it is suggested to start with the integration test libraries.

1) run "mvn eclipse:eclipse" to create an IDE loadable project.

2) open .classpath and add
    `<classpathentry kind="src" path="src/integration/java" including="**/*.java"/>`

3) import the project into eclipse, andgo the the integration test ITBigPetStore.java class.





To run it
=========

- mvn clean verify

  This will run the unit tests and the main tests - which are the integration tests.



    