-- Okay now lets make some summary stats so that the boss man can 
-- decide which products are hottest in which states. 

-- FYI...
-- If you run into errors, you can see them in 
-- ./target/failsafe-reports/TEST-org.bigtop.bigpetstore.integration.BigPetStorePigIT.xml

-- First , we load data in from a file, as tuples.
-- in pig, relations like tables in a relational database
-- so each relation is just a bunch of tuples.
-- in this case csvdata will be a relation, 
-- where each tuple is a single petstore transaction. 
csvdata = 
    LOAD 'bps_integration_/CLEANED' using PigStorage() 
        AS (
          dump:chararray, 
          state:chararray, 
          transaction:int, 
          fname:chararray, 
          lname:chararray, 
          date:chararray, 
          price:float, 
          product:chararray);
          
-- RESULT: 
-- (BigPetStore,storeCode_AK,1,jay,guy,Thu Dec 18 12:17:10 EST 1969,10.5,dog-food) 
-- ...

-- Okay! Now lets group our data so we can do some stats.
-- lets create a new relation, 
-- where each tuple will contain all transactions for a product in a state.

state_product = group csvdata by ( state, product ) ;

-- RESULT
-- ((storeCode_AK,dog-food) , {(BigPetStore,storeCode_AK,1,jay,guy,Thu Dec 18 12:17:10 EST 1969,10.5,dog-food)}) --
-- ...
  
  
-- Okay now lets make some summary stats so that the boss man can 
-- decide which products are hottest in which states. 

summary1 = FOREACH state_product generate group as sp, COUNT($1); 

dump state_product;
dump summary1;

store summary1 into 'bps_integration_/BPS_TEST_PIG_COUNT_PRODUCTS';
