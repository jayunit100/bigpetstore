csvdata = LOAD './d3/cities.csv'using PigStorage(',');
DUMP csvdata;

