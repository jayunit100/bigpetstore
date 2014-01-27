csvdata = LOAD '/tmp/csvdata' USING org.bigtop.bigpetstore.pigudf.LegacyPigCSVLoader()  AS (csvMap:map[]);
DUMP csvdata;

