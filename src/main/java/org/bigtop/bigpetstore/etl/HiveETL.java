package org.bigtop.bigpetstore.etl;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.bigtop.bigpetstore.contract.PetStoreStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Note on running locally:
 * 
 * 1) Local mode requires a hive and hadoop tarball, with HIVE_HOME and
 * HADOOP_HOME pointing to it. 2) In HADOOP_HOME, you will need to cp the
 * HIVE_HOME/lib/hive-serde*jar file into HADOOP_HOME/lib.
 * 
 * Then, the below queries will run.
 * 
 * The reason for this is that the hive SerDe stuff is used in the MapReduce
 * phase of things, so those utils need to be available to hadoop itself. That
 * is because the regex input/output is processed vthe mappers
 * 
 */
public class HiveETL extends PetStoreStatistics {

    public static final String HIVE_JDBC_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
    public static final String HIVE_JDBC_EMBEDDED_CONNECTION = "jdbc:hive://";
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    final static Logger log = LoggerFactory
            .getLogger(HiveETL.class);

    public HiveETL(Path pathToRawInput) throws Exception {
        super();
        Statement stmt = getConnection();
        /**
         * Create an external table so we dont accidentally delete the raw data.
         * ResultSet res = stmt.executeQuery(
         * "CREATE EXTERNAL TABLE InputData (code STRING, transaction STRING) ROW FORMAT "
         * + "DELIMTED FIELDS TERMINATED BY '\t' " + "LINES TERMINATED BY '\n' "
         * + "STORED AS TEXTFILE " + "LOCATION "+rawInput);
         */

        /**
         * * BigPetStore,storeCode_OK,2 yang,jay,Mon Dec 15 23:33:49 EST
         * 1969,69.56,flea collar
         * 
         * ("BigPetStore,storeCode_OK,2",
         * "yang,jay,Mon Dec 15 23:33:49 EST 1969,69.56,flea collar")
         */
        // TODO ^^ load this in from the first input (string[0]);
        stmt.execute("DROP TABLE hive_bigpetstore_etl");

        String create = "CREATE TABLE hive_bigpetstore_etl ("
                + "  state STRING,"
                + "  trans_id STRING,"
                + "  lname STRING,"
                + "  fname STRING,"
                + "  date STRING,"
                + "  price STRING,"
                + "  product STRING"
                + ")"
                + " ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' "
                + "WITH SERDEPROPERTIES  ("
                + "\"input.regex\" = \"INPUT_REGEX\" , "
                + "\"output.format.string\" = \"%1$s %2$s %3$s %4$s %5$s\") "
                + "STORED AS TEXTFILE";
        // \\d+ seems to lose its delimiters when sent to hive ? so opt for
        // another regex
        // create=create.replaceAll("INPUT_REGEX",
        // ".*_([A-Z][A-Z]),(\\d+)\\s+([a-z]*),([a-z]*),([^,]*),([^,]*),([^,]*).*");
        create = create.replaceAll("INPUT_REGEX", "(?:BigPetStore,storeCode_)"
                + "([A-Z][A-Z])," + // state (CT)
                "([0-9]*)" + // state transaction id (1)
                "(?:\t)" + // [tab]
                "([a-z]*)," + // fname (jay)
                "([a-z]*)," + // lname (vyas)
                "([A-Z][^,]*)," + // date starts with capital letter (MWTFSS)
                "([^,]*)," + // price (12.19)
                "([^,]*).*"); // product (premium cat food)
        System.out.println(create);
        ResultSet res = stmt.executeQuery(create);

        res = stmt
                .executeQuery("LOAD DATA INPATH '<rawInput>' INTO TABLE hive_bigpetstore_etl"
                        .replaceAll("<rawInput>", pathToRawInput.toString()));

    }

    private Statement getConnection() throws ClassNotFoundException,
            SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection con = DriverManager.getConnection(
                HIVE_JDBC_EMBEDDED_CONNECTION, "", "");

        Statement stmt = con.createStatement();
        return stmt;
    }


    /**
     * Takes path of transaction data and returns statistics.
     * 
     * @param
     * @return
     */
    public Map<String, Integer> numberOfProductsByProduct() throws Exception{
        Statement stmt = getConnection();
        ResultSet res = 
                stmt.executeQuery(
                        "select product,count(*) as cnt " +
                		"from hive_bigpetstore_etl group by product");
        stmt.close();
        return convert("product",res);
    }

    public Map<String, Integer> numberOfTransactionsByState() throws Exception{
        Statement stmt = getConnection();
        ResultSet res = 
                stmt.executeQuery("select state,count(*) as cnt from hive_bigpetstore_etl group by state");
        stmt.close();
        return convert("state",res);
    }

    /**
     * input : field name (i.e. "state" or "product") which is 
     * 
     * Some utility methods to transform list of tuples into Map.  
     * 
     * i.e. 
     * 
     * [AZ,3],[OK,2],[CA,8]
     * 
     * into 
     * 
     * AZ -> 3
     * OK -> 2 
     * CA -> 8
     */
    
    public static Map<String, Integer> convert(String field,ResultSet r) throws Exception {
        Map<String, Integer> converted = Maps.newHashMap();
        for (Map m : resultSetToArrayList(r)) {
            converted.put((String) m.get(field), ((Number) m.get("cnt")).intValue());
        }
        return converted;
    }

    public static List<Map> resultSetToArrayList(ResultSet rs)
            throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ArrayList<Map> list = new ArrayList<Map>(50);
        while (rs.next()) {
            HashMap row = new HashMap(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(md.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }

        return list;
    }
 
}