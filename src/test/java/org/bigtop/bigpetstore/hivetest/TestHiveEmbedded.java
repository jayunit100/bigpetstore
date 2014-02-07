package org.bigtop.bigpetstore.hivetest;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.bigtop.bigpetstore.etl.HiveETL;
import org.bigtop.bigpetstore.generator.PetStoreJob;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHiveEmbedded {

    final static Logger log = LoggerFactory.getLogger(TestHiveEmbedded.class);

    public static final long ID = System.currentTimeMillis();
    public static final String test_data_directory = "/tmp/BigPetStore" + ID;
    public static final String HIVE_JDBC_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
    public static final String HIVE_JDBC_EMBEDDED_CONNECTION = "jdbc:hive://";
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    @Test
    public void testPetStorePipeline() throws Exception {
        int records = 10;
        /**
         * Setup configuration with prop.
         */
        Configuration conf = new Configuration();
        conf.setInt(PetStoreJob.props.bigpetstore_records.name(), records);

        Path raw_generated_data = new Path(test_data_directory, "generated");

        Job createInput = PetStoreJob.createJob(raw_generated_data, conf);
        createInput.waitForCompletion(true);
        JobID jId = createInput.getJobID();

        Path outputfile = new Path(raw_generated_data, "part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf)
                .pathToFile(outputfile), Charset.defaultCharset());
        System.out.println("output : "
                + FileSystem.getLocal(conf).pathToFile(outputfile));
        for (String l : lines) {
            System.out.println(l);

        }

        Statement stmt = getConnection();
        runHive(stmt, raw_generated_data, conf);

    }

    private void runHive(Statement stmt, Path pathToRawInput, Configuration conf)
            throws Exception {

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

        // res =
        // stmt.executeQuery("select * from hive_bigpetstore_etl group by state");

        res = stmt.executeQuery("select * from hive_bigpetstore_etl");

        List<Map> resultList = resultSetToArrayList(res);

        Map<String, Integer> converted = Maps.newHashMap();
        int k = 0;
        for (Map m : resultList) {
            converted.put((String) m.get("product"), ((Integer) k++));

        }
        log.info(converted.toString());
    } //

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

    public static Map<String, Integer> convert(String field, ResultSet r)
            throws Exception {
        Map<String, Integer> converted = Maps.newHashMap();
        for (Map m : resultSetToArrayList(r)) {
            converted.put((String) m.get(field),
                    ((Number) m.get("cnt")).intValue());
        }
        return converted;
    }

    private Statement getConnection() throws ClassNotFoundException,
            SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection con = DriverManager.getConnection(
                HIVE_JDBC_EMBEDDED_CONNECTION, "", "");

        Statement stmt = con.createStatement();
        return stmt;
    }

}
