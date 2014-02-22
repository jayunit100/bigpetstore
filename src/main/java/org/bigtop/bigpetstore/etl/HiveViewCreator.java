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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.bigtop.bigpetstore.contract.PetStoreStatistics;
import org.bigtop.bigpetstore.util.BigPetStoreConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.protobuf.UnknownFieldSet.Field;

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
public class HiveViewCreator implements Tool {
    
    Configuration conf;
    @Override
    public void setConf(Configuration conf) {
        this.conf=conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Statement stmt = getConnection();
        stmt.execute("DROP TABLE IF EXISTS " + BigPetStoreConstants.OUTPUTS.MAHOUT_CF_IN.name());
        System.out.println("input data " + args[0]);
        System.out.println("output table " + args[1]);
        
        Path inTablePath =  new Path(args[0]);
        String inTableName = "cleaned"+System.currentTimeMillis();
        String outTableName = BigPetStoreConstants.OUTPUTS.MAHOUT_CF_IN.name();

        final String create = "CREATE EXTERNAL TABLE "+inTableName+" ("
                + "  state STRING,"
                + "  trans_id STRING,"
                + "  lname STRING,"
                + "  fname STRING,"
                + "  date STRING,"
                + "  price STRING,"
                + "  product STRING"
                + ") ROW FORMAT "
                + "DELIMITED FIELDS TERMINATED BY '\t' "
                + "LINES TERMINATED BY '\n' "
                + "STORED AS TEXTFILE "
                + "LOCATION '"+inTablePath+"'";
        
        ResultSet res = stmt.executeQuery(create);

        //will change once we add hashes into pig ETL clean
        String create2 = 
                "create table "+outTableName+" as select state from "+inTableName;
        System.out.println("out table= " + create2  );
        res = stmt.executeQuery(create2);
    
        System.out.println("result = "+res.first());

        return 0;
    }

    public static final String HIVE_JDBC_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
    public static final String HIVE_JDBC_EMBEDDED_CONNECTION = "jdbc:hive://";
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    final static Logger log = LoggerFactory.getLogger(HiveViewCreator.class);


    private Statement getConnection() throws ClassNotFoundException,
            SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection con = DriverManager.getConnection(
                HIVE_JDBC_EMBEDDED_CONNECTION, "", "");

        Statement stmt = con.createStatement();
        return stmt;
    }
    
    public static void main(String[] args) throws Exception {
        new HiveViewCreator()
            .run(args);
    }
}