package org.bigtop.bigpetstore.integration;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.bigtop.bigpetstore.etl.HiveETL;
import org.bigtop.bigpetstore.etl.PigETL;
import org.bigtop.bigpetstore.generator.PetStoreJob;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *
 */
public class ITBigPetStoreHive {

    // We need the directory label to read a data from it later
    static long ID = System.currentTimeMillis();
    String test_data_directory  =  "/tmp/BigPetStore"+ID;
    final static Logger log = LoggerFactory.getLogger(ITBigPetStoreHive.class);

    /**
     * Validates the intermediate JSON outputs from Pig and Hive
     */
    public Function<String,Boolean> ETL_JSON_VALIDATOR = 
        new Function<String, Boolean>() {
            public Boolean apply(String line){
                JSONObject jso;
                try {
                    jso = new JSONObject(line);
                    return 
                            (jso.getString("product").length()>0) && 
                            (jso.getInt("count")>0);
                } 
                catch (JSONException e) {
                    return false;
                }
            }
        };
    
    @After
    public void tearDown() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(new File(test_data_directory));
    }
    
    @Test
    public void testPetStorePipeline()  throws Exception {
        int records = 10;
        /**
         * Setup configuration with prop.
         */
        Configuration conf = new Configuration();
        conf.setInt(PetStoreJob.props.bigpetstore_records.name(), records);

        Path raw_generated_data = new Path(test_data_directory,"generated");

        Job createInput= PetStoreJob.createJob(raw_generated_data, conf);
        createInput.waitForCompletion(true);

        Path outputfile = new Path(raw_generated_data,"part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf).pathToFile(outputfile),Charset.defaultCharset());
        log.info("output : " + FileSystem.getLocal(conf).pathToFile(outputfile));
        for(String l : lines){
            System.out.println(l);
            
        }

        runHive(raw_generated_data);



        log.info("hive:"+ hiveResult);
    }

    public static void assertOutput(Path root,Function<String, Boolean> validator) throws Exception{
        FileSystem fs = FileSystem.getLocal(new Configuration());

        FileStatus[] files=fs.listStatus(root);
        //print out all the files.
        for(FileStatus stat : files){
            System.out.println(stat.getPath() +"  " + stat.getLen());
        }

        Path p = new Path(root,"part-r-00000");
        BufferedReader r = 
                new BufferedReader(
                        new InputStreamReader(fs.open(p)));
        
        //line:{"product":"big chew toy","count":3}
        while(r.ready()){
            String line = r.readLine();
            log.info("line:"+line);
            Assert.assertTrue("validationg line : " +line , validator.apply(line));
        }
    }
    
    Map hiveResult;


    private void runHive(Path input) throws Exception {
                hiveResult = new HiveETL(input).numberOfProductsByProduct();

   }
}