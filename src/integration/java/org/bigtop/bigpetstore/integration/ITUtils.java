package org.bigtop.bigpetstore.integration;


import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bigtop.bigpetstore.generator.PetStoreJob;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;


public class ITUtils {
    
    static final Logger log = LoggerFactory.getLogger(ITUtils.class);
    static FileSystem fs;
    static{
        try{
            fs=FileSystem.getLocal(new Configuration());
        }
        catch(Exception e)
        {
            
        }
    }
    public static final Path GENERATED = new Path("/tmp/BigPetStoreTest/generated/").makeQualified(fs);
    public static final Path PIG_OUT = new Path("/tmp/BigPetStoreTest/pig/").makeQualified(fs);
    public static final Path CRUNCH_OUT = new Path("/tmp/BigPetStoreTest/crunch/").makeQualified(fs);
    
    /**
     * A poor mans unit test for stochastic data:
     * Verify that at least one product has valid amount.
     */
    public static boolean hasAProduct(Map<String,? extends Number> m){
        for(STATE s : STATE.values()){
            for(String p : s.products){
                if(m.containsKey(p)){
                    if(m.get(p).intValue()>0){
                        return true;
                }
              }
            }
        }
        return false;
    }
    
    /**
     * Creates a generated input data set in 
     * 
     * test_data_directory/generated.
     * i.e. 
     *  test_data_directory/generated/part-r-00000 
     */
    public static void setup() throws Throwable{
        int records = 10;
        /**
         * Setup configuration with prop.
         */
        Configuration conf = new Configuration();
        conf.setInt(PetStoreJob.props.bigpetstore_records.name(), records);
        
        /**
         * Only create if doesnt exist already.....
         */
        if(FileSystem.getLocal(conf).exists(GENERATED)){
            return;
        }

        /**
         * Create the data set.
         */
        Job createInput= PetStoreJob.createJob(GENERATED, conf);
        createInput.waitForCompletion(true);
        
        Path outputfile = new Path(GENERATED,"part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf).pathToFile(outputfile), Charset.defaultCharset());
        log.info("output : " + FileSystem.getLocal(conf).pathToFile(outputfile));
        for(String l : lines){
            System.out.println(l);
        }
    }

}
