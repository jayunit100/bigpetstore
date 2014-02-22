package org.bigtop.bigpetstore.integration;


import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bigtop.bigpetstore.generator.PetStoreJob;
import org.bigtop.bigpetstore.util.BigPetStoreConstants;
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
    public static final Path BPS_TEST_GENERATED = fs.makeQualified(
            new Path("bps_integration_",BigPetStoreConstants.OUTPUTS.GENERATED.name())) ;
    public static final Path BPS_TEST_PIG_CLEANED = fs.makeQualified(
            new Path("bps_integration_",BigPetStoreConstants.OUTPUTS.CLEANED.name()));
    public static final Path BPS_TEST_MAHOUT_IN = fs.makeQualified(
            new Path("bps_integration_",BigPetStoreConstants.OUTPUTS.MAHOUT_CF_IN.name()));

    public static void main(String[] args){
        
    }
    //public static final Path CRUNCH_OUT = new Path("bps_integration_",BigPetStoreConstants.OUTPUT_3).makeQualified(fs);
    
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
        if(FileSystem.getLocal(conf).exists(BPS_TEST_GENERATED)){
            return;
        }

        /**
         * Create the data set.
         */
        Job createInput= PetStoreJob.createJob(BPS_TEST_GENERATED, conf);
        createInput.waitForCompletion(true);
        
        Path outputfile = new Path(BPS_TEST_GENERATED,"part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf).pathToFile(outputfile), Charset.defaultCharset());
        log.info("output : " + FileSystem.getLocal(conf).pathToFile(outputfile));
        for(String l : lines){
            System.out.println(l);
        }
    }

}
