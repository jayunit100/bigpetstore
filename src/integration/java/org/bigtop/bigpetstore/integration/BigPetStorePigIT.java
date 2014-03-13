package org.bigtop.bigpetstore.integration;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.bigtop.bigpetstore.etl.PigCSVCleaner;
import org.bigtop.bigpetstore.util.BigPetStoreConstants;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

/**
 * Created with IntelliJ IDEA.
 * User: ubu
 * Date: 1/27/14
 * Time: 7:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class BigPetStorePigIT extends ITUtils{

    final static Logger log = LoggerFactory.getLogger(BigPetStorePigIT.class);

    /**
     * An extra unsupported code path that we have so 
     * people can do ad hoc analytics on pig data after it is 
     * cleaned.
     */
    public static final Path BPS_TEST_PIG_COUNT_PRODUCTS = fs.makeQualified(
            new Path("bps_integration_",
                    BigPetStoreConstants.OUTPUTS.BPS_TEST_PIG_COUNT_PRODUCTS.name()));
    
    static final File PIG_SCRIPT = new File("BPS_analytics.pig");
    
    static {
        if(PIG_SCRIPT.exists()) {
            
        }
        else
            throw new RuntimeException("Couldnt find pig script at " + PIG_SCRIPT.getAbsolutePath());
    }
    
    @Before
    public void setupTest() throws Throwable {
        super.setup();
        try{
            FileSystem.get(new Configuration()).delete(BPS_TEST_PIG_CLEANED);
            FileSystem.get(new Configuration()).delete(BPS_TEST_PIG_COUNT_PRODUCTS);
        }
        catch(Exception e){
            System.out.println("didnt need to delete pig output.");
            //not necessarily an error
        }
    }

    static Map<Path,Function<String,Boolean>> TESTS = ImmutableMap.of(
            /**
            * Test of the main output
            */
            BPS_TEST_PIG_CLEANED, 
            new Function<String, Boolean>(){
                public Boolean apply(String x){
                    //System.out.println("Verified...");
                    return true;
                }
            },
            //Example of how to count products
            //after doing basic pig data cleanup
            BPS_TEST_PIG_COUNT_PRODUCTS,
            new Function<String, Boolean>(){
                //Jeff'
                public Boolean apply(String x){
                    return true;
                }
            }); 
            
    /**
     * The "core" task reformats data to TSV.  lets test that first.
     */
    @Test
    public void testPetStoreCorePipeline()  throws Exception {
        runPig(
               BPS_TEST_GENERATED,
               BPS_TEST_PIG_CLEANED,
               PIG_SCRIPT);
        for(Entry<Path,Function<String,Boolean>> e : TESTS.entrySet()) {
            assertOutput(e.getKey(),e.getValue());
        }
    }

    public static void assertOutput(Path base,Function<String, Boolean> validator) throws Exception{
        FileSystem fs = FileSystem.getLocal(new Configuration());

        FileStatus[] files=fs.listStatus(base);
        //print out all the files.
        for(FileStatus stat : files){
            System.out.println(stat.getPath() +"  " + stat.getLen());
        }

        /**
         * Support map OR reduce outputs
         */
        Path partm = new Path(base,"part-m-00000");
        Path partr = new Path(base,"part-r-00000");
        Path p = fs.exists(partm)?partm:partr;
        
        /**
         * Now we read through the file and validate 
         * its contents.
         */
        BufferedReader r =
                new BufferedReader(
                        new InputStreamReader(fs.open(p)));

        //line:{"product":"big chew toy","count":3}
        while(r.ready()){
            String line = r.readLine();
            log.info("line:"+line);
            //System.out.println("line:"+line);
            Assert.assertTrue("validationg line : " + line, validator.apply(line));
        }
    }

    Map pigResult;

    private void runPig(Path input, Path output, File pigscript) throws Exception {
        
        
                new PigCSVCleaner(
                        input,
                        output,
                        ExecType.LOCAL,
                        pigscript);
    }

}
