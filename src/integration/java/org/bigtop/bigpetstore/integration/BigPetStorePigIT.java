package org.bigtop.bigpetstore.integration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.bigtop.bigpetstore.etl.PigCSVCleaner;
import org.bigtop.bigpetstore.integration.ITUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * Created with IntelliJ IDEA.
 * User: ubu
 * Date: 1/27/14
 * Time: 7:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class BigPetStorePigIT extends ITUtils{

    final static Logger log = LoggerFactory.getLogger(BigPetStorePigIT.class);

    @Before
    public void setupTest() throws Throwable {
        super.setup();
        try{
            FileSystem.get(new Configuration()).delete(BPS_TEST_PIG_CLEANED);
        }
        catch(Exception e){
            System.out.println("didnt need to delete pig output.");
            //not necessarily an error
        }
    }
    
    @Test
    public void testPetStorePipeline()  throws Exception {
        runPig(BPS_TEST_GENERATED,BPS_TEST_PIG_CLEANED);
        assertOutput(
                BPS_TEST_PIG_CLEANED, 
                new Function<String, Boolean>(){
                    public Boolean apply(String x){
                        //System.out.println("Verified...");
                        return true;
                    }
                });
    }

    public static void assertOutput(Path base,Function<String, Boolean> validator) throws Exception{
        FileSystem fs = FileSystem.getLocal(new Configuration());

        FileStatus[] files=fs.listStatus(base);
        //print out all the files.
        for(FileStatus stat : files){
            System.out.println(stat.getPath() +"  " + stat.getLen());
        }

        /**
         * EXPECTING MAP ONLY... may change.
         */
        Path p = new Path(base,"part-m-00000");
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

    private void runPig(Path input, Path output) throws Exception {
                new PigCSVCleaner(
                        input,
                        output,
                        ExecType.LOCAL);
    }

}
