package org.bigtop.bigpetstore.integration;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.bigtop.bigpetstore.etl.PigETL;
import org.junit.After;
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

    @After
    public void tearDown() throws Exception {
        //org.apache.commons.io.FileUtils.deleteDirectory(new File(test_data_directory));
    }
    
    @Before
    public void setupTest() throws Throwable {
        super.setup();
    }
    
    @Test
    public void testPetStorePipeline()  throws Exception {
        runPig(GENERATED,PIG_OUT);
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

        Assert.fail();
        //line:{"product":"big chew toy","count":3}
        while(r.ready()){
            String line = r.readLine();
            log.info("line:"+line);
            System.out.println("line:"+line);
            Assert.assertTrue("validationg line : " + line, validator.apply(line));
        }
    }

    Map pigResult;

    private void runPig(Path input, Path output) throws Exception {
        pigResult = new PigETL(
                input.toString(),
                output.toString(), ExecType.LOCAL).numberOfProductsByProduct();
    }

}
