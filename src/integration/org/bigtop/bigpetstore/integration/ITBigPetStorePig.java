package org.bigtop.bigpetstore.integration;

import com.google.common.base.Function;
import com.google.common.io.Files;
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
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ubu
 * Date: 1/27/14
 * Time: 7:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class ITBigPetStorePig {

    // We need the directory label to read a data from it later
    static long ID = System.currentTimeMillis();
    String test_data_directory  =  "/tmp/BigPetStore"+ID;
    final static Logger log = LoggerFactory.getLogger(ITBigPetStoreHive.class);

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
        List<String> lines = Files.readLines(FileSystem.getLocal(conf).pathToFile(outputfile), Charset.defaultCharset());
        log.info("output : " + FileSystem.getLocal(conf).pathToFile(outputfile));
        for(String l : lines){
            System.out.println(l);

        }
        runPig(raw_generated_data,new Path(test_data_directory+"/pig/"));

        log.info("pig:"+ pigResult);


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
