package org.bigtop.bigpetstore.integration;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bigtop.bigpetstore.clustering.BPSRecommnder;
import org.bigtop.bigpetstore.etl.HiveViewCreator;
import org.bigtop.bigpetstore.util.BigPetStoreConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class BigPetStoreMahoutIT extends ITUtils{

    final static Logger log = LoggerFactory.getLogger(BigPetStoreHiveIT.class);

    @Before
    public void setupTest() throws Throwable {
        super.setup();
        try {
            FileSystem.get(new Configuration()).delete(super.BPS_TEST_MAHOUT_OUT);
        } 
        catch (Exception e) {
            System.out.println("didnt need to delete mahout output.");
        }
    }

    @Test
    public void testPetStorePipeline() throws Exception {
        new BPSRecommnder().run(
                new String[]{
                        BPS_TEST_MAHOUT_IN.toString(),
                        BPS_TEST_MAHOUT_OUT.toString()});
        
        assertOutput(BPS_TEST_MAHOUT_OUT, new Function<String, Boolean>() {
            public Boolean apply(String x) {
                System.out.println("Verifying "+x);
                return true;
            }
        });
    }

    public static void assertOutput(Path base,
            Function<String, Boolean> validator) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());

        FileStatus[] files = fs.listStatus(base);
        // print out all the files.
        for (FileStatus stat : files) {
            System.out.println(stat.getPath() + "  " + stat.getLen());
        }

        Path p = new Path(base, "part-r-00000");
        BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(p)));

        // line:{"product":"big chew toy","count":3}
        while (r.ready()) {
            String line = r.readLine();
            log.info("line:" + line);
            System.out.println("line:" + line);
            Assert.assertTrue("validationg line : " + line,
                    validator.apply(line));
        }
    }

}
