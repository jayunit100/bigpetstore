package org.bigtop.bigpetstore.generator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.Date;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;
import org.bigtop.bigpetstore.generator.BPSGenerator.props;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * run this test with vm options -XX:MaxPermSize=256m -Xms512m -Xmx1024m
 * 
 */
public class TestPetStoreTransactionGeneratorJob {

    final static Logger log = LoggerFactory
            .getLogger(TestPetStoreTransactionGeneratorJob.class);

    @Test
    public void test() throws Exception {

        System.out.println("memory : " + Runtime.getRuntime().freeMemory()
                / 1000000);
        if (Runtime.getRuntime().freeMemory() / 1000000 < 75) {
            // throw new
            // RuntimeException("need more memory to run this test !");
        }
        int records = 20;
        /**
         * Setup configuration with prop.
         */
        Configuration c = new Configuration();
        c.setInt(props.bigpetstore_records.name(), records);

        /**
         * Run the job
         */
        Path output = new Path("petstoredata/" + (new Date()).toString());
        Job createInput = BPSGenerator.createJob(output, c);
        createInput.submit();
        System.out.println(createInput);
        createInput.waitForCompletion(true);

        FileSystem fs = FileSystem.getLocal(new Configuration());

        /**
         * Read file output into string.
         */
        DataInputStream f = fs.open(new Path(output, "part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(f));
        String s;
        int recordsSeen = 0;
        boolean CTseen = false;
        boolean AZseen = false;

        // confirm that both CT and AZ are seen in the outputs.
        while (br.ready()) {
            s = br.readLine();
            System.out.println("===>" + s);
            recordsSeen++;
            if (s.contains(STATE.CT.name())) {
                CTseen = true;
            }
            if (s.contains(STATE.AZ.name())) {
                AZseen = true;
            }
        }

        // records seen should = 20
        Assert.assertEquals(records, recordsSeen);
        // Assert that a couple of the states are seen (todo make it
        // comprehensive for all states).
        Assert.assertTrue(CTseen);
        Assert.assertTrue(AZseen);
        log.info("Created " + records + " , file was "
                + fs.getFileStatus(new Path(output, "part-r-00000")).getLen()
                + " bytes.");
    }
}
