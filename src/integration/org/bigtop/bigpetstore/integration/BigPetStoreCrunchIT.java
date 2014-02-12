package org.bigtop.bigpetstore.crunchtest;

import com.google.common.io.Files;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bigtop.bigpetstore.etl.CrunchETL;
import org.bigtop.bigpetstore.etl.LineItem;
import org.bigtop.bigpetstore.generator.PetStoreJob;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by ubu on 2/2/14.
 */
public class BigPetStoreCrunchIT {

    static long ID = System.currentTimeMillis();
    String test_data_directory = "/tmp/BigPetStore" + ID;
    Path outputfile = null;
    Path inputfile = null;

    @Before
    public void setUpData() throws Exception {
        int records = 10;
        Configuration conf = new Configuration();

        conf.setInt(PetStoreJob.props.bigpetstore_records.name(), records);

        Path raw_generated_data = new Path(test_data_directory, "generated");

        Job createInput = PetStoreJob.createJob(raw_generated_data, conf);
        createInput.waitForCompletion(true);

        outputfile = new Path(raw_generated_data, "part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf)
                .pathToFile(outputfile), Charset.defaultCharset());
        System.out.println("output : "
                + FileSystem.getLocal(conf).pathToFile(outputfile));
        
    }

    @Test
    public void testCrunchETL() throws Exception {
        
        new CrunchETL(inputfile,outputfile);
        
    }

}
