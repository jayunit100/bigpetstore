package org.bigtop.bigpetstore.pigtest;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bigtop.bigpetstore.etl.PigETL;
import org.bigtop.bigpetstore.generator.PetStoreJob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.pig.ExecType;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertTrue;

public class TestPig {

    static long ID = System.currentTimeMillis();
    String test_data_directory = "/tmp/BigPetStore" + ID;

    @Test
    public void testPigETL() throws Exception {

        int records = 10;
        /**
         * Setup configuration with prop.
         */
        Configuration conf = new Configuration();

        conf.setInt(PetStoreJob.props.bigpetstore_records.name(), records);

        Path raw_generated_data = new Path(test_data_directory, "generated");

        Job createInput = PetStoreJob.createJob(raw_generated_data, conf);
        createInput.waitForCompletion(true);

        Path outputfile = new Path(raw_generated_data, "part-r-00000");
        List<String> lines = Files.readLines(FileSystem.getLocal(conf)
                .pathToFile(outputfile), Charset.defaultCharset());
        System.out.println("output : "
                + FileSystem.getLocal(conf).pathToFile(outputfile));
        /*
         * for(String l : lines){ System.out.println(l);
         * 
         * }
         */

        // Map pig=runPig(raw_generated_data,new
        // Path(test_data_directory+"/pig/"));

        // System.out.println("pig:"+ pig);

    }

    private Map runPig(Path input, Path output) throws Exception {
        Map pigResult = new PigETL(input.toString(), output.toString(),
                ExecType.LOCAL).numberOfProductsByProduct();
        return pigResult;
    }

}
