package org.bigtop.bigpetstore.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bigtop.bigpetstore.etl.CrunchETL;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by ubu on 2/2/14.
 */
public class BigPetStoreCrunchIT extends ITUtils {

    static long ID = System.currentTimeMillis();

    @Before
    public void setUpData() throws Throwable {
        ITUtils.setup();
        
    }

    @Test
    public void testCrunchETL() throws Exception {
        System.out.println("files : "+
                FileSystem.getLocal(new Configuration()).globStatus(new Path(GENERATED,"part*")).length);
        new CrunchETL(new Path(GENERATED+"part*"), ITUtils.CRUNCH_OUT);
        
    }

}
