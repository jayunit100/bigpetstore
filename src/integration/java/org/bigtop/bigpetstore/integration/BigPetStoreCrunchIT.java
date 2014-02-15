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
        CrunchETL etl = new CrunchETL(GENERATED, ITUtils.CRUNCH_OUT);
        System.out.println(etl.numberOfProductsByProduct());
    }

}
