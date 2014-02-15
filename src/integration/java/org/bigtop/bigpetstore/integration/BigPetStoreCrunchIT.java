package org.bigtop.bigpetstore.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bigtop.bigpetstore.etl.CrunchETL;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;
import org.junit.Assert;
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
        System.out.println("Crunch etl product summary " + etl.numberOfProductsByProduct());
        if(super.hasAProduct(etl.numberOfProductsByProduct())){
           System.out.println("Passed.  At least one product is valid...");
        }
        else{
            Assert.fail("No valid products in "+etl.numberOfProductsByProduct());
        }
        
    }

}
