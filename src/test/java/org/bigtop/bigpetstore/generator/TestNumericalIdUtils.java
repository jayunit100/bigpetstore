package org.bigtop.bigpetstore.generator;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;
import org.bigtop.bigpetstore.util.NumericalIdUtils;
import org.junit.Test;

public class TestNumericalIdUtils {

    @Test
    public void testName() {
        String strId= STATE.OK.name()+"_"+ "jay vyas";
        long id = NumericalIdUtils.toId(strId);
        String strId2= STATE.CO.name()+"_"+ "jay vyas";
        long id2 = NumericalIdUtils.toId(strId2);
        System.out.println(id + " " + id2);
        Assert.assertFalse(id==id2);
    }
}
