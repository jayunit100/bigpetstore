package org.bigtop.bigpetstore.docs;

import java.io.File;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.bigtop.bigpetstore.util.BigPetStoreConstants;
import org.bigtop.bigpetstore.util.BigPetStoreConstants.OUTPUTS;
import org.junit.Test;

public class TestDocs {

    @Test
    public void testGraphViz() throws Exception{
        //test the graphviz file
        //by grepping out the constants.
        String graphviz=FileUtils.readFileToString(new File("arch.dot"));
        System.out.println(graphviz);
        
        org.junit.Assert.assertTrue(
                graphviz.contains(
                        OUTPUTS.generated.name()));

        org.junit.Assert.assertTrue(
                graphviz.contains(
                        OUTPUTS.cleaned.name()));
    
        
    }
}
