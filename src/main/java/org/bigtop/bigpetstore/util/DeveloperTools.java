package org.bigtop.bigpetstore.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;

/*
 * just a simple utility class to generate different type of data files for testing
 */
public class DeveloperTools {
    
    /**
     * Validates that the expected args are present in the "args" array.
     * Just some syntactic sugar for good arg error handling.
     * @param args
     * @param expected arguments.
     */
    public static void validate(String[] args, String... expected) {
        int i=-1;
        try{ 
            for(i = 0 ; i < expected.length ; i++) {
                System.out.println("VALUE OF " + expected[i] + " = " + args[i]);
            }
        }
        catch(Throwable t) {
            System.out.println("rgument " + i + " not available.");
            System.out.println("We expected " + expected.length + " arguments for this phase");
        }
        
        
    }
    public static void main(String[] args) throws Exception {
        Log LOG = LogFactory.getLog(Job.class);
        LOG.info("xAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    }

}
