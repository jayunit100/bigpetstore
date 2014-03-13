package org.bigtop.bigpetstore.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/*
 * just a simple utility class to generate different type of data files for testing
 */
public class DeveloperTools {
    
    public static void validate(String[] minargs, String... expected) {

        try{ 
            for(int i = 0 ; i < minargs.length ; i++) {
                System.out.println("VALUE OF " + expected[i] + " = " + minargs[i]);
            }
        }
        catch(Throwable t) {
            System.out.println("Error parsing arguments.");
            System.out.println("We expect " + expected.length + " arguments for this phase.");
        }
        
        
    }
    public static void main(String[] args) throws Exception {

        FsPermission p = FsPermission.createImmutable((short)0755);
        System.out.println(p.getUserAction()+" " + p.getGroupAction() + " " + p.getOtherAction());
    
        FileSystem fs = FileSystem.get(null);
    }

}
