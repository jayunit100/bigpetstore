package org.bigtop.bigpetstore.util;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/*
 * just a simple utility class to generate different type of data files for testing
 */
public class DeveloperTools {

    File hiveConfigData;

    public static void main(String[] args) throws IOException {

        DeveloperTools tools = new DeveloperTools();
        tools.createInitialData();

    }

    private void createInitialData() throws IOException {
        File hiveConfigData = new File("hiveTestData/a.txt");
        String delim = ",";
        if (!hiveConfigData.exists()) {
            hiveConfigData.createNewFile();
        }
        FileWriter fw = new FileWriter(hiveConfigData);
        BufferedWriter bw = new BufferedWriter(fw);
        for(int k = 0; k <10; k++) {
        String line =  k+delim+"this is the "+k+"th description\n";
        bw.write(line);
        }
        // easy to forget to flush the stream
        bw.flush();
        fw.flush();
        fw.close();




    }
}
