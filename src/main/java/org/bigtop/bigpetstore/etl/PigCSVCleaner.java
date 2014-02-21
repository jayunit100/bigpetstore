package org.bigtop.bigpetstore.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

/**
 * This class operates by ETL'ing the dataset into pig, and then implements the
 * "statistics" contract in the functions which follow.
 * 
 * The pigServer is persisted through the life of the class, so that the
 * intermediate data sets created in the constructor can be reused.
 */
public class PigCSVCleaner  {

    PigServer pigServer;
    
    
    
    public PigCSVCleaner(Path inputPath, ExecType ex)
            throws Exception {

        System.out.println("input  " + inputPath);

        // run pig in local mode
        pigServer = new PigServer(ex);
        // final String datapath =
        // test_data_directory+"/generated/part-r-00000";

        /**
         * First, split the tabs up.
         * 
         * BigPetStore,storeCode_OK,2 yang,jay,Mon Dec 15 23:33:49 EST
         * 1969,69.56,flea collar
         * 
         * ("BigPetStore,storeCode_OK,2",
         * "yang,jay,Mon Dec 15 23:33:49 EST 1969,69.56,flea collar")
         * 
         * BigPetStore,storeCode_AK,1 amanda,fitzgerald,Sat Dec 20 09:44:25 EET
         * 1969,7.5,cat-food
         */
        pigServer.registerQuery("csvdata = LOAD '<i>' AS (ID,DETAILS);"
                .replaceAll("<i>", inputPath.toString()));

        /**
         * Now, we want to split the two tab delimited feidls into uniform
         * fields of comma separated values. To do this, we 1) Internally split
         * the FIRST and SECOND fields by commas "a,b,c" --> (a,b,c) 2) FLATTEN
         * the FIRST and SECOND fields. (d,e) (a,b,c) -> d e a b c
         */
        pigServer
                .registerQuery("id_details = FOREACH csvdata GENERATE "
                        + "FLATTEN" + "(STRSPLIT"
                        + "(ID,',',3)) AS (drop, code, transaction) ,"
                        + "FLATTEN" + "(STRSPLIT" +
                        "(DETAILS,',',5)) AS (lname, fname, date, price, product:chararray);");
        
        pigServer.store("id_details", "transactions-cleaned");
        
    }

    public static void main(final String[] args) throws Exception {
        System.out.println("Starting pig etl " + args.length);
        Configuration c = new Configuration();
        int res = ToolRunner.run(
                c, 
                new Tool() {
                    Configuration conf;
                    @Override
                    public void setConf(Configuration conf) {
                        this.conf=conf;
                    }
                    
                    @Override
                    public Configuration getConf() {
                        return this.conf;
                    }
                    
                    @Override
                    public int run(String[] args) throws Exception {
                        new PigCSVCleaner(
                                new Path(args[0]),
                                ExecType.MAPREDUCE);
                        return 0;
                    }
                }, args);
        System.exit(res);
      }


}
