package org.bigtop.bigpetstore.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.bigtop.bigpetstore.etl.CrunchETL;
import org.bigtop.bigpetstore.etl.PigETL;
import org.bigtop.bigpetstore.generator.PetStoreJob;

/**
 * Executes the full petstore flow.
 */
public class PetStoreMain {

    public static void main(String[] args) throws Exception {

        /**
         * Run the first job.
         */
        Path generated = new Path("BPS_"+System.currentTimeMillis()+"");
        Configuration c = new Configuration();
        Job j = PetStoreJob.createJob(generated, 10);
        j.waitForCompletion(true);
        
        PigETL pigETL = new PigETL(
                generated+"",
                generated+"_pigETL", 
                ExecType.LOCAL);
        
        System.out.println(pigETL.numberOfProductsByProduct());
        System.out.println(pigETL.numberOfTransactionsByState());

        /**
         * Now run PIG ETL of the data.
         */
        CrunchETL crunchETL = new CrunchETL(
                generated,
                new Path(generated+"_pigETL"));
        
        System.out.println(crunchETL.numberOfProductsByProduct());
        System.out.println(crunchETL.numberOfTransactionsByState());

    }
}
