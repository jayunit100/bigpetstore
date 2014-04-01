package org.bigtop.bigpetstore.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.pig.builtin.LOG;
import org.bigtop.bigpetstore.util.DeveloperTools;

/**
 * Implement user based collab filter.
 * 
 * The input set is the 
 * 
 * userid,productid,weight 
 * 
 * rows. 
 */
public class BPSRecommnder implements Tool {

   
    Configuration c;
    @Override
    public void setConf(Configuration conf) {
        c=conf;
    }

    @Override
    public Configuration getConf() {
        return c;
    }

    @Override
    public int run(String[] args) throws Exception {
        DeveloperTools.validate(args,"input path","output path");

        Configuration conf = new Configuration();

        System.out.println("Runnning recommender against : " + args[0] +" -> " + args[1]);

        RecommenderJob recommenderJob = new RecommenderJob();
        /**
        int x = ToolRunner.run(getConf(), new BPSPreparePreferenceMatrixJob(), new String[]{
            "--input", args[0],
            "--output", args[1],
            "--tempDir", "/tmp",
          });
        System.out.println("RETURN = " + x);
         **/
        
        int ret = recommenderJob.run(new String[] {
             "--input",args[0],
             "--output",args[1],
             "--usersFile","/tmp/users.txt",
             "--tempDir", "/tmp/mahout_"+System.currentTimeMillis(),
             "--similarityClassname", "SIMILARITY_PEARSON_CORRELATION",
             "--threshold",".00000000001",
             "--numRecommendations", "4", 
             //"--encodeLongsAsInts",
             //Boolean.FALSE.toString(), 
             //"--itemBased", Boolean.FALSE.toString() 
        });
        
        System.out.println("Exit of recommender: " + ret);
        return ret;
    }

}