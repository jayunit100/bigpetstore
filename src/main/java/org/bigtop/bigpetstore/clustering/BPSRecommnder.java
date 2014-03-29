package org.bigtop.bigpetstore.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
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
        
        int ret = recommenderJob.run(new String[] {
             "--input",args[0],
             "--output",args[1],
             "--usersFile","/tmp/users.txt",
             "--tempDir", "/tmp",
             "--similarityClassname", "SIMILARITY_PEARSON_CORRELATION",
             //"--numRecommendations", "4", 
             //"--encodeLongsAsInts",
             //Boolean.FALSE.toString(), 
             //"--itemBased", Boolean.FALSE.toString() 
        });
        
        System.out.println("Exit of recommender: " + ret);
        return ret;
    }

}