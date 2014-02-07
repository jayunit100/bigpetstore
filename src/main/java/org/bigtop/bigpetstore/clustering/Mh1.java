package org.bigtop.bigpetstore.clustering;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

public class Mh1 {

    /**
	 * 
	 */
    public static void main(String[] args) throws TasteException, IOException {
        DataModel model = new FileDataModel(new File("mahout_data/movies.dat"));
        Recommender recommender = new SlopeOneRecommender(model);
        Recommender cachingRecommender = new CachingRecommender(recommender);
    }
}
