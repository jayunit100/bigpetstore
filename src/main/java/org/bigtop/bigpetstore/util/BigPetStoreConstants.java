package org.bigtop.bigpetstore.util;

/**
 * static final constants
 * 
 * is useful to have the basic sql here as the HIVE SQL can vary between hive
 * versions if updated here will update everywhere
 */
public class BigPetStoreConstants {

   //Files should be stored in graphviz arch.dot
   public enum OUTPUTS{
        generated,//generator
        cleaned,//pig
        pig_ad_hoc_script,
        MAHOUT_CF_IN,//hive view over data for mahout
        MAHOUT_CF_OUT,//mahout cf results 
        CUSTOMER_PAGE//crunchhh
    };
    

}
