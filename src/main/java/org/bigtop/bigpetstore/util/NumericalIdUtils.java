package org.bigtop.bigpetstore.util;

import java.math.BigInteger;

import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;

/**
 * User and Product IDs need numerical
 * identifiers for recommender algorithms
 * which attempt to interpolate new 
 * products.
 */
public class NumericalIdUtils {

    /**
     * People: Leading with ordinal code for state.
     */
    public static long toId(STATE state, String name){
        String fromRawData =
                state==null? 
                        name:
                         (state.name()+"_"+name);  
        return fromRawData.hashCode();
    }
    /**
     * People: Leading with ordinal code for state.
     */
    public static long toId(String name){
        return toId(null,name);
    }
}