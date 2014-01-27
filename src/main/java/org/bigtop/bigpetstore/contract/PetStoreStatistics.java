package org.bigtop.bigpetstore.contract;

import java.util.Map;

/**
 * This is the contract for the web site.
 * This object is created by each ETL tool : Summary stats.
 */
public abstract class PetStoreStatistics {

    public abstract Map<String,? extends Number> numberOfTransactionsByState() throws Exception;
    public abstract Map<String, ? extends Number> numberOfProductsByProduct() throws Exception;

}
