package org.bigtop.bigpetstore.generator;

import java.util.Date;

public interface PetStoreTransaction {

	public String getFirstName();
	
	public String getLastName();
	
	public String getProduct();
	
	public Date getDate();
	
	public Integer getPrice();
	
}
