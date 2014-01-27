package org.bigtop.bigpetstore.etl;

import java.io.Serializable;

class LineItem  {
    public LineItem(String appName, String storeCode, Integer lineId,
            String firstName, String lastName, String timestamp, Double price,
            String description) {
        super();
        this.appName = appName;
        this.storeCode = storeCode;
        this.lineId = lineId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.timestamp = timestamp;
        this.price = price;
        this.description = description;
    }

    String appName;
    String storeCode;
    Integer lineId;
    String firstName;
    String lastName;
    String timestamp;
    Double price;
    String description;

    public LineItem() {
        super();
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getStoreCode() {
        return storeCode;
    }

    public void setStoreCode(String storeCode) {
        this.storeCode = storeCode;
    }

    public int getLineId() {
        return lineId;
    }

    public void setLineId(int lineId) {
        this.lineId = lineId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    // other constructors, parsers, etc.
}
