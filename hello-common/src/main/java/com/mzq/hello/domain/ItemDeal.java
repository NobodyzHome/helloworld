package com.mzq.hello.domain;

public class ItemDeal {

    private final String itemName;
    private final Double dealCount;

    public ItemDeal(String itemName, Double dealCount) {
        this.itemName = itemName;
        this.dealCount = dealCount;
    }

    public String getItemName() {
        return itemName;
    }


    public Double getDealCount() {
        return dealCount;
    }

}
