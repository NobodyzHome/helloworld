package com.mzq.hello.flink.sql.udf.aggregation;

public class AggregateResult {

    private String joined;
    private String[] uniqueStrArray;

    public AggregateResult(String joined, String[] uniqueStrArray) {
        this.joined = joined;
        this.uniqueStrArray = uniqueStrArray;
    }

    public String getJoined() {
        return joined;
    }

    public void setJoined(String joined) {
        this.joined = joined;
    }

    public String[] getUniqueStrArray() {
        return uniqueStrArray;
    }

    public void setUniqueStrArray(String[] uniqueStrArray) {
        this.uniqueStrArray = uniqueStrArray;
    }
}
