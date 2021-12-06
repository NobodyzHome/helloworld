package com.mzq.hello.flink.sql.udf.table;

public class AliasSearchResult {

    private int id;
    private String key;
    private String alias;

    public AliasSearchResult() {
    }

    public AliasSearchResult(int id, String key, String alias) {
        this.id = id;
        this.key = key;
        this.alias = alias;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
