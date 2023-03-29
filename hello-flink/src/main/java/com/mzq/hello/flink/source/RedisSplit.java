package com.mzq.hello.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class RedisSplit implements SourceSplit, Serializable {

    private final String cmd;

    public RedisSplit(String cmd) {
        this.cmd = cmd;
    }

    public String getCmd() {
        return cmd;
    }

    @Override
    public String splitId() {
        return getCmd();
    }
}
