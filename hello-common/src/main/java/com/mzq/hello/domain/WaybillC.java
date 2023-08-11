package com.mzq.hello.domain;

import lombok.Data;

import java.io.Serializable;

@Data
public class WaybillC implements Serializable {

    private String waybillCode;
    private String waybillSign;
    private String siteCode;
    private String siteName;
    private Long timeStamp;
    private Long watermark;
    private String siteWaybills;
    private String dt;
}
