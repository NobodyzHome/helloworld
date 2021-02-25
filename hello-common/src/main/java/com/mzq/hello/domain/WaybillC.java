package com.mzq.hello.domain;

import lombok.Data;

@Data
public class WaybillC {

    private String waybillCode;
    private String waybillSign;
    private String siteCode;
    private String siteName;
    private Long timeStamp;
    private Long watermark;
    private String siteWaybills;
}
