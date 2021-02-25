package com.mzq.hello.domain;

import lombok.Data;

import java.util.Date;

@Data
public class WaybillCEM {
    private String waybillCode;
    private String waybillSign;
    private String siteCode;
    private String siteName;
    private String busiNo;
    private String busiName;
    private String sendPay;
    private Date pickupDate;
    private Date deliveryDate;
}
