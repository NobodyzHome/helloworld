package com.mzq.hello.domain;

import lombok.Data;

import java.util.Date;

@Data
public class WaybillM {
    private String waybillCode;
    private Date pickupDate;
    private Date deliveryDate;
}
