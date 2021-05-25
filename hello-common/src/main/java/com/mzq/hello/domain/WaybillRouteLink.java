package com.mzq.hello.domain;

import lombok.Data;

import java.util.Date;

@Data
public class WaybillRouteLink {

    private String waybillCode;
    private String packageCode;
    private Long staticDeliveryTime;
}
