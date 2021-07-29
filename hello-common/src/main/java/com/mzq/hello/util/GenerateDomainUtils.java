package com.mzq.hello.util;


import com.mzq.hello.domain.BdWaybillOrder;
import com.mzq.hello.domain.WaybillC;
import com.mzq.hello.domain.WaybillRouteLink;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GenerateDomainUtils {

    public static List<BdWaybillOrder> generateBdWaybillOrders(int count) {
        List<BdWaybillOrder> bdWaybillOrders = new ArrayList<>(count);
        do {
            bdWaybillOrders.add(generateBdWaybillOrder());
        } while (bdWaybillOrders.size() != count);
        return bdWaybillOrders;
    }

    public static BdWaybillOrder generateBdWaybillOrder() {
        BdWaybillOrder bdWaybillOrder = new BdWaybillOrder();
        bdWaybillOrder.setWaybillCode(generateOrderCode("JD"));
        bdWaybillOrder.setWaybillSign(generateSign());
        bdWaybillOrder.setSiteCode(String.valueOf(RandomUtils.nextInt(100, 1000)));
        bdWaybillOrder.setSiteName(bdWaybillOrder.getSiteCode() + "站点");
        bdWaybillOrder.setBusiNo(String.valueOf(RandomUtils.nextInt(1, 100)));
        bdWaybillOrder.setBusiName(bdWaybillOrder.getBusiNo() + "商家");
        bdWaybillOrder.setSendPay(generateSign());
        bdWaybillOrder.setPickupDate(generateDate());
        bdWaybillOrder.setDeliveryDate(generateDate());
        bdWaybillOrder.setOrderCode(generateOrderCode("ORDER"));
        bdWaybillOrder.setOrderCreateDate(generateDate());
        bdWaybillOrder.setPackageCode(generateOrderCode("PACKAGE"));
        bdWaybillOrder.setTimestamp(System.currentTimeMillis() + RandomUtils.nextLong(0, 10000));

        return bdWaybillOrder;
    }

    public static WaybillRouteLink generateWaybillRouteLink() {
        WaybillRouteLink waybillRouteLink = new WaybillRouteLink();
        waybillRouteLink.setWaybillCode(generateOrderCode("JD"));
        waybillRouteLink.setPackageCode(waybillRouteLink.getWaybillCode() + "-1-1");
        waybillRouteLink.setStaticDeliveryTime(System.currentTimeMillis());

        return waybillRouteLink;
    }

    public static String generateOrderCode(String prefix) {
        return prefix + StringUtils.leftPad(String.valueOf(RandomUtils.nextInt(1, 10000)), 10, "0");
    }

    public static String generateSign() {
        return RandomStringUtils.random(30, "01");
    }

    public static Date generateDate() {
        return Date.from(ZonedDateTime.now().plusDays(RandomUtils.nextLong(1, 1000)).toInstant());
    }

    public static WaybillC generateWaybillc() {
        WaybillC waybillC = new WaybillC();
        waybillC.setWaybillCode(generateOrderCode("JD"));
        waybillC.setWaybillSign(generateSign());
        waybillC.setSiteCode(String.valueOf(RandomUtils.nextInt(100, 1000)));
        waybillC.setSiteName(waybillC.getSiteCode() + "站点");
        waybillC.setTimeStamp(System.currentTimeMillis());
        waybillC.setWatermark(waybillC.getTimeStamp() - RandomUtils.nextInt(1000, 10000));

        return waybillC;
    }

    public static List<WaybillC> generateWaybillc(int count) {
        List<WaybillC> waybillCList = new ArrayList<>(count);
        do {
            waybillCList.add(generateWaybillc());
        } while (--count > 0);
        return waybillCList;
    }
}
