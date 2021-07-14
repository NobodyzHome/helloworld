package com.mzq.hello.flink.func.source;

import com.mzq.hello.domain.WaybillC;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;

import java.time.Duration;

public class WaybillCSource extends AbstractSourceFunction<WaybillC> {

    private IntCounter intCounter;

    @Override
    protected void init() {
        intCounter = getRuntimeContext().getIntCounter("waybillC-counter");
    }

    @Override
    protected Duration interval() {
        return Duration.ofSeconds(5);
    }

    @Override
    protected WaybillC createElement(SourceContext<WaybillC> ctx) {
        intCounter.add(1);
        Integer value = intCounter.getLocalValue();

        WaybillC waybillC = new WaybillC();
        waybillC.setWaybillCode("JD" + StringUtils.leftPad(value.toString(), 10, "0"));
        waybillC.setWaybillSign(RandomStringUtils.random(30, "01"));
        waybillC.setSiteCode(String.valueOf(RandomUtils.nextInt(1, 10)));
        waybillC.setSiteName(String.format("站点%s", waybillC.getSiteCode()));
        waybillC.setTimeStamp(System.currentTimeMillis());
        return waybillC;
    }
}
