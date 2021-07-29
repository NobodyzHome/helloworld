package com.mzq.hello.flink;

import com.mzq.hello.domain.BdWaybillOrder;

public class BdWaybillInfoDeSerializer extends JsonDeSerializer<BdWaybillOrder> {

    @Override
    Class<BdWaybillOrder> getTargetClass() {
        return BdWaybillOrder.class;
    }
}
