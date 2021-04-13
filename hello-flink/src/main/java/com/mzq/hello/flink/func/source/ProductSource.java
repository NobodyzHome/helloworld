package com.mzq.hello.flink.func.source;

import com.mzq.hello.domain.ProductIncome;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public class ProductSource extends AbstractSourceFunction<ProductIncome> {

    @Override
    protected void init() {

    }

    @Override
    protected ProductIncome createElement(SourceContext<ProductIncome> ctx) {
        ProductIncome productIncome = new ProductIncome();
        productIncome.setProductName("类别" + RandomStringUtils.random(1, "ABCDEFG"));
        productIncome.setIncome(RandomUtils.nextInt(100, 3000));
        return productIncome;
    }
}
