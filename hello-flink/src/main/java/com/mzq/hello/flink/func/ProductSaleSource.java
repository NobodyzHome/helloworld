package com.mzq.hello.flink.func;

import com.mzq.hello.domain.ProductSale;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.time.Duration;

public class ProductSaleSource extends AbstractSourceFunction<ProductSale> {

    @Override
    protected void init() {
    }

    @Override
    protected ProductSale createElement(SourceContext<ProductSale> ctx) {
        ProductSale productIncome = new ProductSale();
        productIncome.setProductName("类别" + RandomStringUtils.random(1, "ABCDEFG"));
        productIncome.setSale(RandomUtils.nextDouble(0.5, 1));
        return productIncome;
    }

    @Override
    protected Duration interval() {
        return Duration.ofSeconds(5);
    }
}
