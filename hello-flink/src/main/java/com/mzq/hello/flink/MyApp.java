package com.mzq.hello.flink;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

@ConfigurationPropertiesScan
public class MyApp {

    @Bean
    @ConfigurationProperties("spring.redis.url.basic-info")
    public JimdbConfig jimdbConfig() {
        return new JimdbConfig();
    }
}
