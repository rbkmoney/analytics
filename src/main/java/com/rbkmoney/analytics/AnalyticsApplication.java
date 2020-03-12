package com.rbkmoney.analytics;

import com.rbkmoney.analytics.flowresolver.FlowResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

import javax.annotation.PreDestroy;

@Slf4j
@ServletComponentScan
@SpringBootApplication
public class AnalyticsApplication extends SpringApplication {

    @Autowired
    public FlowResolver flowResolver;

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }

}
