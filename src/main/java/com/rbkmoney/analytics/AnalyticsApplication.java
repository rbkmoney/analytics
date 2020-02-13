package com.rbkmoney.analytics;

import com.rbkmoney.analytics.listener.StartupListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;

import javax.annotation.PreDestroy;

@Slf4j
@ServletComponentScan
@SpringBootApplication
public class AnalyticsApplication extends SpringApplication {

    @Autowired
    private StartupListener startupListener;

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }

    @PreDestroy
    public void preDestroy() {
        log.info("AnalyticsApplication preDestroy!");
        startupListener.stop();
    }
}
