package com.rbkmoney.analytics.config;

import com.rbkmoney.damsel.payout_processing.PayoutManagementSrv;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;

@Configuration
public class PayouterConfig {

    @Bean
    public PayoutManagementSrv.Iface payouterClient(
            @Value("${service.payouter.url}") Resource resource,
            @Value("${service.payouter.networkTimeout}") int networkTimeout) throws IOException {
        return new THSpawnClientBuilder()
                .withNetworkTimeout(networkTimeout)
                .withAddress(resource.getURI()).build(PayoutManagementSrv.Iface.class);
    }
}
