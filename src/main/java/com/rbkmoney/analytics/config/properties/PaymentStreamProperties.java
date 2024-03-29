package com.rbkmoney.analytics.config.properties;

import com.rbkmoney.mg.event.sink.model.CustomProperties;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "kafka.streams.payment")
public class PaymentStreamProperties extends CustomProperties {

    private boolean enabled;

}
