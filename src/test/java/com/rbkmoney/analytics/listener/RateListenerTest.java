package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.config.KafkaConfig;
import com.rbkmoney.analytics.config.SerializeConfig;
import com.rbkmoney.analytics.listener.handler.rate.RateMachineEventHandler;
import com.rbkmoney.mg.event.sink.service.ConsumerGroupIdService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RateListener.class, KafkaConfig.class, SerializeConfig.class},
        properties = {"kafka.state.cache.size=0"})
public class RateListenerTest extends KafkaAbstractTest {

    @MockBean
    RateMachineEventHandler eventHandler;

    @MockBean
    ConsumerGroupIdService consumerGroupIdService;

    @Test
    public void handle() {
    }

}
