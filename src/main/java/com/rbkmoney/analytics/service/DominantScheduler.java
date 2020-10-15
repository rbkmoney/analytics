package com.rbkmoney.analytics.service;

import com.rbkmoney.damsel.domain_config.RepositorySrv;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "service", name = "dominant.scheduler.enabled", havingValue = "true")
public class DominantScheduler {

    private final DominantService dominantService;

    private final RepositorySrv.Iface dominantClient;

    @Value("${service.dominant.scheduler.querySize}")
    private int querySize;

    @Scheduled(fixedDelayString = "${service.dominant.scheduler.pollingDelay}")
    @SchedulerLock(name = "scheduledTaskName")
    public void pollScheduler() {
        LockAssert.assertLocked();
        dominantService.pullDominantRange(querySize);
    }

}
