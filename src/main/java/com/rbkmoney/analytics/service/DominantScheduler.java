package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.dao.repository.postgres.DominantDao;
import com.rbkmoney.damsel.domain_config.Commit;
import com.rbkmoney.damsel.domain_config.RepositorySrv;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "service", name = "dominant.scheduler.enabled", havingValue = "true")
public class DominantScheduler {

    private final DominantService dominantService;

    private final RepositorySrv.Iface dominantClient;

    private final DominantDao dominantDao;

    @Value("${service.dominant.scheduler.querySize}")
    private int querySize;

    @Scheduled(fixedDelayString = "${service.dominant.scheduler.pollingDelay}")
    @SchedulerLock(name = "scheduledTaskName")
    public void pollScheduler() {
        LockAssert.assertLocked();

        Long lastVersion = dominantDao.getLastVersion();
        if (lastVersion == null) {
            lastVersion = 0L;
        }

        try {
            Map<Long, Commit> commitMap = dominantClient.pullRange(lastVersion, querySize);
            commitMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                dominantService.processCommits(entry.getKey(), entry);
            });
        } catch (Exception e) {
            log.warn("Exception while polling from dominant", e);
        }
    }

}
