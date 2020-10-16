package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.dao.repository.postgres.DominantDao;
import com.rbkmoney.analytics.listener.handler.dominant.DominantHandler;
import com.rbkmoney.analytics.utils.JsonUtil;
import com.rbkmoney.damsel.domain_config.Commit;
import com.rbkmoney.damsel.domain_config.Operation;
import com.rbkmoney.damsel.domain_config.RepositorySrv;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class DominantService {

    private final RepositorySrv.Iface dominantClient;

    private final DominantDao dominantDao;

    private final List<DominantHandler> dominantHandlers;

    @Transactional
    public void pullDominantRange(int querySize) {

        try {
            Long initialVersion = dominantDao.getLastVersion();
            if (initialVersion == null) {
                initialVersion = 0L;
            }
            AtomicLong lastVer = new AtomicLong(initialVersion);
            Map<Long, Commit> commitMap = dominantClient.pullRange(lastVer.get(), querySize);
            commitMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                List<Operation> operations = entry.getValue().getOps();
                operations.forEach(op -> dominantHandlers.forEach(handler -> {
                    if (handler.isHandle(op)) {
                        log.info("Process commit with versionId={} operation={} ", entry.getKey(), JsonUtil.tBaseToJsonString(op));
                        handler.handle(op, entry.getKey());
                    }
                }));
                if (lastVer.get() == 0) {
                    log.info("Save dominant version: {}", entry.getKey());
                    dominantDao.saveVersion(entry.getKey());
                } else {
                    log.info("Update dominant version={} oldVersion={}", entry.getKey(), lastVer.get());
                    dominantDao.updateVersion(entry.getKey(), lastVer.get());
                }
                lastVer.set(entry.getKey());
            });
        } catch (Exception e) {
            throw new IllegalStateException("Dominant pullRange failed", e);
        }
    }

}
