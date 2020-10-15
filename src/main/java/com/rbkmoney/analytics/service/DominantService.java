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

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class DominantService {

    private final RepositorySrv.Iface dominantClient;

    private final DominantDao dominantDao;

    private final List<DominantHandler> dominantHandlers;

    @Transactional
    public void pullDominantRange(int querySize) {
        Long lastVersion = dominantDao.getLastVersion();
        if (lastVersion == null) {
            lastVersion = 0L;
        }
        try {
            Map<Long, Commit> commitMap = dominantClient.pullRange(lastVersion, querySize);
            commitMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                List<Operation> operations = entry.getValue().getOps();
                operations.forEach(op -> dominantHandlers.forEach(handler -> {
                    if (handler.isHandle(op)) {
                        log.info("Process commit with versionId={} operation={} ", entry.getKey(), JsonUtil.tBaseToJsonString(op));
                        handler.handle(op, entry.getKey());
                    }
                }));
            });
        } catch (Exception e) {
            throw new IllegalStateException("Dominant pullRange failed", e);
        }
    }

}
