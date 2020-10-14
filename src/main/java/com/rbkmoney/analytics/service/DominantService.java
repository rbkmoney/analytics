package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.dao.repository.postgres.DominantDao;
import com.rbkmoney.analytics.listener.handler.dominant.DominantHandler;
import com.rbkmoney.analytics.utils.JsonUtil;
import com.rbkmoney.damsel.domain_config.Commit;
import com.rbkmoney.damsel.domain_config.Operation;
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

    private final DominantDao dominantDao;

    private final List<DominantHandler> dominantHandlers;

    @Transactional
    public void processCommits(long versionId, Map.Entry<Long, Commit> entry) {
        List<Operation> operations = entry.getValue().getOps();
        operations.forEach(op -> dominantHandlers.forEach(handler -> {
            if (handler.isHandle(op)) {
                log.info("Process commit with versionId={} operation={} ", versionId, JsonUtil.tBaseToJsonString(op));
                handler.handle(op, versionId);
            }
        }));
        dominantDao.saveVersion(entry.getKey());
    }

}
