package com.rbkmoney.analytics.converter;

import com.rbkmoney.damsel.analytics.ErrorDistributionsResponse;
import com.rbkmoney.damsel.analytics.NamingDistribution;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class DaoErrorReasonDistributionsToResponseConverter {

    public ErrorDistributionsResponse convert(
            List<com.rbkmoney.analytics.dao.model.NamingDistribution> namingDistributions) {
        List<NamingDistribution> collect = namingDistributions.stream()
                .map(paymentToolDistribution -> new NamingDistribution()
                        .setName(paymentToolDistribution.getName())
                        .setPercents(paymentToolDistribution.getPercent())
                ).collect(toList());

        return new ErrorDistributionsResponse()
                .setErrorDistributions(collect);
    }

}
