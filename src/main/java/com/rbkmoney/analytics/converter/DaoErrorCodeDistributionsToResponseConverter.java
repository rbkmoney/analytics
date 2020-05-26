package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.utils.SubErrorGenerator;
import com.rbkmoney.damsel.analytics.ErrorDistribution;
import com.rbkmoney.damsel.analytics.SubErrorDistributionsResponse;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class DaoErrorCodeDistributionsToResponseConverter {

    public SubErrorDistributionsResponse convert(List<com.rbkmoney.analytics.dao.model.NamingDistribution> namingDistributions) {
        List<ErrorDistribution> collect = namingDistributions.stream()
                .map(paymentToolDistribution -> new ErrorDistribution()
                        .setError(SubErrorGenerator.generateError(paymentToolDistribution.getName()))
                        .setPercents(paymentToolDistribution.getPercent())
                ).collect(toList());

        return new SubErrorDistributionsResponse()
                .setErrorDistributions(collect);
    }

}
