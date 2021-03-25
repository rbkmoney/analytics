package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.converter.RawToNamingDistributionConverter;
import com.rbkmoney.analytics.converter.RawToNumModelConverter;
import com.rbkmoney.analytics.converter.RawToShopAmountModelConverter;
import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.model.NamingDistribution;
import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.ShopAmountModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RawMapperConfig {

    @Bean
    public CommonRowsMapper<NumberModel> costCommonRowsMapper(RawToNumModelConverter rawToNumModelConverter) {
        return new CommonRowsMapper<>(rawToNumModelConverter);
    }

    @Bean
    public CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper(
            RawToNamingDistributionConverter rawToNamingDistributionConverter) {
        return new CommonRowsMapper<>(rawToNamingDistributionConverter);
    }

    @Bean
    public CommonRowsMapper<ShopAmountModel> shopAmountModelCommonRowsMapper(
            RawToShopAmountModelConverter rawToShopAmountModelConverter) {
        return new CommonRowsMapper<>(rawToShopAmountModelConverter);
    }

}
