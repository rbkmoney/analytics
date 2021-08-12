package com.rbkmoney.analytics.resource.handler;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.model.ContractFilter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.model.PartyFilter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.model.ShopFilter;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.damsel.analytics.search.PartyFilterRequest;
import com.rbkmoney.damsel.analytics.search.SearchServiceSrv;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class SearchHandler implements SearchServiceSrv.Iface {

    private final PartyDao partyDao;

    @Override
    public List<String> findPartyIds(PartyFilterRequest partyFilterRequest) throws TException {
        log.info("Find party ids. filter={}", partyFilterRequest);
        PartyFilter partyFilter = convertFilter(partyFilterRequest);
        return partyDao.getPartyByFilter(partyFilter).stream()
                .map(Party::getPartyId)
                .collect(Collectors.toList());
    }

    private PartyFilter convertFilter(PartyFilterRequest partyFilterRequest) {
        PartyFilter.PartyFilterBuilder partyFilterBuilder = PartyFilter.builder();
        partyFilterBuilder.email(partyFilterRequest.party_filter.contact_info_email);
        if (partyFilterRequest.shop_filter != null) {
            ShopFilter.ShopFilterBuilder shopFilterBuilder = ShopFilter.builder();
            shopFilterBuilder.locationUrl(partyFilterRequest.shop_filter.location_url);
            if (partyFilterRequest.shop_filter.category_filter != null) {
                shopFilterBuilder.categoryName(partyFilterRequest.shop_filter.category_filter.name);
            }
            partyFilterBuilder.shopFilter(shopFilterBuilder.build());
        }
        if (partyFilterRequest.contract_filter != null) {
            ContractFilter.ContractFilterBuilder contractFilterBuilder = ContractFilter.builder();
            contractFilterBuilder.legalAgreementSignedAt(partyFilterRequest.contract_filter.legal_agreement_signed_at);
            partyFilterBuilder.contractFilter(contractFilterBuilder.build());
        }
        return partyFilterBuilder.build();
    }

}
