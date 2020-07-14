package com.rbkmoney.analytics.service.model;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartyWrapper {

    private Party party;

    public PartyWrapper copy() {
        Party targetParty = new Party();
        BeanUtils.copyProperties(party, targetParty);

        return new PartyWrapper(targetParty);
    }


}
