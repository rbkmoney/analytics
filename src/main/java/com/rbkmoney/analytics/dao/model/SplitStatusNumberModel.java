package com.rbkmoney.analytics.dao.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class SplitStatusNumberModel extends SplitNumberModel {

    private String status;

}
