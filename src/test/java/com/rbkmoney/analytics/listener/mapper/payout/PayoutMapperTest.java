package com.rbkmoney.analytics.listener.mapper.payout;

import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePayoutRepository;
import com.rbkmoney.analytics.listener.mapper.factory.PayoutRowFactory;
import com.rbkmoney.payout.manager.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class PayoutMapperTest {

    private static final String PAYOUT_ID = "PAYOUT_ID";

    @Mock
    private PayoutRowFactory payoutRowFactory;

    @Mock
    private ClickHousePayoutRepository clickHousePayoutRepository;

    @InjectMocks
    private PayoutMapper payoutMapper;

    @Before
    public void setUp() {
        when(payoutRowFactory.create(any(), any(), any()))
                .thenReturn(new PayoutRow());

        when(clickHousePayoutRepository.getPaidEvent(PAYOUT_ID)).thenReturn(PAYOUT_ID);
    }

    @Test
    public void shouldMapPayoutStatusChangedEvent() {
        // Given
        PayoutChange payoutChange = PayoutChange.status_changed(
                new PayoutStatusChanged()
                        .setStatus(PayoutStatus.paid(new PayoutPaid())));

        Payout payoutCreated = new Payout();

        Event event = new Event()
                .setSequenceId(1)
                .setPayoutId(PAYOUT_ID)
                .setPayout(payoutCreated);

        // When
        payoutMapper.map(payoutChange, event);

        // Then
        verify(payoutRowFactory, only())
                .create(event, PAYOUT_ID, PayoutStatus.paid(new PayoutPaid()));
    }

    @Test
    public void shouldMapPayoutCancelledAfterBeingPaidEvent() {
        // Given
        PayoutChange payoutChange = PayoutChange.status_changed(
                new PayoutStatusChanged()
                        .setStatus(PayoutStatus.cancelled(new PayoutCancelled())));

        Payout payoutCreated = new Payout();

        Event event = new Event()
                .setSequenceId(1)
                .setPayoutId(PAYOUT_ID)
                .setPayout(payoutCreated);

        // When
        PayoutRow row = payoutMapper.map(payoutChange, event);

        // Then
        verify(payoutRowFactory, only())
                .create(event, PAYOUT_ID, PayoutStatus.cancelled(new PayoutCancelled()));

        assertTrue(row.isCancelledAfterBeingPaid());
    }
}