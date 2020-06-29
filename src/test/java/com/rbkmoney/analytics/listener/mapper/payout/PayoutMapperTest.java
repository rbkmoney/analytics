package com.rbkmoney.analytics.listener.mapper.payout;

import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.listener.mapper.factory.PayoutRowFactory;
import com.rbkmoney.analytics.service.PayouterClientService;
import com.rbkmoney.damsel.payout_processing.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class PayoutMapperTest {

    private static final String PAYOUT_ID = "PAYOUT_ID";

    @Mock
    private PayouterClientService payouterClientService;

    @Mock
    private PayoutRowFactory payoutRowFactory;

    @InjectMocks
    private PayoutMapper payoutMapper;

    @Before
    public void setUp() {
        when(payoutRowFactory.create(any(), any(), any(), any()))
                .thenReturn(new PayoutRow());
    }

    @Test
    public void shouldMapPayoutStatusChangedEvent() {
        // Given
        PayoutChange payoutChange = PayoutChange.payout_status_changed(
                new PayoutStatusChanged()
                        .setStatus(PayoutStatus.paid(new PayoutPaid())));

        Event event = new Event()
                .setId(1L)
                .setSource(EventSource.payout_id(PAYOUT_ID));

        Payout payoutCreated = new Payout();
        Event payoutCreatedEvent = new Event()
                .setId(2L)
                .setPayload(EventPayload.payout_changes(
                        List.of(PayoutChange.payout_created(new PayoutCreated()
                                .setPayout(payoutCreated)))));

        when(payouterClientService.getEvents(PAYOUT_ID, 1L))
                .thenReturn(List.of(payoutCreatedEvent));

        // When
        payoutMapper.map(payoutChange, event);

        // Then
        verify(payoutRowFactory, only())
                .create(event, payoutCreated, PAYOUT_ID, PayoutStatus.paid(new PayoutPaid()));
    }

    @Test
    public void shouldMapPayoutCancelledAfterBeingPaidEvent() {
        // Given
        PayoutChange payoutChange = PayoutChange.payout_status_changed(
                new PayoutStatusChanged()
                        .setStatus(PayoutStatus.cancelled(new PayoutCancelled())));

        Event event = new Event()
                .setId(1L)
                .setSource(EventSource.payout_id(PAYOUT_ID));

        Payout payoutCreated = new Payout();
        Event payoutCreatedEvent = new Event()
                .setId(2L)
                .setPayload(EventPayload.payout_changes(
                        List.of(PayoutChange.payout_created(new PayoutCreated()
                                .setPayout(payoutCreated)))));

        Event payoutPaidEvent = new Event()
                .setId(3L)
                .setPayload(EventPayload.payout_changes(
                        List.of(PayoutChange.payout_status_changed(new PayoutStatusChanged()
                                .setStatus(PayoutStatus.paid(new PayoutPaid()))))));

        when(payouterClientService.getEvents(PAYOUT_ID, 1L))
                .thenReturn(List.of(payoutCreatedEvent, payoutPaidEvent));

        // When
        PayoutRow row = payoutMapper.map(payoutChange, event);

        // Then
        verify(payoutRowFactory, only())
                .create(event, payoutCreated, PAYOUT_ID, PayoutStatus.cancelled(new PayoutCancelled()));

        assertTrue(row.isCancelledAfterBeingPaid());
    }
}