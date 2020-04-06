INSERT INTO analytic.events_sink (timestamp, eventTime, eventTimeHour, partyId, shopId, email, providerName, amount,
guaranteeDeposit, systemFee, providerFee, externalFee, currency, status, errorReason,
invoiceId, paymentId, sequenceId, ip, bin, maskedPan, paymentTool)
VALUES
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f','ad8b7bfd-0760-4781-a400-51903ee8e509', '72bdf4db749fab52fdac7ec43d828c0f@example.com', 'RSB', 50000,0,0,0,0, 'RUB', 'captured', '','1DkraVdGJfs', '1', 9, '204.26.61.110', '546960', '3125', 'bank_card'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f','ad8b7bfd-0760-4781-a400-51903ee8e501', 'test@example.com','RSB', 5000,0,0,0,0, 'RUB', 'captured', '','1DkratTHbpg', '1',15,'204.26.61.110', '546960', '3125', 'bank_card'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a','ad8b7bfd-0760-4781-a400-51903ee8e502', 'test@example.com','RSB', 5000,0,0,0,0, 'RUB', 'captured', '','1DkratTHbpg', '1',18,'204.26.61.110', '546960', '3125', 'bank_card'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a','ad8b7bfd-0760-4781-a400-51903ee8e503', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'captured', '','1DkratTHbpd', '1',20,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a','ad8b7bfd-0760-4781-a400-51903ee8e503', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'failed', 'card is failed','1DkratTHbpq', '1',21,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772k','ad8b7bfd-0760-4781-a400-51903ee8e504', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'failed', 'card is failed','1DkratTHbpc', '1',22,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772k','ad8b7bfd-0760-4781-a400-51903ee8e504', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'failed', 'bad req','1DkratTHbpc', '1',24,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772k','ad8b7bfd-0760-4781-a400-51903ee8e504', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'failed', 'bad req','1DkratTHbpc', '1',26,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-05', 1575556887,1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772d','ad8b7bfd-0760-4781-a400-51903ee8e509', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'captured', '','1DkratTHbpc', '1',27,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-06', 1575579600,1575579600000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772d','ad8b7bfd-0760-4781-a400-51903ee8e509', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'captured', '','1DkratTHbpc', '1',28,'204.26.61.110', '546960', '3125', 'payment_terminal'),
('2019-12-07', 1575666000,1575666000000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772d','ad8b7bfd-0760-4781-a400-51903ee8e509', 'test@example.com','RSB', 1000,0,0,0,0, 'RUB', 'captured', '','1DkratTHbpc', '1',29,'204.26.61.110', '546960', '3125', 'payment_terminal');

INSERT INTO analytic.events_sink_refund (timestamp, eventTime, eventTimeHour, partyId, shopId, email, providerName, amount,
guaranteeDeposit, systemFee, providerFee, externalFee, currency, status, errorReason,
invoiceId, paymentId, refundId, sequenceId, ip)
VALUES
('2019-12-05', 1575556887, 1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f','ad8b7bfd-0760-4781-a400-51903ee8e509', '72bdf4db749fab52fdac7ec43d828c0f@example.com', 'RSB', 5000,0,0,0,0, 'RUB', 'failed', '','1DkraVdGJfs', '1','1', 9, '204.26.61.110'),
('2019-12-05', 1575556887, 1575554400000, 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f','ad8b7bfd-0760-4781-a400-51903ee8e509', 'test@example.com','RSB', 5000,0,0,0,0, 'RUB', 'succeeded', '','1DkratTHbpg', '1','2', 15,'204.26.61.110'),