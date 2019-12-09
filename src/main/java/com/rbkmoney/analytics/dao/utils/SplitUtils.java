package com.rbkmoney.analytics.dao.utils;

import com.rbkmoney.damsel.analytics.SplitUnit;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

import static java.time.ZoneOffset.UTC;

public class SplitUtils {

    public static final String HOUR = "hour";
    public static final String MINUTES = "minutes";
    public static final String DAY = "day";
    public static final String YEAR = "year";
    public static final String MONTHS = "months";

    public static Long generateOffset(Map row, SplitUnit splitUnit) {
        switch (splitUnit) {
            case MINUTE: {
                Long hour = (Long) row.get(HOUR);
                Long minutes = (Long) row.get(MINUTES);
                Date date = (Date) row.get(DAY);
                LocalDateTime localDateTime = date.toLocalDate()
                        .atStartOfDay()
                        .plusHours(hour)
                        .plusMinutes(minutes);
                return localDateTime
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case HOUR: {
                Long hour = (Long) row.get(HOUR);
                Date date = (Date) row.get(DAY);
                LocalDateTime localDateTime = date.toLocalDate()
                        .atStartOfDay()
                        .plusHours(hour);
                return localDateTime
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case DAY: {
                Date date = (Date) row.get(DAY);
                return date.toLocalDate()
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case MONTH: {
                Long date = (Long) row.get(YEAR);
                Long months = (Long) row.get(MONTHS);
                return LocalDate.of(date.intValue(), months.intValue(), 1)
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case YEAR: {
                Long date = (Long) row.get(YEAR);
                return LocalDate.of(date.intValue(), 1, 1)
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            default:
                throw new RuntimeException();
        }
    }

    public static String initGroupByFunction(SplitUnit splitUnit) {
        String groupBy;
        switch (splitUnit) {
            case MINUTE:
                groupBy = "timestamp as day, toHour(toDateTime(eventTime)) as hour, toMinute(toDateTime(eventTime)) as minutes";
                break;
            case HOUR:
                groupBy = "timestamp as day, toHour(toDateTime(eventTime)) as hour";
                break;
            case DAY:
                groupBy = "timestamp as day";
                break;
            case MONTH:
                groupBy = "toYear(timestamp) as year, toMonth(timestamp) as months";
                break;
            case YEAR:
                groupBy = "toYear(timestamp) as year";
                break;
            default:
                throw new RuntimeException();
        }
        return groupBy;
    }


}
