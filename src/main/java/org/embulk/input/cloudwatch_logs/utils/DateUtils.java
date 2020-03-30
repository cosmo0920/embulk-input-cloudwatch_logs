package org.embulk.input.cloudwatch_logs.utils;

import com.google.common.base.Joiner;
import org.embulk.config.ConfigException;
import org.joda.time.format.DateTimeFormat;

import java.util.Date;
import java.util.List;

public class DateUtils
{
    public static Date parseDateStr(final String value, final List<String> supportedDateFormats)
            throws ConfigException
    {
        for (final String format : supportedDateFormats) {
            try {
                return DateTimeFormat.forPattern(format).parseDateTime(value).toDate();
            }
            catch (final IllegalArgumentException e) {
                // ignorable exception
            }
        }
        throw new ConfigException("Unsupported DateTime value: '" + value + "', supported formats: [" + Joiner.on(",").join(supportedDateFormats) + "]");
    }

    private DateUtils()
    {
    }
}
