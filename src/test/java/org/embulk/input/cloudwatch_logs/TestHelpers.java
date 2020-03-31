package org.embulk.input.cloudwatch_logs;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public final class TestHelpers
{
    private TestHelpers() {}

    public static String getLogGroupName()
    {
        Date d = new Date();
        return String.format("embulk-input-cloudwatch-test-%d", d.getTime());
    }

    public static String getLogStreamName()
    {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}
