package org.embulk.input.cloudwatch_logs;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DeleteLogGroupRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.ResourceNotFoundException;

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

    static class CloudWatchLogsTestUtils
    {
        private final AWSLogs logs;
        private final String logGroupName;
        private final String logStreamName;

        public CloudWatchLogsTestUtils(AWSLogs logs, String logGroupName, String logStreamName)
        {
            this.logs = logs;
            this.logGroupName = logGroupName;
            this.logStreamName = logStreamName;
        }

        public void clearLogGroup()
        {
            DeleteLogGroupRequest request = new DeleteLogGroupRequest();
            request.setLogGroupName(logGroupName);
            try {
                logs.deleteLogGroup(request);
            } catch (ResourceNotFoundException ex) {
                // Just ignored.
            }
        }

        public void createLogStream()
        {
            CreateLogGroupRequest groupRequest = new CreateLogGroupRequest();
            groupRequest.setLogGroupName(logGroupName);
            logs.createLogGroup(groupRequest);

            CreateLogStreamRequest streamRequest = new CreateLogStreamRequest();
            streamRequest.setLogGroupName(logGroupName);
            streamRequest.setLogStreamName(logStreamName);
            logs.createLogStream(streamRequest);
        }

        public void putLogEvents(List<InputLogEvent> events) {
            PutLogEventsRequest request = new PutLogEventsRequest()
                    .withLogGroupName(logGroupName)
                    .withLogStreamName(logStreamName)
                    .withLogEvents(events);
            logs.putLogEvents(request);
        }
    }
}
