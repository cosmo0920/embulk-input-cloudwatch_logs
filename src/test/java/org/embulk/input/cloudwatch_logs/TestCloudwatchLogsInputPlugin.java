package org.embulk.input.cloudwatch_logs;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DeleteLogGroupRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.ResourceNotFoundException;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.test.TestingEmbulk;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.embulk.input.cloudwatch_logs.TestHelpers;

import static org.embulk.input.cloudwatch_logs.CloudwatchLogsInputPlugin.CloudWatchLogsPluginTask;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestCloudwatchLogsInputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "cloudwatch_logs", CloudwatchLogsInputPlugin.class)
            .build();

    private CloudwatchLogsInputPlugin plugin;

    private ConfigSource config;
    private MockPageOutput output = new MockPageOutput();
    private PageBuilder pageBuilder;
    private String logGroupName;
    private String logStreamName;
    private AWSLogs logsClient;

    private static String EMBULK_LOGS_TEST_REGION;
    private static String EMBULK_LOGS_TEST_ACCESS_KEY_ID;
    private static String EMBULK_LOGS_TEST_SECRET_ACCESS_KEY;

    /*
     * This test case requires environment variables:
     *   EMBULK_LOGS_TEST_REGION
     *   EMBULK_LOGS_TEST_ACCESS_KEY_ID
     *   EMBULK_LOGS_TEST_SECRET_ACCESS_KEY
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        EMBULK_LOGS_TEST_REGION = System.getenv("EMBULK_LOGS_TEST_REGION");
        EMBULK_LOGS_TEST_ACCESS_KEY_ID = System.getenv("EMBULK_LOGS_TEST_ACCESS_KEY_ID");
        EMBULK_LOGS_TEST_SECRET_ACCESS_KEY = System.getenv("EMBULK_LOGS_TEST_SECRET_ACCESS_KEY");
        assumeNotNull(EMBULK_LOGS_TEST_REGION, EMBULK_LOGS_TEST_ACCESS_KEY_ID, EMBULK_LOGS_TEST_SECRET_ACCESS_KEY);
    }

    @Before
    public void setUp() throws IOException
    {
        logGroupName = TestHelpers.getLogGroupName();
        logStreamName = TestHelpers.getLogStreamName();

        if (plugin == null) {
            plugin = Mockito.spy(new CloudwatchLogsInputPlugin());
            config = runtime.getExec().newConfigSource()
                    .set("type", "cloudwatch_logs")
                    .set("log_group_name", logGroupName)
                    .set("log_stream_name", logStreamName)
                    .set("region", EMBULK_LOGS_TEST_REGION)
                    .set("aws_access_key_id", EMBULK_LOGS_TEST_ACCESS_KEY_ID)
                    .set("aws_secret_access_key", EMBULK_LOGS_TEST_SECRET_ACCESS_KEY);
            pageBuilder = Mockito.mock(PageBuilder.class);
        }
        doReturn(pageBuilder).when(plugin).getPageBuilder(Mockito.any(), Mockito.any());
        CloudWatchLogsPluginTask task = config.loadConfig(CloudWatchLogsPluginTask.class);
        CloudwatchLogsInputPlugin plugin = runtime.getInstance(CloudwatchLogsInputPlugin.class);
        logsClient = plugin.newLogsClient(task);
    }

    @After
    public void tearDown() throws IOException
    {
        if (logGroupName != null) {
            DeleteLogGroupRequest request = new DeleteLogGroupRequest();
            request.setLogGroupName(logGroupName);
            try {
                logsClient.deleteLogGroup(request);
            } catch (ResourceNotFoundException ex) {
                // Just ignored.
            }
        }
    }

    private void createLogStream() {
        CreateLogGroupRequest groupRequest = new CreateLogGroupRequest();
        groupRequest.setLogGroupName(logGroupName);
        logsClient.createLogGroup(groupRequest);

        CreateLogStreamRequest streamRequest = new CreateLogStreamRequest();
        streamRequest.setLogGroupName(logGroupName);
        streamRequest.setLogStreamName(logStreamName);
        logsClient.createLogStream(streamRequest);
    }

    private void putLogEvents(List<InputLogEvent> events) {
        PutLogEventsRequest request = new PutLogEventsRequest()
                .withLogGroupName(logGroupName)
                .withLogStreamName(logStreamName)
                .withLogEvents(events);
        logsClient.putLogEvents(request);
    }

    @Test
    public void test_simple() throws IOException
    {
        createLogStream();

        List<InputLogEvent> events = new ArrayList<>();
        Date d = new Date();
        for (int i = 0; i < 3; i++) {
            InputLogEvent event = new InputLogEvent();
            event.setTimestamp(d.getTime());
            event.setMessage(String.format("CloudWatchLogs from Embulk take %d", i));
            events.add(event);
        }
        putLogEvents(events);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }

        plugin.transaction(config, new Control());
        verify(pageBuilder, times(3)).addRecord();
        verify(pageBuilder, times(1)).finish();
    }

    @Test
    public void configuredRegion()
    {
        CloudWatchLogsPluginTask task = this.config.deepCopy()
                .set("region", "ap-southeast-2")
                .remove("endpoint")
                .loadConfig(CloudWatchLogsPluginTask.class);
        CloudwatchLogsInputPlugin plugin = runtime.getInstance(CloudwatchLogsInputPlugin.class);
        AWSLogs logsClient = plugin.newLogsClient(task);

        // Should not be null
        assumeNotNull(logsClient);
    }

    private class Control implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(final TaskSource taskSource, final Schema schema, final int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(plugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }
}
