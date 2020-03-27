package org.embulk.input.cloudwatch_logs;

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.embulk.input.cloudwatch_logs.CloudwatchLogsInputPlugin;

import static org.embulk.input.cloudwatch_logs.CloudwatchLogsInputPlugin.CloudWatchLogsPluginTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAwsCredentials
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

    private static String EMBULK_LOGS_TEST_GROUP_NAME;
    private static String EMBULK_LOGS_TEST_STREAM_NAME;
    private static String EMBULK_LOGS_TEST_REGION;
    private static String EMBULK_LOGS_TEST_ACCESS_KEY_ID;
    private static String EMBULK_LOGS_TEST_SECRET_ACCESS_KEY;

    /*
     * This test case requires environment variables:
     *   EMBULK_LOGS_TEST_GROUP_NAME
     *   EMBULK_LOGS_TEST_STREAM_NAME
     *   EMBULK_LOGS_TEST_REGION
     *   EMBULK_LOGS_TEST_ACCESS_KEY_ID
     *   EMBULK_LOGS_TEST_SECRET_ACCESS_KEY
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        EMBULK_LOGS_TEST_GROUP_NAME = System.getenv("EMBULK_LOGS_TEST_GROUP_NAME");
        EMBULK_LOGS_TEST_STREAM_NAME = System.getenv("EMBULK_LOGS_TEST_STREAM_NAME");
        EMBULK_LOGS_TEST_REGION = System.getenv("EMBULK_LOGS_TEST_REGION");
        EMBULK_LOGS_TEST_ACCESS_KEY_ID = System.getenv("EMBULK_LOGS_TEST_ACCESS_KEY_ID");
        EMBULK_LOGS_TEST_SECRET_ACCESS_KEY = System.getenv("EMBULK_LOGS_TEST_SECRET_ACCESS_KEY");
        assumeNotNull(EMBULK_LOGS_TEST_GROUP_NAME, EMBULK_LOGS_TEST_STREAM_NAME, EMBULK_LOGS_TEST_REGION, EMBULK_LOGS_TEST_ACCESS_KEY_ID, EMBULK_LOGS_TEST_SECRET_ACCESS_KEY);
    }

    @Before
    public void setUp() throws IOException
    {
        if (plugin == null) {
            plugin = Mockito.spy(new CloudwatchLogsInputPlugin());
            config = runtime.getExec().newConfigSource()
                    .set("type", "cloudwatch_logs")
                    .set("log_group_name", EMBULK_LOGS_TEST_GROUP_NAME)
                    .set("log_stream_name", EMBULK_LOGS_TEST_STREAM_NAME)
                    .set("use_log_stream_name_prefix", "true")
                    .set("region", EMBULK_LOGS_TEST_REGION);
            pageBuilder = Mockito.mock(PageBuilder.class);
        doReturn(pageBuilder).when(plugin).getPageBuilder(Mockito.any(), Mockito.any());
        }
    }

    private void doTest(ConfigSource config) throws IOException
    {
        plugin.transaction(config, new Control());
        verify(pageBuilder, times(1)).finish();
    }

    @Test
    public void useBasic() throws IOException
    {
        ConfigSource config = this.config.deepCopy()
                .set("authentication_method", "basic")
                .set("aws_access_key_id", EMBULK_LOGS_TEST_ACCESS_KEY_ID)
                .set("aws_secret_access_key", EMBULK_LOGS_TEST_SECRET_ACCESS_KEY);
        doTest(config);
    }

    @Test
    public void useEnv()
    {
        // TODO
    }

    @Test
    public void useInstance()
    {
        // TODO
    }

    @Test
    public void useProfile()
    {
        // TODO
    }

    @Test
    public void useProperties() throws IOException
    {
        String prevAccessKeyId = System.getProperty("aws.accessKeyId");
        String prevSecretKey = System.getProperty("aws.secretKey");
        try {
            ConfigSource config = this.config.deepCopy().set("authentication_method", "properties");
            System.setProperty("aws.accessKeyId", EMBULK_LOGS_TEST_ACCESS_KEY_ID);
            System.setProperty("aws.secretKey", EMBULK_LOGS_TEST_SECRET_ACCESS_KEY);
            doTest(config);
        }
        finally {
            if (prevAccessKeyId != null) {
                System.setProperty("aws.accessKeyId", prevAccessKeyId);
            }
            if (prevSecretKey != null) {
                System.setProperty("aws.secretKey", prevSecretKey);
            }
        }
    }

    @Test
    public void useAnonymous()
    {
        // TODO
    }

    @Test
    public void useSession()
    {
        //TODO
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
