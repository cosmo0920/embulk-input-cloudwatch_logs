package org.embulk.input.cloudwatch_logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.OutputLogEvent;

import org.embulk.input.cloudwatch_logs.aws.AwsCredentials;
import org.embulk.input.cloudwatch_logs.aws.AwsCredentialsTask;
import org.embulk.input.cloudwatch_logs.utils.DateUtils;

public abstract class AbstractCloudwatchLogsInputPlugin
        implements InputPlugin
{
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public interface PluginTask
            extends AwsCredentialsTask, Task
    {
        @Config("log_group_name")
        public String getLogGroupName();

        @Config("log_stream_name")
        @ConfigDefault("null")
        public Optional<String> getLogStreamName();

        @Config("use_log_stream_name_prefix")
        @ConfigDefault("false")
        public boolean getUseLogStreamNamePrefix();

        @Config("start_time")
        @ConfigDefault("null")
        public Optional<String> getStartTime();

        @Config("end_time")
        @ConfigDefault("null")
        public Optional<String> getEndTime();

        @Config("time_range_format")
        @ConfigDefault("null")
        public Optional<String> getTimeRangeFormat();

        @Config("column_name")
        @ConfigDefault("\"message\"")
        public String getColumnName();
    }

    protected abstract Class<? extends PluginTask> getTaskClass();

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(getTaskClass());

        Schema schema = new Schema.Builder()
                .add("timestamp", Types.TIMESTAMP)
                .add(task.getColumnName(), Types.STRING)
                .build();
        int taskCount = 1;  // number of run() method calls
        String time_range_format = DEFAULT_DATE_FORMAT;
        if (task.getTimeRangeFormat().isPresent()) {
            time_range_format = task.getTimeRangeFormat().get();
        }
        if (task.getStartTime().isPresent() && task.getEndTime().isPresent()) {
            Date startTime = DateUtils.parseDateStr(task.getStartTime().get(),
                                                    Collections.singletonList(time_range_format));
            Date endTime = DateUtils.parseDateStr(task.getEndTime().get(),
                                                  Collections.singletonList(time_range_format));
            if (endTime.before(startTime)) {
                throw new ConfigException(String.format("endTime(%s) must not be earlier than startTime(%s).",
                                                        task.getEndTime().get(),
                                                        task.getStartTime().get()));
            }
        }

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        // Do nothing.
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());

        AWSLogs client = newLogsClient(task);
        CloudWatchLogsDrainer drainer = new CloudWatchLogsDrainer(task, client);
        if (task.getUseLogStreamNamePrefix()) {
            List<LogStream> defaultLogStream = new ArrayList<LogStream>();
            List<LogStream> logStreams = drainer.describeLogStreams(defaultLogStream, null);
            try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                for (LogStream stream : logStreams) {
                    String logStreamName = stream.getLogStreamName();
                    addRecords(drainer, pageBuilder, logStreamName);
                }

                pageBuilder.finish();
            }
        }
        else {
            try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                String logStreamName = null;
                if (task.getLogStreamName().isPresent()) {
                    logStreamName = task.getLogStreamName().get();
                }
                addRecords(drainer, pageBuilder, logStreamName);

                pageBuilder.finish();
            }
        }

        return Exec.newTaskReport();
    }

    private void addRecords(CloudWatchLogsDrainer drainer, PageBuilder pageBuilder, String logStreamName)
    {
        String nextToken = null;
        while (true) {
            GetLogEventsResult result = drainer.getEvents(logStreamName, nextToken);
            List<OutputLogEvent> events = result.getEvents();
            for (OutputLogEvent event : events) {
                pageBuilder.setTimestamp(0, Timestamp.ofEpochMilli(event.getTimestamp()));
                pageBuilder.setString(1, event.getMessage());

                pageBuilder.addRecord();
            }
            String nextForwardToken = result.getNextForwardToken();
            if (nextForwardToken == null || nextForwardToken.equals(nextToken)) {
                break;
            }
            nextToken = nextForwardToken;
        }
    }

    /**
     * Provide an overridable default client.
     * Since this returns an immutable object, it is not for any further customizations by mutating,
     * Subclass's customization should be done through {@link AbstractCloudwatchLogsInputPlugin#defaultLogsClientBuilder}.
     * @param task Embulk plugin task
     * @return AWSLogs
     */
    protected AWSLogs newLogsClient(PluginTask task)
    {
        return defaultLogsClientBuilder(task).build();
    }

    /**
     * A base builder for the subclasses to then customize.builder
     * @param task Embulk plugin
     * @return AWSLogsClientBuilder
     **/
    protected AWSLogsClientBuilder defaultLogsClientBuilder(PluginTask task)
    {
        return AWSLogsClientBuilder
                .standard()
                .withCredentials(getCredentialsProvider(task))
                .withClientConfiguration(getClientConfiguration(task));
    }

    protected AWSCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(50); // SDK default: 50
//        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000
        clientConfig.setRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY);
        // TODO: implement http proxy

        return clientConfig;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    @VisibleForTesting
    public PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    @VisibleForTesting
    static class CloudWatchLogsDrainer
    {
        private final AWSLogs client;
        private final PluginTask task;

        public CloudWatchLogsDrainer(PluginTask task, AWSLogs client)
        {
            this.client = client;
            this.task = task;
        }

        private GetLogEventsResult getEvents(String logStreamName, String nextToken)
        {
            try {
                String logGroupName = task.getLogGroupName();
                GetLogEventsRequest request = new GetLogEventsRequest()
                        .withLogGroupName(logGroupName)
                        .withLogStreamName(logStreamName)
                        .withStartFromHead(true);
                String time_range_format = DEFAULT_DATE_FORMAT;
                if (task.getTimeRangeFormat().isPresent()) {
                    time_range_format = task.getTimeRangeFormat().get();
                }
                if (task.getStartTime().isPresent()) {
                    String startTimeStr = task.getStartTime().get();
                    Date startTime = DateUtils.parseDateStr(startTimeStr, Collections.singletonList(time_range_format));
                    request.setStartTime(startTime.getTime());
                }
                if (task.getEndTime().isPresent()) {
                    String endTimeStr = task.getEndTime().get();
                    Date endTime = DateUtils.parseDateStr(endTimeStr, Collections.singletonList(time_range_format));
                    request.setEndTime(endTime.getTime());
                }
                if (nextToken != null) {
                    request.setNextToken(nextToken);
                }
                GetLogEventsResult response = client.getLogEvents(request);

                return response;
            }
            catch (AmazonServiceException ex) {
                if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                    // HTTP 40x errors. auth error etc. See AWS document for the full list:
                    // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CommonErrors.html
                    if (ex.getStatusCode() != 400   // 404 Bad Request is unexpected error
                        || "ExpiredToken".equalsIgnoreCase(ex.getErrorCode())) { // if statusCode == 400 && errorCode == ExpiredToken => throws ConfigException
                        throw new ConfigException(ex);
                    }
                }
                throw ex;
            }
        }

        private List<LogStream> describeLogStreams(List<LogStream> logStreams, String nextToken)
        {
            try {
                String logGroupName = task.getLogGroupName();
                DescribeLogStreamsRequest request = new DescribeLogStreamsRequest();
                request.setLogGroupName(logGroupName);
                if (nextToken != null) {
                    request.setNextToken(nextToken);
                }
                if (task.getLogStreamName().isPresent()) {
                    request.setLogStreamNamePrefix(task.getLogStreamName().get());
                }

                DescribeLogStreamsResult response = client.describeLogStreams(request);
                if (!logStreams.isEmpty()) {
                    for (LogStream stream : response.getLogStreams()) {
                        logStreams.add(stream);
                    }
                }
                else {
                    logStreams = response.getLogStreams();
                }
                if (response.getNextToken() != null) {
                    logStreams = describeLogStreams(logStreams, response.getNextToken());
                }

                return logStreams;
            }
            catch (AmazonServiceException ex) {
                if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                    // HTTP 40x errors. auth error etc. See AWS document for the full list:
                    // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CommonErrors.html
                    if (ex.getStatusCode() != 400   // 404 Bad Request is unexpected error
                        || "ExpiredToken".equalsIgnoreCase(ex.getErrorCode())) { // if statusCode == 400 && errorCode == ExpiredToken => throws ConfigException
                        throw new ConfigException(ex);
                    }
                }
                throw ex;
            }
        }
    }
}
