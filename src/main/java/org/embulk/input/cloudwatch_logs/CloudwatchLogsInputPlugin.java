package org.embulk.input.cloudwatch_logs;

import java.util.List;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.AWSLogs;
import com.google.common.base.Optional;

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
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

public class CloudwatchLogsInputPlugin
        extends AbstractCloudwatchLogsInputPlugin
{
    public interface CloudWatchLogsPluginTask
            extends PluginTask
    {
        @Config("region")
        @ConfigDefault("null")
        Optional<String> getRegion();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return CloudWatchLogsPluginTask.class;
    }

    @Override
    protected AWSLogs newLogsClient(PluginTask task)
    {
        CloudWatchLogsPluginTask t = (CloudWatchLogsPluginTask) task;
        Optional<String> region = t.getRegion();
        AWSLogsClientBuilder builder = super.defaultLogsClientBuilder(t);

        if (region.isPresent()) {
            builder.setRegion(region.get());
        }
        else {
            throw new ConfigException("region is required");
        }

        return builder.build();
    }
}
