Embulk::JavaPlugin.register_input(
  "cloudwatch_logs", "org.embulk.input.cloudwatch_logs.CloudwatchLogsInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
