package org.embulk.input.cloudwatch_logs.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import org.embulk.config.ConfigException;

import java.util.Optional;

public abstract class AwsCredentials
{
    private AwsCredentials()
    {
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider(AwsCredentialsConfig task)
    {
        String authenticationMethodOption = "authentication_method";
        String awsSessionTokenOption = "aws_session_token";
        String awsAccessKeyIdOption = "aws_access_key_id";
        String awsSecretAccessKeyOption = "aws_secret_access_key";
        String awsProfileFileOption = "aws_profile_file";
        String awsProfileNameOption = "aws_profile_name";

        switch (task.getAuthenticationMethod()) {
        case "basic": {
            String accessKeyId = required(task.getAwsAccessKeyId(), "'" + awsAccessKeyIdOption + "', '" + awsSecretAccessKeyOption + "'");
            String secretAccessKey = required(task.getAwsSecretAccessKey(), "'" + awsSecretAccessKeyOption + "'");
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            final BasicAWSCredentials creds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
                return new AWSCredentialsProvider() {
                    public AWSCredentials getCredentials()
                    {
                        return creds;
                    }

                    public void refresh()
                    {
                    }
                };
        }

        case "env":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return overwriteAwsCredentials(task, new EnvironmentVariableCredentialsProvider().getCredentials());

        case "instance":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return new InstanceProfileCredentialsProvider();

        case "profile":
            {
                invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
                invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);

                String profileName = task.getAwsProfileName().orElse("default");
                ProfileCredentialsProvider provider;
                if (task.getAwsProfileFile().isPresent()) {
                    ProfilesConfigFile file = new ProfilesConfigFile(task.getAwsProfileFile().get().getFile());
                    provider = new ProfileCredentialsProvider(file, profileName);
                }
                else {
                    provider = new ProfileCredentialsProvider(profileName);
                }

                return overwriteAwsCredentials(task, provider.getCredentials());
            }

        case "properties":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return overwriteAwsCredentials(task, new SystemPropertiesCredentialsProvider().getCredentials());

        case "anonymous":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);
            return new AWSCredentialsProvider() {
                public AWSCredentials getCredentials()
                {
                    return new AnonymousAWSCredentials();
                }

                public void refresh()
                {
                }
            };

        case "session":
            {
                String accessKeyId = required(task.getAwsAccessKeyId(),
                                              "'" + awsAccessKeyIdOption + "', '" + awsSecretAccessKeyOption + "', '" + awsSessionTokenOption + "'");
                String secretAccessKey = required(task.getAwsSecretAccessKey(), "'" + awsSecretAccessKeyOption + "', '" + awsSessionTokenOption + "'");
                String sessionToken = required(task.getAwsSessionToken(),
                                               "'" + awsSessionTokenOption + "'");
                invalid(task.getAwsProfileFile(), awsProfileFileOption);
                invalid(task.getAwsProfileName(), awsProfileNameOption);
                final AWSSessionCredentials creds = new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
                return new AWSSessionCredentialsProvider() {
                    public AWSSessionCredentials getCredentials()
                    {
                        return creds;
                    }

                    public void refresh()
                    {
                    }
                };
            }

        case "default":
            {
                invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
                invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
                invalid(task.getAwsProfileFile(), awsProfileFileOption);
                invalid(task.getAwsProfileName(), awsProfileNameOption);

                return new DefaultAWSCredentialsProviderChain();
            }

        default:
            throw new ConfigException(String.format("Unknown authentication_method '%s'. Supported methods are basic, env, instance, profile, properties, anonymous, and default.",
                        task.getAuthenticationMethod()));
        }
    }

    private static AWSCredentialsProvider overwriteAwsCredentials(AwsCredentialsConfig task, final AWSCredentials creds)
    {
        return new AWSCredentialsProvider() {
            public AWSCredentials getCredentials()
            {
                return creds;
            }

            public void refresh()
            {
            }
        };
    }

    private static <T> T required(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            return value.get();
        }
        else {
            throw new ConfigException("Required option is not set: " + message);
        }
    }

    private static <T> void invalid(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            throw new ConfigException("Invalid option is set: " + message);
        }
    }
}
