package org.embulk.input.cloudwatch_logs.aws;

import org.embulk.spi.unit.LocalFile;

import java.util.Optional;

public interface AwsCredentialsConfig
{
    String getAuthenticationMethod();

    void setAuthenticationMethod(String method);

    Optional<String> getAwsAccessKeyId();

    void setAwsAccessKeyId(Optional<String> value);

    Optional<String> getAwsSecretAccessKey();

    void setAwsSecretAccessKey(Optional<String> value);

    Optional<String> getAwsSessionToken();

    void setAwsSessionToken(Optional<String> value);

    Optional<LocalFile> getAwsProfileFile();

    void setAwsProfileFile(Optional<String> value);

    Optional<String> getAwsProfileName();

    void setAwsProfileName(Optional<String> value);
}
