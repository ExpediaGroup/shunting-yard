/**
 * Copyright (C) 2016-2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.shunting.yard.receiver.sqs.aws;

import static java.lang.Boolean.TRUE;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.receiver.sqs.SqsProperty.AWS_ACCESS_KEY;
import static com.hotels.shunting.yard.receiver.sqs.SqsProperty.AWS_SECRET_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.auth.AWSCredentials;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationAwsCredentialsProviderTest {

  private final @Spy Configuration conf = new Configuration();

  @Test
  public void credentialsFallbackToPlainTextConfig() {
    conf.set(CredentialProvider.CLEAR_TEXT_FALLBACK, TRUE.toString());
    conf.set(AWS_ACCESS_KEY.key(), "aws_access_key");
    conf.set("aws_access_key", "access");
    conf.set(AWS_SECRET_KEY.key(), "aws_secret_key");
    conf.set("aws_secret_key", "secret");
    ConfigurationAwsCredentialsProvider provider = new ConfigurationAwsCredentialsProvider(conf);
    AWSCredentials creds = provider.getCredentials();
    assertThat(creds.getAWSAccessKeyId()).isEqualTo("access");
    assertThat(creds.getAWSSecretKey()).isEqualTo("secret");
  }

  @Test(expected = RuntimeException.class)
  public void credentialsCannotBeRead() throws Exception {
    conf.set(AWS_ACCESS_KEY.key(), "aws_access_key");
    conf.set(AWS_SECRET_KEY.key(), "aws_secret_key");
    when(conf.getPassword("aws_access_key")).thenThrow(IOException.class);
    new ConfigurationAwsCredentialsProvider(conf).getCredentials();
  }

  @Test(expected = RuntimeException.class)
  public void credentialsFailure() {
    new ConfigurationAwsCredentialsProvider(conf).getCredentials();
  }

}
