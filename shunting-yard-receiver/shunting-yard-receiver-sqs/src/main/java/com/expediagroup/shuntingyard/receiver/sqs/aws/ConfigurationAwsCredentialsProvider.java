/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.receiver.sqs.aws;

import static com.expediagroup.shuntingyard.common.Preconditions.checkNotNull;
import static com.expediagroup.shuntingyard.common.PropertyUtils.stringProperty;
import static com.expediagroup.shuntingyard.receiver.sqs.SqsConsumerProperty.AWS_ACCESS_KEY;
import static com.expediagroup.shuntingyard.receiver.sqs.SqsConsumerProperty.AWS_SECRET_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.expediagroup.shuntingyard.receiver.sqs.SqsConsumerProperty;

public class ConfigurationAwsCredentialsProvider implements AWSCredentialsProvider {

  private final Configuration conf;

  public ConfigurationAwsCredentialsProvider(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public AWSCredentials getCredentials() {
    return new BasicAWSCredentials(secret(AWS_ACCESS_KEY), secret(AWS_SECRET_KEY));
  }

  @Override
  public void refresh() {}

  private String secret(SqsConsumerProperty property) {
    String key = checkNotNull(stringProperty(conf, property), "Property " + property + " is not set");
    try {
      return new String(conf.getPassword(key));
    } catch (IOException e) {
      throw new RuntimeException("Unable to read property " + property + " from configuration", e);
    }
  }

}
