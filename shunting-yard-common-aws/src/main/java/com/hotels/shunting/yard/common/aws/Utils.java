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
package com.hotels.shunting.yard.common.aws;

import static com.hotels.shunting.yard.common.Utils.checkNotNull;
import static com.hotels.shunting.yard.common.aws.SqsProperty.GROUP_ID;
import static com.hotels.shunting.yard.common.aws.SqsProperty.QUEUE;
import static com.hotels.shunting.yard.common.aws.SqsProperty.REGION;
import static com.hotels.shunting.yard.common.aws.SqsProperty.WAIT_TIME_SECONDS;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

public final class Utils {

  private Utils() {}

  public static String stringProperty(Configuration conf, SqsProperty property) {
    return conf.get(property.key(), (String) property.defaultValue());
  }

  public static Integer intProperty(Configuration conf, SqsProperty property) {
    return conf.getInt(property.key(), (Integer) property.defaultValue());
  }

  public static String queue(Configuration conf) {
    return checkNotNull(stringProperty(conf, QUEUE), "Property " + QUEUE + " is not set");
  }

  public static String region(Configuration conf) {
    return checkNotNull(stringProperty(conf, REGION), "Property " + REGION + " is not set");
  }

  public static String groupId(Configuration conf) {
    return checkNotNull(stringProperty(conf, GROUP_ID), "Property " + GROUP_ID + " is not set");
  }

  public static int waitTimeSeconds(Configuration conf) {
    return checkNotNull(intProperty(conf, WAIT_TIME_SECONDS), "Property " + WAIT_TIME_SECONDS + " is not set");
  }

  public static AWSCredentialsProvider credentials(final Configuration conf) {
    return new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(),
        new InstanceProfileCredentialsProvider(false), new ConfigurationAwsCredentialsProvider(conf));
  }

}
