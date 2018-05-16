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
package com.hotels.shunting.yard.emitter.sqs;

import static com.hotels.shunting.yard.common.Preconditions.checkNotNull;
import static com.hotels.shunting.yard.common.PropertyUtils.stringProperty;
import static com.hotels.shunting.yard.emitter.sqs.SqsProperty.GROUP_ID;
import static com.hotels.shunting.yard.emitter.sqs.SqsProperty.QUEUE;
import static com.hotels.shunting.yard.emitter.sqs.SqsProperty.REGION;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

import com.hotels.shunting.yard.emitter.sqs.aws.ConfigurationAwsCredentialsProvider;

public final class SqsEmitterUtils {

  private SqsEmitterUtils() {}

  public static String queue(Configuration conf) {
    return checkNotNull(stringProperty(conf, QUEUE), "Property " + QUEUE + " is not set");
  }

  public static String region(Configuration conf) {
    return checkNotNull(stringProperty(conf, REGION), "Property " + REGION + " is not set");
  }

  public static String groupId(Configuration conf) {
    return checkNotNull(stringProperty(conf, GROUP_ID), "Property " + GROUP_ID + " is not set");
  }

  public static AWSCredentialsProvider credentials(final Configuration conf) {
    return new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(),
        new InstanceProfileCredentialsProvider(false), new ConfigurationAwsCredentialsProvider(conf));
  }

}
