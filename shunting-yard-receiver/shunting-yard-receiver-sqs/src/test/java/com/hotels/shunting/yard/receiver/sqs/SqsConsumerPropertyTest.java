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
package com.hotels.shunting.yard.receiver.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.AWS_ACCESS_KEY;
import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.AWS_SECRET_KEY;
import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.QUEUE;
import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.REGION;
import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.WAIT_TIME_SECONDS;

import org.junit.Test;

public class SqsConsumerPropertyTest {

  private static String prefixedKey(String key) {
    return "com.hotels.shunting.yard.event.receiver.sqs." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(SqsConsumerProperty.values().length).isEqualTo(5);
  }

  @Test
  public void queue() {
    assertThat(QUEUE.unPrefixedKey()).isEqualTo("queue");
    assertThat(QUEUE.key()).isEqualTo(prefixedKey("queue"));
    assertThat(QUEUE.defaultValue()).isNull();
  }

  @Test
  public void region() {
    assertThat(REGION.unPrefixedKey()).isEqualTo("region");
    assertThat(REGION.key()).isEqualTo(prefixedKey("region"));
    assertThat(REGION.defaultValue()).isEqualTo("us-west-2");
  }

  @Test
  public void waitTimeSeconds() {
    assertThat(WAIT_TIME_SECONDS.unPrefixedKey()).isEqualTo("wait.time.seconds");
    assertThat(WAIT_TIME_SECONDS.key()).isEqualTo(prefixedKey("wait.time.seconds"));
    assertThat(WAIT_TIME_SECONDS.defaultValue()).isEqualTo(10);
  }

  @Test
  public void awsAccessKey() {
    assertThat(AWS_ACCESS_KEY.unPrefixedKey()).isEqualTo("aws.access.key");
    assertThat(AWS_ACCESS_KEY.key()).isEqualTo(prefixedKey("aws.access.key"));
    assertThat(AWS_ACCESS_KEY.defaultValue()).isNull();
  }

  @Test
  public void awsSecretKey() {
    assertThat(AWS_SECRET_KEY.unPrefixedKey()).isEqualTo("aws.secret.key");
    assertThat(AWS_SECRET_KEY.key()).isEqualTo(prefixedKey("aws.secret.key"));
    assertThat(AWS_SECRET_KEY.defaultValue()).isNull();
  }

}
