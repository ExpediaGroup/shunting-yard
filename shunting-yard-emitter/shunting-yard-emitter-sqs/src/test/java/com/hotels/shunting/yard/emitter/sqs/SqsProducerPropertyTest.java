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

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.emitter.sqs.SqsProducerProperty.AWS_ACCESS_KEY;
import static com.hotels.shunting.yard.emitter.sqs.SqsProducerProperty.AWS_SECRET_KEY;
import static com.hotels.shunting.yard.emitter.sqs.SqsProducerProperty.QUEUE;
import static com.hotels.shunting.yard.emitter.sqs.SqsProducerProperty.REGION;
import static com.hotels.shunting.yard.emitter.sqs.SqsProducerProperty.SERDE_CLASS;

import org.junit.Test;

import com.hotels.shunting.yard.common.io.jackson.JsonMetaStoreEventSerDe;

public class SqsProducerPropertyTest {

  private static String prefixedKey(String key) {
    return "com.hotels.shunting.yard.event.emitter.sqs." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(SqsProducerProperty.values().length).isEqualTo(6);
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

  @Test
  public void serdeClass() {
    assertThat(SERDE_CLASS.unPrefixedKey()).isEqualTo("serde.class");
    assertThat(SERDE_CLASS.key()).isEqualTo(prefixedKey("serde.class"));
    assertThat(SERDE_CLASS.defaultValue()).isEqualTo(JsonMetaStoreEventSerDe.class.getName());
  }

}
