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

import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.QUEUE;
import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.REGION;
import static com.hotels.shunting.yard.receiver.sqs.Utils.queue;
import static com.hotels.shunting.yard.receiver.sqs.Utils.region;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class UtilsTest {

  private final Configuration conf = new Configuration();

  @Test
  public void queueIsNotNull() {
    conf.set(QUEUE.key(), "queue");
    assertThat(queue(conf)).isEqualTo("queue");
  }

  @Test(expected = IllegalArgumentException.class)
  public void queueIsNull() {
    conf.set(QUEUE.key(), null);
    queue(conf);
  }

  @Test
  public void regionIsNotNull() {
    conf.set(REGION.key(), "region");
    assertThat(region(conf)).isEqualTo("region");
  }

  @Test(expected = IllegalArgumentException.class)
  public void regionIsNull() {
    conf.set(REGION.key(), null);
    region(conf);
  }

}
