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
package com.hotels.shunting.yard.receiver.sqs.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import static com.hotels.shunting.yard.receiver.sqs.SqsProperty.QUEUE;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQS;

import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageReaderTest {

  private static final String QUEUE_NAME = "queue";

  private @Mock MetaStoreEventSerDe serDe;
  private @Mock AmazonSQS consumer;

  private final Configuration conf = new Configuration();
  private SqsMessageReader reader;

  @Before
  public void init() {
    conf.set(QUEUE.key(), QUEUE_NAME);
    reader = new SqsMessageReader(conf, serDe, consumer);
  }

  @Test
  public void close() throws Exception {
    reader.close();
    verify(consumer).shutdown();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void remove() {
    reader.remove();
  }

  @Test
  public void hasNext() {
    assertThat(reader.hasNext()).isTrue();
  }

}
