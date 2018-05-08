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
package com.hotels.shunting.yard.receiver.kinesis.adapter.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.BUFFER_CAPACITY;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.POLLING_TIMEOUT_MS;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.model.Record;

import com.hotels.shunting.yard.receiver.kinesis.adapter.buffer.DefaultKinesisRecordBuffer;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKinesisRecordBufferTest {

  private static final int CAPACITY = 2;
  private static final long TIMEOUT = 100L;

  private @Mock Configuration conf;
  private @Mock Record record;

  private DefaultKinesisRecordBuffer buffer;

  @Before
  public void init() {
    when(conf.getInt(eq(BUFFER_CAPACITY.key()), anyInt())).thenReturn(CAPACITY);
    when(conf.getLong(eq(POLLING_TIMEOUT_MS.key()), anyLong())).thenReturn(TIMEOUT);
    buffer = DefaultKinesisRecordBuffer.create(conf);
  }

  @Test(expected = NullPointerException.class)
  public void nullRecord() {
    buffer.put(null);
  }

  @Test
  public void queueIsFull() {
    assertThat(buffer.put(record)).isTrue();
    assertThat(buffer.put(record)).isTrue();
    assertThat(buffer.put(record)).isFalse();
  }

  @Test
  public void queueIsEmpty() {
    assertThat(buffer.get()).isNull();
  }

  @Test
  public void readAfterWrite() {
    assertThat(buffer.put(record)).isTrue();
    assertThat(buffer.get()).isSameAs(record);
  }

}
