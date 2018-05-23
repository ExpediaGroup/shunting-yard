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
package com.hotels.shunting.yard.receiver.kinesis.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.model.Record;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.SerDeException;
import com.hotels.shunting.yard.receiver.kinesis.adapter.KinesisConsumer;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageReaderTest {

  private static final byte[] MESSAGE_CONTENT = "message".getBytes();
  private static final ByteBuffer RECORD_DATA = ByteBuffer.wrap(MESSAGE_CONTENT);

  private @Mock MetaStoreEventSerDe serDe;
  private @Mock KinesisConsumer consumer;
  private @Mock Record record;
  private @Mock SerializableListenerEvent event;

  private KinesisMessageReader reader;

  @Before
  public void init() throws Exception {
    when(consumer.next()).thenReturn(record);
    when(record.getData()).thenReturn(RECORD_DATA);
    when(serDe.unmarshal(MESSAGE_CONTENT)).thenReturn(event);
    reader = new KinesisMessageReader(serDe, consumer);
  }

  @Test
  public void close() throws Exception {
    reader.close();
    verify(consumer).close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void remove() {
    reader.remove();
  }

  @Test
  public void hasNext() {
    assertThat(reader.hasNext()).isTrue();
  }

  @Test
  public void nextReadsRecordsFromQueue() throws Exception {
    assertThat(reader.next()).isSameAs(event);
    verify(consumer).next();
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test(expected = NullPointerException.class)
  public void nextReadsNoRecordsFromQueue() throws Exception {
    reset(consumer);
    reader.next();
  }

  @Test(expected = SerDeException.class)
  public void unmarhsallThrowsException() throws Exception {
    when(serDe.unmarshal(any(byte[].class))).thenThrow(RuntimeException.class);
    reader.next();
  }

}
