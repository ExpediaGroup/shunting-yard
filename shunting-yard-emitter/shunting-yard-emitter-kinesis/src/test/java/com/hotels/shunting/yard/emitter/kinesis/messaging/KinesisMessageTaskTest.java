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
package com.hotels.bdp.circus.train.event.emitter.kinesis.messaging;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;

import com.hotels.bdp.circus.train.event.common.messaging.Message;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageTaskTest {

  private static final String STREAM = "stream";
  private static final byte[] PAYLOAD = "payload".getBytes();
  private static final int RETRIES = 2;

  private @Mock KinesisProducer producer;
  private @Mock ListenableFuture<UserRecordResult> future;
  private @Mock UserRecordResult userRecordResult;
  private @Mock Message message;

  private KinesisMessageTask kinesisTask;

  @Before
  public void init() throws Exception {
    when(message.getQualifiedTableName()).thenReturn("db.table");
    when(message.getPayload()).thenReturn(PAYLOAD);
    when(producer.addUserRecord(any(UserRecord.class))).thenReturn(future);
    when(future.get()).thenReturn(userRecordResult);
    kinesisTask = new KinesisMessageTask(producer, STREAM, message, RETRIES);
  }

  @Test
  public void typical() {
    when(userRecordResult.isSuccessful()).thenReturn(true);
    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    kinesisTask.run();
    verify(producer, times(1)).addUserRecord(captor.capture());
    assertThat(captor.getValue().getStreamName(), is(STREAM));
    assertThat(captor.getValue().getPartitionKey(), is("db.table"));
    assertThat(captor.getValue().getData(), is(ByteBuffer.wrap(PAYLOAD)));
  }

  @Test
  public void retry() {
    when(userRecordResult.isSuccessful()).thenReturn(false, true);
    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    kinesisTask.run();
    verify(producer, times(2)).addUserRecord(captor.capture());
    assertThat(captor.getValue().getStreamName(), is(STREAM));
    assertThat(captor.getValue().getPartitionKey(), is("db.table"));
    assertThat(captor.getValue().getData(), is(ByteBuffer.wrap(PAYLOAD)));
  }

}
