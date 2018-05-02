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
package com.hotels.shunting.yard.emitter.kinesis.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;

import com.hotels.shunting.yard.common.exception.ShuntingYardException;
import com.hotels.shunting.yard.common.messaging.Message;

@RunWith(MockitoJUnitRunner.class)
public class KinesisMessageTaskTest {

  private static final String STREAM = "stream";
  private static final byte[] PAYLOAD = "payload".getBytes();
  private static final int RETRIES = 2;

  public @Rule ExpectedException expectedException = ExpectedException.none();

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
    assertThat(captor.getValue().getStreamName()).isEqualTo(STREAM);
    assertThat(captor.getValue().getPartitionKey()).isEqualTo("db.table");
    assertThat(captor.getValue().getData()).isEqualTo(ByteBuffer.wrap(PAYLOAD));
  }

  @Test
  public void retry() {
    when(userRecordResult.isSuccessful()).thenReturn(false, true);
    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    kinesisTask.run();
    verify(producer, times(2)).addUserRecord(captor.capture());
    assertThat(captor.getValue().getStreamName()).isEqualTo(STREAM);
    assertThat(captor.getValue().getPartitionKey()).isEqualTo("db.table");
    assertThat(captor.getValue().getData()).isEqualTo(ByteBuffer.wrap(PAYLOAD));
  }

  @Test
  public void exhaustRetries() {
    expectedException.expect(ShuntingYardException.class);
    expectedException.expectCause(nullValue(Throwable.class));
    when(userRecordResult.isSuccessful()).thenReturn(false, false);
    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    kinesisTask.run();
    verify(producer, times(2)).addUserRecord(captor.capture());
  }

  @Test
  public void taskInterrupted() throws Exception {
    expectedException.expect(ShuntingYardException.class);
    expectedException.expectCause(CoreMatchers.<Throwable> instanceOf(InterruptedException.class));
    reset(future);
    when(future.get()).thenThrow(InterruptedException.class);
    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    kinesisTask.run();
    verify(producer, times(2)).addUserRecord(captor.capture());
  }

  @Test
  public void excutionError() throws Exception {
    expectedException.expect(ShuntingYardException.class);
    expectedException.expectCause(CoreMatchers.<Throwable> instanceOf(ExecutionException.class));
    reset(future);
    when(future.get()).thenThrow(ExecutionException.class);
    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    kinesisTask.run();
    verify(producer, times(2)).addUserRecord(captor.capture());
  }

}
