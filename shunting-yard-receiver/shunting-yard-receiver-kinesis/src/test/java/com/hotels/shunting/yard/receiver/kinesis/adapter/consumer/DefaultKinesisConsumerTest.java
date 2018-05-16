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
package com.hotels.shunting.yard.receiver.kinesis.adapter.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;

import com.hotels.shunting.yard.receiver.kinesis.adapter.KinesisRecordBuffer;
import com.hotels.shunting.yard.receiver.kinesis.adapter.WorkerFactory;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKinesisConsumerTest {

  private @Mock KinesisRecordBuffer recordBuffer;
  private @Mock ExecutorService executor;
  private @Mock WorkerFactory workerFactory;
  private @Mock Record record;
  private @Mock Worker worker;

  private DefaultKinesisConsumer consumer;

  @Before
  public void init() {
    when(workerFactory.newWorker()).thenReturn(worker);
    consumer = new DefaultKinesisConsumer(recordBuffer, executor, workerFactory);
  }

  @Test
  public void initialisation() {
    verify(executor).execute(worker);
  }

  @Test
  public void next() {
    when(recordBuffer.get()).thenReturn(record);
    assertThat(consumer.next()).isSameAs(record);
  }

  @Test
  public void nextReadsUntilRecordIsAvailable() {
    when(recordBuffer.get()).thenReturn(null, record);
    assertThat(consumer.next()).isSameAs(record);
  }

  @Test
  public void closeConsmuer() {
    consumer.close();
    verify(worker).shutdown();
  }

}
