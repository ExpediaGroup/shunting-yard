/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.exec.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.ApplicationArguments;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageProperty;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.messaging.MetaStoreEventReader;
import com.hotels.shunting.yard.replicator.exec.receiver.ReplicationMetaStoreEventListener;

@RunWith(MockitoJUnitRunner.class)
public class ReplicationRunnerTest {

  private @Mock ApplicationArguments args;
  private @Mock MetaStoreEventReader eventReader;
  private @Mock ReplicationMetaStoreEventListener listener;
  private @Mock MetaStoreEvent event;

  private final ExecutorService executor = Executors.newFixedThreadPool(1);
  private ReplicationRunner runner;

  @Before
  public void init() {
    runner = new ReplicationRunner(eventReader, listener);
  }

  private class Runner implements Runnable {

    @Override
    public void run() {
      runner.run(args);
    }

  }

  private void runRunner() throws InterruptedException {
    executor.execute(new Runner());
    Thread.sleep(500);
    runner.stop();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void exitCode() {
    assertThat(runner.getExitCode()).isEqualTo(0);
  }

  @Test
  public void onEvent() throws InterruptedException {
    when(eventReader.read()).thenReturn(Optional.of(event));
    when(event.getMessageProperties()).thenReturn(Collections.singletonMap(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE, "receiptHandle"));
    runRunner();
    verify(listener, atLeastOnce()).onEvent(event);
    verify(eventReader, atLeastOnce()).delete("receiptHandle");
  }

  @Test
  public void onEmptyEvent() throws InterruptedException {
    when(eventReader.read()).thenReturn(Optional.empty());
    runRunner();
    verifyZeroInteractions(listener);
    verify(eventReader, times(0)).delete(any(String.class));
  }

  @Test
  public void onEventProcessingFailure() throws InterruptedException {
    when(eventReader.read()).thenThrow(RuntimeException.class);
    runRunner();
    verifyZeroInteractions(listener);
    verify(eventReader, times(0)).delete(any(String.class));
  }

}
