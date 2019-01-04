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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.ApplicationArguments;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.messaging.MetaStoreEventReader;
import com.hotels.shunting.yard.replicator.exec.receiver.ReplicationMetaStoreEventListener;

@RunWith(MockitoJUnitRunner.class)
public class ReplicationRunnerTest {

  private @Mock ApplicationArguments args;
  private @Mock MetaStoreEventReader eventReader;
  private @Mock ReplicationMetaStoreEventListener listener;
  private @Mock MetaStoreEvent event;

  private ReplicationRunner runner;

  @Before
  public void init() {
    when(eventReader.next()).thenReturn(Optional.of(event));
    runner = new ReplicationRunner(eventReader, listener);
  }

  @Test
  public void exitCode() {
    assertThat(runner.getExitCode()).isEqualTo(0);
  }

  @Ignore("TODO: kick runner of in its own thread, then wait for counter for a certain amount of time and either timeout or assert results")
  @Test
  public void onEvent() {
    //TODO: need to kick runner off in its own thread
    runner.run(args);
    runner.stop();
    verify(listener).onEvent(event);
    verifyNoMoreInteractions(listener);
  }

  @Ignore("TODO: kick runner of in its own thread, then wait for counter for a certain amount of time and either timeout or assert results")
  @Test
  public void onEventProcessingFailure() {
    when(eventReader.next()).thenThrow(RuntimeException.class);
    runner.run(args);
    runner.stop();
    verifyNoMoreInteractions(listener);
  }

}
