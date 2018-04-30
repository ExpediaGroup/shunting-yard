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
package com.hotels.bdp.circus.train.event.emitter.sqs.messaging;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQS;

import com.hotels.bdp.circus.train.event.common.messaging.Message;
import com.hotels.bdp.circus.train.event.common.messaging.MessageTask;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageTaskFactoryTest {

  private static final String QUEUE_URL = "queue";
  private static final String GROUP_ID = "group";

  private @Mock Message message;

  @Test
  public void taskType() {
    AmazonSQS producer = mock(AmazonSQS.class);
    MessageTask task = new SqsMessageTaskFactory(QUEUE_URL, GROUP_ID, producer).newTask(message);
    assertThat(task, is(instanceOf(SqsMessageTask.class)));
  }

}
