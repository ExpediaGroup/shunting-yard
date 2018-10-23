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
package com.hotels.shunting.yard.emitter.sqs.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.hotels.shunting.yard.common.messaging.Message;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageTaskTest {

  private static final String TOPIC_URL = "topic";
  private static final byte[] PAYLOAD = "payload".getBytes();

  private @Mock AmazonSQS producer;
  private @Mock Message message;

  private SqsMessageTask sqsTask;

  @Before
  public void init() {
    when(message.getPayload()).thenReturn(PAYLOAD);
    sqsTask = new SqsMessageTask(producer, TOPIC_URL, message);
  }

  @Test
  public void typical() {
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);

    sqsTask.run();
    verify(producer).sendMessage(captor.capture());
    assertThat(captor.getValue().getQueueUrl()).isEqualTo(TOPIC_URL);
    assertThat(captor.getValue().getMessageBody()).isEqualTo(new String(PAYLOAD));
  }

}
