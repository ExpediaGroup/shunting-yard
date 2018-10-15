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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.model.Message;

@RunWith(MockitoJUnitRunner.class)
public class MessageDecoderTest {

  private static final String BODY = "payload";

  private @Mock Message message;

  private final MessageDecoder decoder = MessageDecoder.DEFAULT;

  @Before
  public void init() {
    when(message.getBody()).thenReturn(BODY);
  }

  @Test
  public void typical() {
    assertThat(decoder.decode(message)).isEqualTo(BODY.getBytes());
  }

  @Test(expected = NullPointerException.class)
  public void nullBody() {
    reset(message);
    decoder.decode(message);
  }

}
