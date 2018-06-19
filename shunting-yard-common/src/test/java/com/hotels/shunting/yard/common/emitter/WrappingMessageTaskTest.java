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
package com.hotels.shunting.yard.common.emitter;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.messaging.MessageTask;

@RunWith(MockitoJUnitRunner.class)
public class WrappingMessageTaskTest {

  private @Mock MessageTask delegate;

  private WrappingMessageTask wrapper;

  @Before
  public void init() {
    wrapper = new WrappingMessageTask(delegate);
  }

  @Test
  public void success() {
    wrapper.run();
    verify(delegate).run();
  }

  @Test
  public void error() {
    doThrow(RuntimeException.class).when(delegate).run();
    wrapper.run();
    verify(delegate).run();
  }

}
