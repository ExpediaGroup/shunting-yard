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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListenerEventUtilsTest {

  private @Mock ListenerEvent event;
  private @Mock EnvironmentContext context;
  private @Mock Map<String, String> properties;

  @Before
  public void init() {
    when(event.getEnvironmentContext()).thenReturn(context);
    when(context.isSetProperties()).thenReturn(true);
    when(context.getProperties()).thenReturn(properties);
  }

  @Test
  public void isCacade() {
    when(properties.get(StatsSetupConst.CASCADE)).thenReturn("true");
    assertThat(ListenerEventUtils.isCascade(event)).isTrue();
  }

  @Test
  public void isNotCacadeWhenContextIsNull() {
    reset(event);
    assertThat(ListenerEventUtils.isCascade(event)).isFalse();
  }

  @Test
  public void isNotCacadeWhenContextPropertiesAreNotSet() {
    reset(context);
    when(context.isSetProperties()).thenReturn(false);
    assertThat(ListenerEventUtils.isCascade(event)).isFalse();
  }

  @Test
  public void isNotCacadeWhenContextPropertyIsNotCascade() {
    when(properties.get(StatsSetupConst.CASCADE)).thenReturn("other");
    assertThat(ListenerEventUtils.isCascade(event)).isFalse();
  }

}
