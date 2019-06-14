/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.common.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.shuntingyard.common.metrics.HiveMetricsHelper;

@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricsFactory.class)
public class HiveMetricsHelperTest {

  private @Mock Metrics metrics;

  @Before
  public void init() throws Exception {
    mockStatic(MetricsFactory.class);
  }

  @Test
  public void nullMetricsFactory() {
    when(MetricsFactory.getInstance()).thenReturn(null);
    assertThat(HiveMetricsHelper.incrementCounter("name")).isNotPresent();
  }

  @Test
  public void exceptionInIncrementCounter() {
    when(MetricsFactory.getInstance()).thenReturn(metrics);
    when(metrics.incrementCounter("name")).thenThrow(RuntimeException.class);
    assertThat(HiveMetricsHelper.incrementCounter("name")).isNotPresent();
  }

  @Test
  public void incrementCounter() {
    when(MetricsFactory.getInstance()).thenReturn(metrics);
    when(metrics.incrementCounter("name")).thenReturn(123L);
    assertThat(HiveMetricsHelper.incrementCounter("name")).get().isEqualTo(123L);
  }

}
