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
package com.hotels.shunting.yard.receiver.kinesis.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KinesisRecordProcessorFactoryTest {

  private @Mock KinesisRecordBuffer buffer;

  private KinesisRecordProcessorFactory factory;

  @Before
  public void init() {
    factory = new KinesisRecordProcessorFactory(buffer);
  }

  @Test
  public void createProcessor() {
    assertThat(factory.createProcessor()).isInstanceOf(HiveMetaStoreEventRecordProcessor.class);
  }

}
