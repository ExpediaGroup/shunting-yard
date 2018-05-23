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

import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.APPLICTION_ID;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.MAX_RECORDS;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.REGION;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.STREAM;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.WORKER_ID;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class WorkerFactoryTest {

  private static final String APPLICTION_ID_VALUE = "application_id";
  private static final String STREAM_VALUE = "stream";
  private static final String WORKER_ID_VALUE = "worker_id";
  private static final int MAX_RECORDS_VALUE = 2;
  private static final String REGION_VALUE = "us-west-2";

  private @Mock KinesisRecordBuffer recordBuffer;
  private @Mock IRecordProcessorFactory recordProcessorFactory;

  private final Configuration conf = new Configuration();

  @Before
  public void init() {
    conf.set(APPLICTION_ID.key(), APPLICTION_ID_VALUE);
    conf.set(STREAM.key(), STREAM_VALUE);
    conf.set(WORKER_ID.key(), WORKER_ID_VALUE);
    conf.setInt(MAX_RECORDS.key(), MAX_RECORDS_VALUE);
    conf.set(REGION.key(), REGION_VALUE);
  }

  @Test
  public void kinesisProperties() {
    KinesisClientLibConfiguration kinesisConfig = WorkerFactory.kinesisProperties(conf);
    assertThat(kinesisConfig.getApplicationName()).isEqualTo(APPLICTION_ID_VALUE);
    assertThat(kinesisConfig.getStreamName()).isEqualTo(STREAM_VALUE);
    assertThat(kinesisConfig.getWorkerIdentifier()).isEqualTo(WORKER_ID_VALUE);
    assertThat(kinesisConfig.getMaxRecords()).isEqualTo(MAX_RECORDS_VALUE);
    assertThat(kinesisConfig.getRegionName()).isEqualTo(REGION_VALUE);
  }

  @Test
  public void kinesisPropertiesMissingWorkerId() {
    conf.unset(WORKER_ID.key());
    KinesisClientLibConfiguration kinesisConfig = WorkerFactory.kinesisProperties(conf);
    assertThat(kinesisConfig.getWorkerIdentifier()).isEqualTo(WORKER_ID.defaultValue());
  }

  @Test
  public void kinesisPropertiesMissingMaxRecord() {
    conf.unset(MAX_RECORDS.key());
    KinesisClientLibConfiguration kinesisConfig = WorkerFactory.kinesisProperties(conf);
    assertThat(kinesisConfig.getMaxRecords()).isEqualTo(MAX_RECORDS.defaultValue());
  }

  @Test
  public void kinesisPropertiesMissingRegion() {
    conf.unset(REGION.key());
    KinesisClientLibConfiguration kinesisConfig = WorkerFactory.kinesisProperties(conf);
    assertThat(kinesisConfig.getRegionName()).isEqualTo(REGION.defaultValue());
  }

  @Test(expected = NullPointerException.class)
  public void kinesisPropertiesMissingApplicationId() {
    conf.unset(APPLICTION_ID.key());
    WorkerFactory.kinesisProperties(conf);
  }

  @Test(expected = NullPointerException.class)
  public void kinesisPropertiesMissingStream() {
    conf.unset(STREAM.key());
    WorkerFactory.kinesisProperties(conf);
  }

  @Test(expected = NullPointerException.class)
  public void buildWithNullConfig() {
    WorkerFactory.builder(null);
  }

  @Test
  public void buildWithRecordBuffer() {
    assertThat(WorkerFactory.builder(conf).recordBuffer(recordBuffer).build()).isNotNull();
  }

  @Test
  public void buildWithRecordProcessorFactory() {
    assertThat(WorkerFactory.builder(conf).recordProcessorFactory(recordProcessorFactory).build()).isNotNull();
  }

  @Test
  public void buildWithNoneRecordBufferAndRecordProcessorFactory() {
    assertThat(WorkerFactory.builder(conf).build()).isNotNull();
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithBothRecordBufferAndRecordProcessorFactory() {
    WorkerFactory.builder(conf).recordBuffer(recordBuffer).recordProcessorFactory(recordProcessorFactory).build();
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithBothRecordProcessorFactoryAndRecordBuffer() {
    WorkerFactory.builder(conf).recordProcessorFactory(recordProcessorFactory).recordBuffer(recordBuffer).build();
  }

  @Test
  public void createWorker() {
    WorkerFactory factory = WorkerFactory.builder(conf).recordProcessorFactory(recordProcessorFactory).build();
    assertThat(factory.newWorker()).isNotNull();
  }

}
