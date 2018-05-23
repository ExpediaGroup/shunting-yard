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
package com.hotels.shunting.yard.receiver.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.BOOTSTRAP_SERVERS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.CLIENT_ID;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.GROUP_ID;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class HiveMetaStoreEventKafkaConsumerTest {

  private static final String BOOTSTRAP_SERVERS_STRING = "bootstrap_servers";
  private static final String GROUP_NAME = "group";
  private static final String CLIENT_NAME = "client";

  private final Configuration conf = new Configuration();

  @Before
  public void init() {
    conf.set(BOOTSTRAP_SERVERS.key(), BOOTSTRAP_SERVERS_STRING);
    conf.set(GROUP_ID.key(), GROUP_NAME);
    conf.set(CLIENT_ID.key(), CLIENT_NAME);
  }

  @Test
  public void allMandatoryPropertiesSet() {
    assertThat(HiveMetaStoreEventKafkaConsumer.kafkaProperties(conf)).isNotNull();
  }

  @Test(expected = NullPointerException.class)
  public void missingBootstrapServers() {
    conf.unset(BOOTSTRAP_SERVERS.key());
    HiveMetaStoreEventKafkaConsumer.kafkaProperties(conf);
  }

  @Test(expected = NullPointerException.class)
  public void missingGroupId() {
    conf.unset(GROUP_ID.key());
    HiveMetaStoreEventKafkaConsumer.kafkaProperties(conf);
  }

}
