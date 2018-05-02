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

import static com.hotels.shunting.yard.common.Preconditions.checkNotNull;
import static com.hotels.shunting.yard.common.PropertyUtils.booleanProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.intProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.longProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.stringProperty;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.AUTO_COMMIT_INTERVAL_MS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.BOOTSTRAP_SERVERS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.CLIENT_ID;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.CONNECTIONS_MAX_IDLE_MS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.ENABLE_AUTO_COMMIT;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.FETCH_MAX_BYTES;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.GROUP_ID;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.MAX_POLL_INTERVAL_MS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.MAX_POLL_RECORDS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.RECEIVE_BUFFER_BYTES;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.RECONNECT_BACKOFF_MAX_MS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.RECONNECT_BACKOFF_MS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.RETRY_BACKOFF_MS;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.SESSION_TIMEOUT_MS;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.annotations.VisibleForTesting;

class HiveMetaStoreEventKafkaConsumer extends KafkaConsumer<Long, byte[]> {

  public HiveMetaStoreEventKafkaConsumer(Configuration conf) {
    super(kafkaProperties(conf));
  }

  @VisibleForTesting
  static Properties kafkaProperties(Configuration conf) {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS.unPrefixedKey(),
        checkNotNull(stringProperty(conf, BOOTSTRAP_SERVERS), "Property " + BOOTSTRAP_SERVERS + " is not set"));
    props.put(GROUP_ID.unPrefixedKey(),
        checkNotNull(stringProperty(conf, GROUP_ID), "Property " + GROUP_ID + " is not set"));
    props.put(CLIENT_ID.unPrefixedKey(),
        checkNotNull(stringProperty(conf, CLIENT_ID), "Property " + CLIENT_ID + " is not set"));
    props.put(SESSION_TIMEOUT_MS.unPrefixedKey(), intProperty(conf, SESSION_TIMEOUT_MS));
    props.put(CONNECTIONS_MAX_IDLE_MS.unPrefixedKey(), longProperty(conf, CONNECTIONS_MAX_IDLE_MS));
    props.put(RECONNECT_BACKOFF_MAX_MS.unPrefixedKey(), longProperty(conf, RECONNECT_BACKOFF_MAX_MS));
    props.put(RECONNECT_BACKOFF_MS.unPrefixedKey(), longProperty(conf, RECONNECT_BACKOFF_MS));
    props.put(RETRY_BACKOFF_MS.unPrefixedKey(), longProperty(conf, RETRY_BACKOFF_MS));
    props.put(MAX_POLL_INTERVAL_MS.unPrefixedKey(), intProperty(conf, MAX_POLL_INTERVAL_MS));
    props.put(MAX_POLL_RECORDS.unPrefixedKey(), intProperty(conf, MAX_POLL_RECORDS));
    props.put(ENABLE_AUTO_COMMIT.unPrefixedKey(), booleanProperty(conf, ENABLE_AUTO_COMMIT));
    props.put(AUTO_COMMIT_INTERVAL_MS.unPrefixedKey(), intProperty(conf, AUTO_COMMIT_INTERVAL_MS));
    props.put(FETCH_MAX_BYTES.unPrefixedKey(), intProperty(conf, FETCH_MAX_BYTES));
    props.put(RECEIVE_BUFFER_BYTES.unPrefixedKey(), intProperty(conf, RECEIVE_BUFFER_BYTES));
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return props;
  }

}
