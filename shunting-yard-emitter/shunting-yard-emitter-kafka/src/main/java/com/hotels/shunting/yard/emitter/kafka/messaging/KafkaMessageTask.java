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
package com.hotels.bdp.circus.train.event.emitter.kafka.messaging;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.hotels.bdp.circus.train.event.common.messaging.Message;
import com.hotels.bdp.circus.train.event.common.messaging.MessageTask;

class KafkaMessageTask implements MessageTask {
  private final Producer<Long, byte[]> producer;
  private final String topic;
  private final Message message;
  private final int numberOfPartitions;

  KafkaMessageTask(Producer<Long, byte[]> producer, String topic, int numberOfPartitions, Message message) {
    this.producer = producer;
    this.topic = topic;
    this.numberOfPartitions = numberOfPartitions;
    this.message = message;
  }

  @Override
  public void run() {
    producer.send(new ProducerRecord<>(topic, partition(), message.getTimestamp(), message.getPayload()));
  }

  private int partition() {
    return Math.abs(message.getQualifiedTableName().hashCode() % numberOfPartitions);
  }

}
