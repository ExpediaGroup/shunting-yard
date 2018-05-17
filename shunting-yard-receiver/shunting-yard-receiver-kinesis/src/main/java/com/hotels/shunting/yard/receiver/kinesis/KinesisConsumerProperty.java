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
package com.hotels.shunting.yard.receiver.kinesis;

import static java.util.concurrent.TimeUnit.SECONDS;

import static com.amazonaws.regions.Regions.US_WEST_2;

import com.hotels.shunting.yard.common.Property;

public enum KinesisConsumerProperty implements Property {
  STREAM("stream", null),
  REGION("region", US_WEST_2.getName()),
  APPLICTION_ID("application.id", null),
  WORKER_ID("worker.id", "CircusTrainEventDrivenReceiver"),
  MAX_RECORDS("max.records", 30),
  BUFFER_CAPACITY("buffer.capacity", 100),
  POLLING_TIMEOUT_MS("polling.timeout.ms", SECONDS.toMillis(30));

  private static final String PROPERTY_PREFIX = "com.hotels.shunting.yard.event.receiver.kinesis.";

  private final String unPrefixedKey;
  private final Object defaultValue;

  private KinesisConsumerProperty(String unPrefixedKey, Object defaultValue) {
    this.unPrefixedKey = unPrefixedKey;
    this.defaultValue = defaultValue;
  }

  @Override
  public String key() {
    return new StringBuffer(PROPERTY_PREFIX).append(unPrefixedKey).toString();
  }

  @Override
  public String unPrefixedKey() {
    return unPrefixedKey;
  }

  @Override
  public Object defaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return key();
  }

}
