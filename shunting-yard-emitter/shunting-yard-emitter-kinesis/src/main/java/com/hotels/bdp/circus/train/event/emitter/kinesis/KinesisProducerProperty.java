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
package com.hotels.bdp.circus.train.event.emitter.kinesis;

import com.amazonaws.regions.Regions;

public enum KinesisProducerProperty {
  STREAM("stream", null),
  REGION("region", Regions.US_WEST_2.getName()),
  MAX_CONNECTIONS("max.connections", 1L),
  REQUEST_TIMEOUT("request.timeout", 60000L),
  RECORD_MAX_BUFFERED_TIME("record.max.buffered.time", 15000L),
  RETRIES("retries", 3);

  private static final String PROPERTY_PREFIX = "com.hotels.bdp.circus.train.event.emitter.kinesis.";

  private final String unPrefixedKey;
  private final Object defaultValue;

  private KinesisProducerProperty(String unPrefixedKey, Object defaultValue) {
    this.unPrefixedKey = unPrefixedKey;
    this.defaultValue = defaultValue;
  }

  public String key() {
    return new StringBuffer(PROPERTY_PREFIX).append(unPrefixedKey).toString();
  }

  public String unPrefixedKey() {
    return unPrefixedKey;
  }

  public Object defaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return key();
  }

}
