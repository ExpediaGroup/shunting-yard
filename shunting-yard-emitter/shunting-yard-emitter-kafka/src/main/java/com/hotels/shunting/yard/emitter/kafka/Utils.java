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
package com.hotels.shunting.yard.emitter.kafka;

import org.apache.hadoop.conf.Configuration;

public final class Utils {

  private Utils() {}

  public static String stringProperty(Configuration conf, KafkaProducerProperty property) {
    return conf.get(property.key(), (String) property.defaultValue());
  }

  public static Integer intProperty(Configuration conf, KafkaProducerProperty property) {
    return conf.getInt(property.key(), (int) property.defaultValue());
  }

  public static Long longProperty(Configuration conf, KafkaProducerProperty property) {
    return conf.getLong(property.key(), (long) property.defaultValue());
  }

}
