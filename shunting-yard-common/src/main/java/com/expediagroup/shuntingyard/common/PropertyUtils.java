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
package com.expediagroup.shuntingyard.common;

import org.apache.hadoop.conf.Configuration;

public class PropertyUtils {

  private PropertyUtils() {}

  public static String stringProperty(Configuration conf, Property property) {
    return conf.get(property.key(), (String) property.defaultValue());
  }

  public static boolean booleanProperty(Configuration conf, Property property) {
    return conf.getBoolean(property.key(), (boolean) property.defaultValue());
  }

  public static int intProperty(Configuration conf, Property property) {
    return conf.getInt(property.key(), (int) property.defaultValue());
  }

  public static long longProperty(Configuration conf, Property property) {
    return conf.getLong(property.key(), (long) property.defaultValue());
  }

}
