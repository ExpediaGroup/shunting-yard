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
package com.hotels.shunting.yard.common.io.jackson;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.SkewedInfo;

class TestUtils {

  private static final Collector<CharSequence, ?, String> COMMA_COLLECTOR = Collectors.joining(",");

  static String toJson(SkewedInfo skewedInfo) {
    List<String> fields = new ArrayList<>();
    if (skewedInfo.getSkewedColNames() != null) {
      String field = new StringBuilder("'skewedColNames':[")
          .append(skewedInfo.getSkewedColNames().stream().map(s -> "'" + s + "'").collect(COMMA_COLLECTOR))
          .append("]")
          .toString();
      fields.add(field);
    } else {
      fields.add("'skewedColNames':null");
    }
    if (skewedInfo.getSkewedColValues() != null) {
      String field = new StringBuilder("'skewedColValues':[")
          .append(skewedInfo.getSkewedColValues().stream().map(vals -> {
            return new StringBuilder("[")
                .append(vals.stream().map(s -> "'" + s + "'").collect(COMMA_COLLECTOR))
                .append("]")
                .toString();
          }).collect(COMMA_COLLECTOR))
          .append("]")
          .toString();
      fields.add(field);
    } else {
      fields.add("'skewedColValues':null");
    }
    if (skewedInfo.getSkewedColValueLocationMaps() != null) {
      String field = new StringBuilder("'skewedColValueLocationMaps':[")
          .append(skewedInfo.getSkewedColValueLocationMaps().entrySet().stream().map(entry -> {
            return new StringBuilder("{'key':[")
                .append(entry.getKey().stream().map(s -> "'" + s + "'").collect(COMMA_COLLECTOR))
                .append("],'value':'")
                .append(entry.getValue())
                .append("'}")
                .toString();
          }).collect(COMMA_COLLECTOR))
          .append("]")
          .toString();
      fields.add(field);
    } else {
      fields.add("'skewedColValueLocationMaps':null");
    }
    String json = new StringBuilder("{")
        .append(fields.stream().collect(COMMA_COLLECTOR))
        .append("}")
        .toString()
        .replaceAll("'", "\"");
    return json;
  }

}
