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

import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.metastore.api.SkewedInfo;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class SkewedInfoDeserializer extends StdDeserializer<SkewedInfo> {
  private static final long serialVersionUID = 1L;

  private static final TypeReference<List<String>> SKEWED_COL_NAMES_TYPE_REF = new TypeReference<List<String>>() {};
  private static final TypeReference<List<List<String>>> SKEWED_COL_VALUES_TYPE_REF = new TypeReference<List<List<String>>>() {};
  private static final TypeReference<List<DummyMapEntry<List<String>, String>>> SKEWED_COL_VALUE_LOCATION_MAPS_TYPE_REF = new TypeReference<List<DummyMapEntry<List<String>, String>>>() {};

  public SkewedInfoDeserializer() {
    super(SkewedInfo.class);
  }

  @Override
  public SkewedInfo deserialize(JsonParser parser, DeserializationContext context)
    throws IOException, JsonProcessingException {
    SkewedInfo skewedInfo = new SkewedInfo();
    JsonToken currentToken = null;
    while ((currentToken = parser.nextValue()) != null && currentToken != END_OBJECT) {
      switch (currentToken) {
      case START_ARRAY:
        switch (parser.getCurrentName()) {
        case "skewedColNames":
          skewedInfo.setSkewedColNames(parser.readValueAs(SKEWED_COL_NAMES_TYPE_REF));
          break;
        case "skewedColValues":
          skewedInfo.setSkewedColValues(parser.readValueAs(SKEWED_COL_VALUES_TYPE_REF));
          break;
        case "skewedColValueLocationMaps":
          List<Entry<List<String>, String>> mapEntries = parser.readValueAs(SKEWED_COL_VALUE_LOCATION_MAPS_TYPE_REF);
          Map<List<String>, String> map = new HashMap<>();
          for (Entry<List<String>, String> entry : mapEntries) {
            map.put(entry.getKey(), entry.getValue());
          }
          skewedInfo.setSkewedColValueLocationMaps(map);
          break;
        }
        break;
      default:
        break;
      }
    }
    return skewedInfo;
  }

}
