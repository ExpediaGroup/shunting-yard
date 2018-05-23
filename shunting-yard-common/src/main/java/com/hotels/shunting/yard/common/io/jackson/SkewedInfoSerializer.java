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

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hive.metastore.api.SkewedInfo;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class SkewedInfoSerializer extends StdSerializer<SkewedInfo> {
  private static final long serialVersionUID = 1L;

  public SkewedInfoSerializer() {
    super(SkewedInfo.class);
  }

  @Override
  public void serialize(SkewedInfo skewedInfo, JsonGenerator generator, SerializerProvider serializers)
    throws IOException {
    generator.writeStartObject();
    generator.writeObjectField("skewedColNames", skewedInfo.getSkewedColNames());
    generator.writeObjectField("skewedColValues", skewedInfo.getSkewedColValues());
    if (skewedInfo.getSkewedColValueLocationMaps() != null) {
      generator.writeArrayFieldStart("skewedColValueLocationMaps");
      for (Entry<List<String>, String> entry : skewedInfo.getSkewedColValueLocationMaps().entrySet()) {
        generator.writeStartObject();
        generator.writeObjectField("key", entry.getKey());
        generator.writeObjectField("value", entry.getValue());
        generator.writeEndObject();
      }
      generator.writeEndArray();
    } else {
      generator.writeNullField("skewedColValueLocationMaps");
    }
    generator.writeEndObject();
  }

}
