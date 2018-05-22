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

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@SuppressWarnings("rawtypes")
public class JacksonThriftSerializer<T extends TBase> extends StdSerializer<T> {
  private static final long serialVersionUID = 1L;

  protected JacksonThriftSerializer(Class<T> clazz) {
    super(clazz);
  }

  @Override
  public void serialize(T t, JsonGenerator generator, SerializerProvider serializers) throws IOException {
    TFieldIdEnum[] fields = fields(t);
    if (fields != null) {
      serialize(t, fields, generator, serializers);
    } else {
      generator.writeObject(t);
    }
  }

  private void serialize(T t, TFieldIdEnum[] fields, JsonGenerator generator, SerializerProvider serializers)
    throws IOException {
    generator.writeStartObject();
    for (TFieldIdEnum fieldId : fields) {
      Object value = t.getFieldValue(fieldId);
      String name = fieldId.getFieldName();
      generator.writeObjectField(name, value);
    }
    generator.writeEndObject();
  }

  private <E extends TFieldIdEnum> E[] fields(T value) {
    for (Class<?> clazz : value.getClass().getDeclaredClasses()) {
      if (TFieldIdEnum.class.isAssignableFrom(clazz)) {
        return (E[]) clazz.getEnumConstants();
      }
    }
    return null;
  }

}
