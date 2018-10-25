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

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

public class ApiarySqsMessageSerDe {
  private static final Logger log = LoggerFactory.getLogger(ApiarySqsMessageSerDe.class);

  private final ObjectMapper mapper;
  private final MetaStoreEventSerDe delegateSerDe;

  public ApiarySqsMessageSerDe(MetaStoreEventSerDe delegateSerDe) {
    this.delegateSerDe = delegateSerDe;
    SimpleModule thriftModule = new SimpleModule("ThriftModule");
    registerSerializers(thriftModule);
    registerDeserializers(thriftModule);
    mapper = new ObjectMapper();
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.registerModule(thriftModule);
  }

  private void registerSerializers(SimpleModule module) {
    module.addSerializer(new SkewedInfoSerializer());
    module.addSerializer(new JacksonThriftSerializer<>(TBase.class));
  }

  private void registerDeserializers(SimpleModule module) {
    module.addDeserializer(SkewedInfo.class, new SkewedInfoDeserializer());
  }

  public byte[] marshal(SqsMessage message) throws MetaException {
    try {
      log.debug("Marshalling SqsMessage: {}", message);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      mapper.writer().writeValue(buffer, message);
      byte[] bytes = buffer.toByteArray();
      if (log.isDebugEnabled()) {
        log.debug("Marshalled SqsMessage is: {}", new String(bytes));
      }
      return bytes;
    } catch (IOException e) {
      String errorMessage = "Unable to marshal payload " + message;
      log.error(errorMessage, e);
      throw new MetaException(errorMessage);
    }
  }

  public <T extends SerializableListenerEvent> T unmarshal(byte[] payload) throws MetaException {
    try {
      System.out.println(new String(payload));
      if (log.isDebugEnabled()) {
        log.debug("Unmarshalled payload is: {}", new String(payload));
      }
      ByteArrayInputStream buffer = new ByteArrayInputStream(payload);
      SqsMessage sqsMessage = mapper.readerFor(SqsMessage.class).readValue(buffer);
      T event = delegateSerDe.unmarshal(sqsMessage.getMessage().getBytes());
      return event;
    } catch (Exception e) {
      String message = "Unable to unmarshal event from payload";
      log.error(message, e);
      throw new MetaException(message);
    }

  }

}
