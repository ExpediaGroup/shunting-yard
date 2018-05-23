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
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

public class JsonMetaStoreEventSerDe implements MetaStoreEventSerDe {
  private static final Logger log = LoggerFactory.getLogger(JsonMetaStoreEventSerDe.class);

  private final ObjectMapper mapper;

  public JsonMetaStoreEventSerDe() {
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

  @Override
  public byte[] marshal(SerializableListenerEvent listenerEvent) throws MetaException {
    try {
      log.debug("Marshalling event: {}", listenerEvent);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      mapper.writer().writeValue(buffer, listenerEvent);
      byte[] bytes = buffer.toByteArray();
      if (log.isDebugEnabled()) {
        log.debug("Marshalled event is: {}", new String(bytes));
      }
      return bytes;
    } catch (IOException e) {
      String message = "Unable to marhsal event " + listenerEvent;
      log.error(message, e);
      throw new MetaException(message);
    }
  }

  @Override
  public <T extends SerializableListenerEvent> T unmarshal(byte[] payload) throws MetaException {
    try {
      if (log.isDebugEnabled()) {
        log.debug("Marshalled event is: {}", new String(payload));
      }
      ByteArrayInputStream buffer = new ByteArrayInputStream(payload);
      // As we don't know the type in advance we can only deserialize the event twice:
      // 1. Create a dummy object just to find out the type
      T genericEvent = mapper.readerFor(HeplerSerializableListenerEvent.class).readValue(buffer);
      log.debug("Umarshal event of type: {}", genericEvent.getEventType());
      // 2. Deserialize the actual object
      buffer.reset();
      T event = mapper.readerFor(genericEvent.getEventType().eventClass()).readValue(buffer);
      log.debug("Unmarshalled event is: {}", event);
      return event;
    } catch (Exception e) {
      String message = "Unable to unmarshal event from payload";
      log.error(message, e);
      throw new MetaException(message);
    }
  }

  static class HeplerSerializableListenerEvent extends SerializableListenerEvent {
    private static final long serialVersionUID = 1L;
    private static final ListenerEvent DUMMY_EVENT = new CreateTableEvent(null, false, null);

    private EventType eventType;

    HeplerSerializableListenerEvent() {
      super(DUMMY_EVENT);
    }

    @Override
    public EventType getEventType() {
      return eventType;
    }

    public void setEventType(EventType eventType) {
      this.eventType = eventType;
    }

    @Override
    public String getDatabaseName() {
      return null;
    }

    @Override
    public String getTableName() {
      return null;
    }
  }

}
