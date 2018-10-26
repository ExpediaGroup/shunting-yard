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

import java.io.StringReader;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventDeserializer;

public class JsonMetaStoreEventDeserializer implements MetaStoreEventDeserializer {
  private static final Logger log = LoggerFactory.getLogger(JsonMetaStoreEventDeserializer.class);

  private final ObjectMapper mapper = new ObjectMapper();

  public JsonMetaStoreEventDeserializer() {
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public <T extends ListenerEvent> T unmarshal(String payload) throws MetaException {
    try {
      log.debug("Marshalled event is: {}", payload);
      StringReader buffer = new StringReader(payload);

      // As we don't know the type in advance we can only deserialize the event twice:
      // 1. Create a dummy object just to find out the type
      T genericEvent = mapper.readerFor(HelperSerializableListenerEvent.class).readValue(buffer);
      log.debug("Unmarshal event of type: {}", genericEvent.getEventType());
      buffer.reset();
      // 2. Deserialize the actual object
      T event = mapper.readerFor(genericEvent.getEventType().eventClass()).readValue(buffer);
      log.debug("Unmarshalled event is: {}", event);
      return event;
    } catch (Exception e) {
      String message = "Unable to unmarshal event from payload";
      log.error(message, e);
      throw new MetaException(message);
    }
  }

  static class HelperSerializableListenerEvent extends ListenerEvent {
    private static final long serialVersionUID = 1L;

    private EventType eventType;

    HelperSerializableListenerEvent() {}

    @Override
    public EventType getEventType() {
      return eventType;
    }

    public void setEventType(EventType eventType) {
      this.eventType = eventType;
    }

    @Override
    public String getDbName() {
      return null;
    }

    @Override
    public String getTableName() {
      return null;
    }
  }

}
