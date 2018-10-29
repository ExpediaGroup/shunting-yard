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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventDeserializer;

public class ApiarySqsMessageDeserializer {
  private static final Logger log = LoggerFactory.getLogger(ApiarySqsMessageDeserializer.class);

  private final ObjectMapper mapper = new ObjectMapper();;
  private final MetaStoreEventDeserializer delegateSerDe;

  public ApiarySqsMessageDeserializer(MetaStoreEventDeserializer delegateSerDe) {
    this.delegateSerDe = delegateSerDe;
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public <T extends ListenerEvent> T unmarshal(String payload) throws ShuntingYardException {
    try {
      log.debug("Unmarshalled payload is: {}", payload);
      SqsMessage sqsMessage = mapper.readerFor(SqsMessage.class).readValue(payload);
      T event = delegateSerDe.unmarshal(sqsMessage.getMessage());
      return event;
    } catch (Exception e) {
      String message = "Unable to unmarshal event from payload";
      throw new ShuntingYardException(message, e);
    }

  }

}
