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
package com.hotels.shunting.yard.common.io.java;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

public class JavaSerializationMetaStoreEventSerDe implements MetaStoreEventSerDe {
  private static final Logger log = LoggerFactory.getLogger(JavaSerializationMetaStoreEventSerDe.class);

  @Override
  public byte[] marshall(SerializableListenerEvent listenerEvent) throws MetaException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
      out.writeObject(listenerEvent);
      return buffer.toByteArray();
    } catch (IOException e) {
      String message = "Unable to serialize event " + listenerEvent;
      log.error(message, e);
      throw new MetaException(message);
    }
  }

  @Override
  public <T extends SerializableListenerEvent> T unmarshall(byte[] payload) throws MetaException {
    ByteArrayInputStream buffer = new ByteArrayInputStream(payload);
    try (ObjectInputStream in = new ObjectInputStream(buffer)) {
      return (T) in.readObject();
    } catch (Exception e) {
      String message = "Unable to deserialize event from payload";
      log.error(message, e);
      throw new MetaException(message);
    }
  }

}
