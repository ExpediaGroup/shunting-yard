/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.objenesis.instantiator.ObjectInstantiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

public class KryoMetaStoreEventSerDe implements MetaStoreEventSerDe {
  private static final Logger log = LoggerFactory.getLogger(KryoMetaStoreEventSerDe.class);

  private final Kryo kryo;

  public KryoMetaStoreEventSerDe() {
    kryo = new Kryo();
  }

  // private void registerSerializers(SimpleModule module) {
  // module.addSerializer(new SkewedInfoSerializer());
  // module.addSerializer(new JacksonThriftSerializer<>(TBase.class));
  // }
  //
  // private void registerDeserializers(SimpleModule module) {
  // module.addDeserializer(SkewedInfo.class, new SkewedInfoDeserializer());
  // }

  @Override
  public byte[] marshal(ListenerEvent listenerEvent) throws MetaException {
    try {
      log.debug("Marshalling event: {}", listenerEvent);
      Output output = new Output(new ByteArrayOutputStream());
      Registration registration = kryo.register(CreateTableEvent.class);
      kryo.register(HMSHandler.class);
      kryo.register(EnvironmentContext.class);

      ImmutableMapSerializer.registerSerializers(kryo);
      kryo.register(ImmutableMap.class, new ImmutableMapSerializer());

      //
      // Objenesis objenesis = new ObjenesisStd();
      //
      // ObjectInstantiator<CreateTableEvent> createTableInstantiator = objenesis
      // .getInstantiatorOf(CreateTableEvent.class);
      //
      // registration.setInstantiator(createTableInstantiator);

      registration.setInstantiator(new ObjectInstantiator<EnvironmentContext>() {

        @Override
        public EnvironmentContext newInstance() {
          EnvironmentContext ctxt = null;
          ctxt = new EnvironmentContext();
          return ctxt;
        }

      });

      registration.setInstantiator(new ObjectInstantiator<CreateTableEvent>() {

        @Override
        public CreateTableEvent newInstance() {
          CreateTableEvent event = null;
          try {
            event = new CreateTableEvent(new Table(), true, new HMSHandler("hello", new HiveConf(), false));
          } catch (MetaException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

          return event;
        }

      });

      registration.setInstantiator(new ObjectInstantiator<HMSHandler>() {

        @Override
        public HMSHandler newInstance() {
          HMSHandler hmsHandler = null;
          try {
            hmsHandler = new HMSHandler("hello", new HiveConf(), false);
          } catch (MetaException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

          return hmsHandler;
        }

      });

      kryo.writeClassAndObject(output, listenerEvent);
      byte[] bytes = output.toBytes();
      if (log.isDebugEnabled()) {
        log.debug("Marshalled event is: {}", new String(bytes));
      }
      return bytes;
    } catch (Exception e) {
      String message = "Unable to marshal event " + listenerEvent;
      log.error(message, e);
      throw new MetaException(message);
    }
  }

  @Override
  public ListenerEvent unmarshal(byte[] payload) throws MetaException {
    try {
      // System.out.println("Marshalled event is: " + new String(payload));
      ByteArrayInputStream buffer = new ByteArrayInputStream(payload);
      Input input = new Input(buffer);
      // As we don't know the type in advance we can only deserialize the event twice:
      // 1. Create a dummy object just to find out the type
      // T genericEvent = (T) kryo.readObject(input, ListenerEvent.class);
      // log.debug("Umarshal event of type: {}", genericEvent.getEventType());
      // 2. Deserialize the actual object

      // T event = mapper.readerFor(genericEvent.getEventType().eventClass()).readValue(buffer);
      ListenerEvent event = (ListenerEvent) kryo.readClassAndObject(input);
      buffer.reset();
      input.close();
      log.debug("Unmarshalled event is: {}", event);
      return event;
    } catch (Exception e) {
      String message = "Unable to unmarshal event from payload";
      System.out.println(message);
      System.out.println(e);
      throw new MetaException(message);
    }
  }

}
