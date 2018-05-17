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
package com.hotels.shunting.yard.common.messaging;

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

public interface MessageReaderFactory {

  final static MessageReaderFactory DEFAULT = new MessageReaderFactory() {
    @Override
    public MessageReader create(String className, Configuration conf, MetaStoreEventSerDe metaStoreEventSerDe) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends MessageReader> clazz = (Class<? extends MessageReader>) Class.forName(className);
        Constructor<? extends MessageReader> constructor = clazz.getConstructor(Configuration.class,
            MetaStoreEventSerDe.class);
        return constructor.newInstance(conf, metaStoreEventSerDe);
      } catch (Exception e) {
        throw new ShuntingYardException("Unable to instantiate a MessageReader of class " + className, e);
      }
    }

  };

  MessageReader create(String className, Configuration conf, MetaStoreEventSerDe metaStoreEventSerDe);

}
