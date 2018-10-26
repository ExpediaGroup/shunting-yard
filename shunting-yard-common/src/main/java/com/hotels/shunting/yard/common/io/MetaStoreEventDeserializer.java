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
package com.hotels.shunting.yard.common.io;

import org.apache.hadoop.hive.metastore.api.MetaException;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.ListenerEvent;

public interface MetaStoreEventDeserializer {

  static <T extends MetaStoreEventDeserializer> T serDeForClassName(String className) {
    try {
      Class<T> clazz = (Class<T>) Class.forName(className);
      return clazz.newInstance();
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to instantiate MetaStoreEventSerDe of class " + className, e);
    }
  }

  <T extends ListenerEvent> T unmarshal(String payload) throws MetaException;

}
