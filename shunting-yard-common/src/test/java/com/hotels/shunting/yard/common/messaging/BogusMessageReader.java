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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

class BogusMessageReader implements MessageReader {

  public BogusMessageReader(Configuration conf, MetaStoreEventSerDe serDe) {
    throw new RuntimeException("You cannot construct me");
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public SerializableListenerEvent next() {
    return null;
  }

  @Override
  public void close() throws IOException {}

}
