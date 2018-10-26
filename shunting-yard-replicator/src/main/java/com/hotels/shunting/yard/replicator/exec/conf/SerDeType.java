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
package com.hotels.shunting.yard.replicator.exec.conf;

import com.hotels.shunting.yard.common.io.MetaStoreEventDeserializer;
import com.hotels.shunting.yard.common.io.jackson.JsonMetaStoreEventDeserializer;

public enum SerDeType {
  JSON(JsonMetaStoreEventDeserializer.class);

  private final Class<? extends MetaStoreEventDeserializer> serDeClass;

  private SerDeType(Class<? extends MetaStoreEventDeserializer> serDeClass) {
    this.serDeClass = serDeClass;
  }

  public MetaStoreEventDeserializer instantiate() {
    return MetaStoreEventDeserializer.serDeForClassName(serDeClass.getName());
  }

}
