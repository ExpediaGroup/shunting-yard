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

import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.jackson.JsonMetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.java.JavaMetaStoreEventSerDe;

public enum SerDeClass {

  JAVA(JavaMetaStoreEventSerDe.class),
  JSON(JsonMetaStoreEventSerDe.class);

  private final Class<? extends MetaStoreEventSerDe> serDeClass;

  private SerDeClass(Class<? extends MetaStoreEventSerDe> serDeClass) {
    this.serDeClass = serDeClass;
  }

  public MetaStoreEventSerDe instantiate() {
    return MetaStoreEventSerDe.serDeForClassName(serDeClass.getName());
  }

}
