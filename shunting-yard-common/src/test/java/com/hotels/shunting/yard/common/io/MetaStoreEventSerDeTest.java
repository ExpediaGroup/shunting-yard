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
package com.hotels.shunting.yard.common.io;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.common.io.MetaStoreEventSerDe.serDeForClassName;

import org.junit.Test;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.io.jackson.JsonMetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.jackson.KryoMetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.java.JavaMetaStoreEventSerDe;

public class MetaStoreEventSerDeTest {

  @Test
  public void instantiateJavaSerDe() {
    MetaStoreEventSerDe serDe = serDeForClassName(JavaMetaStoreEventSerDe.class.getName());
    assertThat(serDe).isExactlyInstanceOf(JavaMetaStoreEventSerDe.class);
  }

  @Test
  public void instantiateJsonSerDe() {
    MetaStoreEventSerDe serDe = serDeForClassName(JsonMetaStoreEventSerDe.class.getName());
    assertThat(serDe).isExactlyInstanceOf(JsonMetaStoreEventSerDe.class);
  }

  @Test
  public void instantiateKryoSerDe() {
    MetaStoreEventSerDe serDe = serDeForClassName(KryoMetaStoreEventSerDe.class.getName());
    assertThat(serDe).isExactlyInstanceOf(KryoMetaStoreEventSerDe.class);
  }

  @Test(expected = ShuntingYardException.class)
  public void invalidSerDeClassName() {
    serDeForClassName("com.hotels.shunting.yard.common.io.unknown.MetaStoreEventSerDe");
  }

}
