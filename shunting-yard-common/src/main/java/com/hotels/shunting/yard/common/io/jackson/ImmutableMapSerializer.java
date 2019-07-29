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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Maps;

public class ImmutableMapSerializer extends Serializer<ImmutableMap<Object, ? extends Object>> {
  private static final boolean DOES_NOT_ACCEPT_NULL = true;
  private static final boolean IMMUTABLE = true;

  public ImmutableMapSerializer() {
    super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableMap<Object, ? extends Object> immutableMap) {
    kryo.writeObject(output, Maps.newHashMap(immutableMap));
  }

  @Override
  public ImmutableMap<Object, ? extends Object> read(
      Kryo kryo,
      Input input,
      Class<ImmutableMap<Object, ? extends Object>> type) {
    Map map = kryo.readObject(input, HashMap.class);
    return ImmutableMap.copyOf(map);
  }

  /**
   * Creates a new {@link ImmutableMapSerializer} and registers its serializer for the several ImmutableMap related
   * classes.
   *
   * @param kryo the {@link Kryo} instance to set the serializer on
   */
  public static void registerSerializers(final Kryo kryo) {

    // we're writing a HashMap, therefore we should register it
    kryo.register(java.util.HashMap.class);

    final ImmutableMapSerializer serializer = new ImmutableMapSerializer();

    // ImmutableMap (abstract class)
    // +- EmptyImmutableBiMap
    // +- SingletonImmutableBiMap
    // +- RegularImmutableMap
    // +- ImmutableEnumMap
    // +- RowMap from DenseImmutableTable
    // +- Row from DenseImmutableTable
    // +- ColumnMap from DenseImmutableTable
    // +- Column from DenseImmutableTable

    kryo.register(ImmutableMap.class, serializer);
    kryo.register(ImmutableMap.of().getClass(), serializer);

    Object o1 = new Object();
    Object o2 = new Object();

    kryo.register(ImmutableMap.of(o1, o1).getClass(), serializer);
    kryo.register(ImmutableMap.of(o1, o1, o2, o2).getClass(), serializer);

    Map<DummyEnum, Object> enumMap = new EnumMap<DummyEnum, Object>(DummyEnum.class);
    for (DummyEnum e : DummyEnum.values()) {
      enumMap.put(e, o1);
    }

    kryo.register(ImmutableMap.copyOf(enumMap).getClass(), serializer);

    ImmutableTable<Object, Object, Object> denseImmutableTable = ImmutableTable
        .builder()
        .put("a", 1, 1)
        .put("b", 1, 1)
        .build();

    kryo.register(denseImmutableTable.rowMap().getClass(), serializer); // RowMap
    kryo.register(denseImmutableTable.rowMap().get("a").getClass(), serializer); // Row
    kryo.register(denseImmutableTable.columnMap().getClass(), serializer); // ColumnMap
    kryo.register(denseImmutableTable.columnMap().get(1).getClass(), serializer); // Column
  }

  private enum DummyEnum {
    VALUE1,
    VALUE2
  }
}
