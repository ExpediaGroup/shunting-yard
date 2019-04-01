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
package com.hotels.shunting.yard.replicator.exec.event;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expedia.apiary.extensions.receiver.common.event.EventType.CREATE_TABLE;

import java.util.Arrays;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class MetaStoreEventTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final String REPLICA_DATABASE = "replica_db";
  private static final String REPLICA_TABLE = "replica_tbl";

  @Test(expected = NullPointerException.class)
  public void missingEventType() {
    MetaStoreEvent.builder(null, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE);
  }

  @Test(expected = NullPointerException.class)
  public void missingDatabaseName() {
    MetaStoreEvent.builder(CREATE_TABLE, null, TABLE, REPLICA_DATABASE, REPLICA_TABLE);
  }

  @Test(expected = NullPointerException.class)
  public void missingTableName() {
    MetaStoreEvent.builder(CREATE_TABLE, DATABASE, null, REPLICA_DATABASE, REPLICA_TABLE);
  }

  @Test(expected = IllegalStateException.class)
  public void callPartitionColumnsMoreThanOnce() {
    MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(Arrays.asList("p"))
        .partitionColumns(Arrays.asList("p"));
  }

  @Test(expected = NullPointerException.class)
  public void callPartitionColumnsWithNullValue() {
    MetaStoreEvent.builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE).partitionColumns(null);
  }

  @Test(expected = IllegalStateException.class)
  public void callPartitionValuesWithOutSetting() {
    MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionValues(Arrays.asList("p1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void callPartitionValuesWithFewerValuesThanPartitionColums() {
    MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(Arrays.asList("p", "q"))
        .partitionValues(Arrays.asList("p1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void callPartitionValuesWithMoreValuesThanPartitionColums() {
    MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(Arrays.asList("p"))
        .partitionValues(Arrays.asList("p1", "q1"));
  }

  @Test(expected = NullPointerException.class)
  public void callPartitionValuesWithNull() {
    MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(Arrays.asList("p"))
        .partitionValues(null);
  }

  @Test
  public void basicUnpartitionedTableEvent() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .build();
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", CREATE_TABLE)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", null)
        .hasFieldOrPropertyWithValue("partitionValues", null)
        .hasFieldOrPropertyWithValue("parameters", null)
        .hasFieldOrPropertyWithValue("environmentContext", null);
  }

  @Test
  public void fullUnpartitionedTableEvent() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .parameter("key", "value")
        .parameters(ImmutableMap.of("moreKeys", "moreValues"))
        .environmentContext(ImmutableMap.of("envKeys", "envValues"))
        .build();
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", CREATE_TABLE)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", null)
        .hasFieldOrPropertyWithValue("partitionValues", null)
        .hasFieldOrPropertyWithValue("parameters", ImmutableMap.of("key", "value", "moreKeys", "moreValues"))
        .hasFieldOrPropertyWithValue("environmentContext", ImmutableMap.of("envKeys", "envValues"));
  }

  @Test
  public void basicPartitionedTableEvent() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(Arrays.asList("a", "b"))
        .partitionValues(Arrays.asList("a1", "b1"))
        .partitionValues(Arrays.asList("a2", "b2"))
        .build();
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", CREATE_TABLE)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", Arrays.asList("a", "b"))
        .hasFieldOrPropertyWithValue("partitionValues",
            Arrays.asList(Arrays.asList("a1", "b1"), Arrays.asList("a2", "b2")))
        .hasFieldOrPropertyWithValue("parameters", null)
        .hasFieldOrPropertyWithValue("environmentContext", null);
  }

  @Test
  public void fullPartitionedTableEvent() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(Arrays.asList("p"))
        .partitionValues(Arrays.asList("p1"))
        .parameter("key", "value")
        .parameters(ImmutableMap.of("moreKeys", "moreValues"))
        .environmentContext(ImmutableMap.of("envKeys", "envValues"))
        .build();
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", CREATE_TABLE)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", Arrays.asList("p"))
        .hasFieldOrPropertyWithValue("partitionValues", Arrays.asList(Arrays.asList("p1")))
        .hasFieldOrPropertyWithValue("parameters", ImmutableMap.of("key", "value", "moreKeys", "moreValues"))
        .hasFieldOrPropertyWithValue("environmentContext", ImmutableMap.of("envKeys", "envValues"));
  }

  @Test
  public void callEnviromentPropertiesMoreThanOnce() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(ImmutableMap.of("envKey1", "envValue1"))
        .environmentContext(ImmutableMap.of("envKey2", "envValue2"))
        .build();
    assertThat(event)
        .hasFieldOrPropertyWithValue("environmentContext",
            ImmutableMap.of("envKey1", "envValue1", "envKey2", "envValue2"));
  }

  @Test
  public void isCascade() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(ImmutableMap.of(StatsSetupConst.CASCADE, "true"))
        .build();
    assertThat(event.isCascade()).isTrue();
  }

  @Test
  public void isNotCascade() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(ImmutableMap.of("p", "v"))
        .build();
    assertThat(event.isCascade()).isFalse();
  }

  @Test
  public void isNotCascadeIfEnvironmentContextIsNotSet() {
    MetaStoreEvent event = MetaStoreEvent
        .builder(CREATE_TABLE, DATABASE, TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .build();
    assertThat(event.isCascade()).isFalse();
  }

}
