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
package com.hotels.shunting.yard.replicator.exec.event.aggregation;

import static java.util.Arrays.asList;

import static org.apache.hadoop.hive.common.StatsSetupConst.CASCADE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.common.event.EventType.ON_ADD_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_ALTER_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_ALTER_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_CREATE_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_INSERT;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

@RunWith(MockitoJUnitRunner.class)
public class EventMergerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final List<String> PARTITION_COLS = asList("p");
  private static final String QUALIFIED_TABLE_NAME = DATABASE + "." + TABLE;
  private static final List<String> TABLE_PARTITIONS_COLS = asList("p");

  private @Mock MetaStoreEvent eventA, eventB;

  private final EventMerger merger = new EventMerger();

  @Before
  public void init() {
    when(eventA.getDatabaseName()).thenReturn(DATABASE);
    when(eventA.getTableName()).thenReturn(TABLE);
    when(eventA.getQualifiedTableName()).thenReturn(QUALIFIED_TABLE_NAME);
    when(eventA.getPartitionColumns()).thenReturn(TABLE_PARTITIONS_COLS);
    when(eventA.getDatabaseName()).thenReturn(DATABASE);
    when(eventA.getTableName()).thenReturn(TABLE);
    when(eventB.getQualifiedTableName()).thenReturn(QUALIFIED_TABLE_NAME);
    when(eventB.getPartitionColumns()).thenReturn(TABLE_PARTITIONS_COLS);
  }

  @Test
  public void canMergeTwoDropTableEvents() {
    when(eventA.getEventType()).thenReturn(ON_DROP_TABLE);
    when(eventB.getEventType()).thenReturn(ON_DROP_TABLE);
    assertThat(merger.canMerge(eventA, eventB)).isTrue();
  }

  @Test
  public void canMergeTwoDropPartitionEvents() {
    when(eventA.getEventType()).thenReturn(ON_DROP_PARTITION);
    when(eventB.getEventType()).thenReturn(ON_DROP_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isTrue();
  }

  @Test
  public void canMergeCreateTableAndAddPartitionEvents() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isTrue();
  }

  @Test
  public void canMergeCreateTableAndAlterPartitionEvents() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_ALTER_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isTrue();
  }

  @Test
  public void canMergeCreateTableAndInsertEvents() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_INSERT);
    assertThat(merger.canMerge(eventA, eventB)).isTrue();
  }

  @Test
  public void canMergeUnpartitionTableEvents() {
    reset(eventA);
    reset(eventB);
    when(eventA.getQualifiedTableName()).thenReturn(QUALIFIED_TABLE_NAME);
    when(eventB.getQualifiedTableName()).thenReturn(QUALIFIED_TABLE_NAME);
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isTrue();
  }

  @Test
  public void cannotMergeEventsFromDifferentTables() {
    reset(eventB);
    when(eventB.getQualifiedTableName()).thenReturn("db.another_tbl");
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotMergeEventsWithDifferentPartitions() {
    reset(eventB);
    when(eventB.getQualifiedTableName()).thenReturn(QUALIFIED_TABLE_NAME);
    when(eventB.getPartitionColumns()).thenReturn(asList("other_p"));
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotMergeEventsFromPartitionAndUnpartitionedTables() {
    reset(eventB);
    when(eventB.getQualifiedTableName()).thenReturn(QUALIFIED_TABLE_NAME);
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotMergeDropTableAndDropPartition() {
    when(eventA.getEventType()).thenReturn(ON_DROP_TABLE);
    when(eventA.isDropEvent()).thenReturn(true);
    when(eventB.getEventType()).thenReturn(ON_DROP_PARTITION);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotCreateTableEventAndDropTableEvent() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_DROP_TABLE);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotCreateTableEventAndDropPartitionEvent() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventB.getEventType()).thenReturn(ON_DROP_PARTITION);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotAlterTableEventAndDropTableEvent() {
    when(eventA.getEventType()).thenReturn(ON_ALTER_TABLE);
    when(eventB.getEventType()).thenReturn(ON_DROP_TABLE);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotAlterTableEventAndDropPartitionEvent() {
    when(eventA.getEventType()).thenReturn(ON_ALTER_TABLE);
    when(eventB.getEventType()).thenReturn(ON_DROP_PARTITION);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotAddPartitionEventAndDropPartitionEvent() {
    when(eventA.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventB.getEventType()).thenReturn(ON_DROP_PARTITION);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotAddPartitionEventAndDropTableEvent() {
    when(eventA.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventB.getEventType()).thenReturn(ON_DROP_TABLE);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotAlterPartitionEventAndDropPartitionEvent() {
    when(eventA.getEventType()).thenReturn(ON_ALTER_PARTITION);
    when(eventB.getEventType()).thenReturn(ON_DROP_PARTITION);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void cannotAlterPartitionEventAndDropTableEvent() {
    when(eventA.getEventType()).thenReturn(ON_ALTER_PARTITION);
    when(eventB.getEventType()).thenReturn(ON_DROP_TABLE);
    when(eventB.isDropEvent()).thenReturn(true);
    assertThat(merger.canMerge(eventA, eventB)).isFalse();
  }

  @Test
  public void mergeUnpartitionedCreateTableAndAlterTable() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventA.getPartitionColumns()).thenReturn(null);
    when(eventA.getParameters()).thenReturn(ImmutableMap.of("p", "va1"));
    when(eventA.getEnvironmentContext()).thenReturn(ImmutableMap.of("eka1", "eva1"));
    when(eventB.getEventType()).thenReturn(ON_ALTER_TABLE);
    when(eventB.getPartitionColumns()).thenReturn(null);
    when(eventB.getParameters()).thenReturn(ImmutableMap.of("p", "vb1"));
    when(eventB.getEnvironmentContext()).thenReturn(ImmutableMap.of("ekb1", "evb1"));
    MetaStoreEvent event = merger.merge(eventA, eventB);
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", ON_CREATE_TABLE)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", null)
        .hasFieldOrPropertyWithValue("partitionValues", null)
        .hasFieldOrPropertyWithValue("parameters", ImmutableMap.of("p", "vb1"))
        .hasFieldOrPropertyWithValue("environmentContext", ImmutableMap.of("eka1", "eva1", "ekb1", "evb1"));
    assertThat(event.isCascade()).isFalse();
  }

  @Test
  public void mergeCreateTableAndAddPartition() {
    when(eventA.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(eventA.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventA.getParameters()).thenReturn(ImmutableMap.of("pa1", "va1"));
    when(eventA.getEnvironmentContext()).thenReturn(ImmutableMap.of("eka1", "eva1"));
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventB.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventB.getPartitionValues()).thenReturn(asList(asList("b1")));
    when(eventB.getParameters()).thenReturn(ImmutableMap.of("pb1", "vb1"));
    when(eventB.getEnvironmentContext()).thenReturn(ImmutableMap.of("ekb1", "evb1"));
    MetaStoreEvent event = merger.merge(eventA, eventB);
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", ON_CREATE_TABLE)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", PARTITION_COLS)
        .hasFieldOrPropertyWithValue("partitionValues", asList(asList("b1")))
        .hasFieldOrPropertyWithValue("parameters", ImmutableMap.of("pa1", "va1", "pb1", "vb1"))
        .hasFieldOrPropertyWithValue("environmentContext", ImmutableMap.of("eka1", "eva1", "ekb1", "evb1"));
    assertThat(event.isCascade()).isFalse();
  }

  @Test
  public void mergeTwoAddPartitionEvents() {
    when(eventA.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventA.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventA.getPartitionValues()).thenReturn(asList(asList("a1")));
    when(eventA.getParameters()).thenReturn(ImmutableMap.of("pa1", "va1"));
    when(eventA.getEnvironmentContext()).thenReturn(ImmutableMap.of("eka1", "eva1"));
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventB.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventB.getPartitionValues()).thenReturn(asList(asList("b1")));
    when(eventB.getParameters()).thenReturn(ImmutableMap.of("pb1", "vb1"));
    when(eventB.getEnvironmentContext()).thenReturn(ImmutableMap.of("ekb1", "evb1"));
    MetaStoreEvent event = merger.merge(eventA, eventB);
    assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", ON_ADD_PARTITION)
        .hasFieldOrPropertyWithValue("databaseName", DATABASE)
        .hasFieldOrPropertyWithValue("tableName", TABLE)
        .hasFieldOrPropertyWithValue("partitionColumns", PARTITION_COLS)
        .hasFieldOrPropertyWithValue("partitionValues", asList(asList("a1"), asList("b1")))
        .hasFieldOrPropertyWithValue("parameters", ImmutableMap.of("pa1", "va1", "pb1", "vb1"))
        .hasFieldOrPropertyWithValue("environmentContext", ImmutableMap.of("eka1", "eva1", "ekb1", "evb1"));
    assertThat(event.isCascade()).isFalse();
  }

  @Test
  public void propagateCascadePropertyWhenPreviousEventIsCascade() {
    when(eventA.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventA.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventA.getPartitionValues()).thenReturn(asList(asList("a1")));
    when(eventA.getEnvironmentContext()).thenReturn(ImmutableMap.of(CASCADE, "true"));
    when(eventA.isCascade()).thenReturn(true);
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventB.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventB.getPartitionValues()).thenReturn(asList(asList("b1")));
    when(eventB.getEnvironmentContext()).thenReturn(ImmutableMap.of(CASCADE, "false"));
    MetaStoreEvent event = merger.merge(eventA, eventB);
    assertThat(event.isCascade()).isTrue();
  }

  @Test
  public void propagateCascadePropertyWhenLaterEventIsCascade() {
    when(eventA.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventA.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventA.getPartitionValues()).thenReturn(asList(asList("a1")));
    when(eventA.getEnvironmentContext()).thenReturn(ImmutableMap.of(CASCADE, "false"));
    when(eventB.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(eventB.getPartitionColumns()).thenReturn(PARTITION_COLS);
    when(eventB.getPartitionValues()).thenReturn(asList(asList("b1")));
    when(eventB.getEnvironmentContext()).thenReturn(ImmutableMap.of(CASCADE, "true"));
    when(eventB.isCascade()).thenReturn(true);
    MetaStoreEvent event = merger.merge(eventA, eventB);
    assertThat(event.isCascade()).isTrue();
  }

}
