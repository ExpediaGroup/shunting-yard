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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterTableEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

@RunWith(Parameterized.class)
public class MetaStoreEventSerDeTest {

  private static final String DATABASE = "test_db";
  private static final String TABLE = "test_table";
  private static final List<FieldSchema> DATA_COLS = Arrays.asList(new FieldSchema("col", "integer", "comment"));
  private static final List<FieldSchema> PARTITION_COLS = Arrays.asList(new FieldSchema("part", "string", "comment"));

  private static InsertEventRequestData mockInsertEventRequestData() {
    InsertEventRequestData eventRequestData = mock(InsertEventRequestData.class);
    when(eventRequestData.getFilesAdded()).thenReturn(Arrays.asList("file_0000"));
    return eventRequestData;
  }

  private static HMSHandler mockHandler() throws Exception {
    GetTableResult getTableResult = mock(GetTableResult.class);
    when(getTableResult.getTable()).thenReturn(createTable());
    HMSHandler handler = mock(HMSHandler.class);
    when(handler.get_table_req(any(GetTableRequest.class))).thenReturn(getTableResult);
    return handler;
  }

  private static Table createTable(FieldSchema... moreCols) {
    Table table = new Table();
    table.setDbName(DATABASE);
    table.setTableName(TABLE);
    table.setPartitionKeys(PARTITION_COLS);
    table.setSd(new StorageDescriptor());
    List<FieldSchema> cols = new ArrayList<>(DATA_COLS);
    if (moreCols != null) {
      Collections.addAll(cols, moreCols);
    }
    table.getSd().setCols(cols);
    table.getSd().setLocation("hdfs://server:8020/foo/bar/");
    table.setParameters(new HashMap<String, String>());
    table.getParameters().put("foo", "bar");
    return table;
  }

  private static Partition createPartition(String value) {
    Partition partition = new Partition();
    partition.setDbName(DATABASE);
    partition.setTableName(TABLE);
    partition.setValues(Arrays.asList(value));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setCols(DATA_COLS);
    partition.getSd().setLocation("hdfs://server:8020/foo/bar/part=a");
    partition.setParameters(new HashMap<String, String>());
    partition.getParameters().put("foo", "bazz");
    return partition;
  }

  @Parameters
  public static SerializableListenerEvent[] data() throws Exception {
    return new SerializableListenerEvent[] {
        new SerializableCreateTableEvent(new CreateTableEvent(createTable(), true, mockHandler())),
        new SerializableAlterTableEvent(new AlterTableEvent(createTable(),
            createTable(new FieldSchema("new_col", "string", null)), true, mockHandler())),
        new SerializableDropTableEvent(new DropTableEvent(createTable(), true, false, mockHandler())),
        new SerializableAddPartitionEvent(
            new AddPartitionEvent(createTable(), createPartition("a"), true, mockHandler())),
        new SerializableAlterPartitionEvent(
            new AlterPartitionEvent(createPartition("a"), createPartition("b"), createTable(), true, mockHandler())),
        new SerializableDropPartitionEvent(
            new DropPartitionEvent(createTable(), createPartition("a"), true, false, mockHandler())),
        new SerializableInsertEvent(
            new InsertEvent(DATABASE, TABLE, Arrays.asList("a"), mockInsertEventRequestData(), true, mockHandler())) };
  }

  public @Parameter SerializableListenerEvent event;

  private final MetaStoreEventSerDe serDe = new MetaStoreEventSerDe();

  @Test
  public void typical() throws Exception {
    SerializableListenerEvent processedEvent = serDe.unmarshall(serDe.marshall(event));
    assertThat(processedEvent).isEqualTo(event);
  }

}
