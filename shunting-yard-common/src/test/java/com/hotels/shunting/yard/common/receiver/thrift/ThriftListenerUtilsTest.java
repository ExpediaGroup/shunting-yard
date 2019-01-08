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
package com.hotels.shunting.yard.common.receiver.thrift;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.base.Joiner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftListenerUtilsTest {

  private static final String DATABASE = "test_db";
  private static final String TABLE = "test_table";
  private static final List<FieldSchema> DATA_COLS = Arrays.asList(new FieldSchema("col", "integer", "comment"));

  private static Table createTable(FieldSchema... partCols) {
    Table table = new Table();
    table.setDbName(DATABASE);
    table.setTableName(TABLE);
    table.setPartitionKeys(Arrays.asList(partCols));
    table.setSd(new StorageDescriptor());
    List<FieldSchema> cols = new ArrayList<>(DATA_COLS);
    table.getSd().setCols(cols);
    table.getSd().setLocation("hdfs://server:8020/foo/bar/");
    table.setParameters(new HashMap<String, String>());
    table.getParameters().put("foo", "bar");
    return table;
  }

  private static Partition createPartition(String... values) {
    Partition partition = new Partition();
    partition.setDbName(DATABASE);
    partition.setTableName(TABLE);
    partition.setValues(Arrays.asList(values));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setCols(DATA_COLS);
    partition.getSd().setLocation("hdfs://server:8020/foo/bar/" + Joiner.on("/").join(values));
    partition.setParameters(new HashMap<String, String>());
    partition.getParameters().put("foo", "bazz");
    return partition;
  }

  public @Rule ExpectedException exception = ExpectedException.none();

  @Test
  public void singlePartitionCol() {
    List<ObjectPair<Integer, byte[]>> pairs = ThriftListenerUtils
        .toObjectPairs(createTable(new FieldSchema("part", "string", null)),
            Arrays.asList(createPartition("a"), createPartition("b")));
    assertThat(pairs.size()).isEqualTo(2);
    for (ObjectPair<Integer, byte[]> pair : pairs) {
      assertThat(pair.getFirst()).isEqualTo(1);
    }
  }

  @Test
  public void twoPartitionCols() {
    List<ObjectPair<Integer, byte[]>> pairs = ThriftListenerUtils
        .toObjectPairs(createTable(new FieldSchema("part_1", "string", null), new FieldSchema("part_2", "int", null)),
            Arrays.asList(createPartition("a", "1"), createPartition("b", "2")));
    assertThat(pairs.size()).isEqualTo(2);
    for (ObjectPair<Integer, byte[]> pair : pairs) {
      assertThat(pair.getFirst()).isEqualTo(2);
    }
  }

  @Test(expected = RuntimeException.class)
  public void moreValuesThanPartitionCols() {
    ThriftListenerUtils
        .toObjectPairs(createTable(new FieldSchema("part", "string", null)), Arrays.asList(createPartition("a", "1")));
  }

}
