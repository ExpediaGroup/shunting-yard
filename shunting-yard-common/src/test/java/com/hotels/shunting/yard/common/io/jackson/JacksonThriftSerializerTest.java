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
package com.hotels.shunting.yard.common.io.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createEnvironmentContext;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createPartition;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createTable;

import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class JacksonThriftSerializerTest {
  private ObjectMapper mapper;

  @Before
  public void init() {
    JacksonThriftSerializer<TBase> serializer = new JacksonThriftSerializer<>(TBase.class);
    SimpleModule testModule = new SimpleModule("testModule");
    testModule.addSerializer(serializer);
    mapper = new ObjectMapper();
    mapper.registerModule(testModule);
  }

  @Test
  public void serializeTable() throws Exception {
    Table table = createTable();
    String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(table);
    assertThat(json).isEqualTo(new TextStringBuilder()
        .appendln("{")
        .appendln("  'tableName' : 'test_table',")
        .appendln("  'dbName' : 'test_db',")
        .appendln("  'owner' : 'shunting_yard',")
        .appendln("  'createTime' : 1526391934,")
        .appendln("  'lastAccessTime' : 1526458642,")
        .appendln("  'retention' : 5,")
        .appendln("  'sd' : {")
        .appendln("    'cols' : [ {")
        .appendln("      'name' : 'col',")
        .appendln("      'type' : 'integer',")
        .appendln("      'comment' : 'comment'")
        .appendln("    } ],")
        .appendln("    'location' : 'hdfs://server:8020/foo/bar/',")
        .appendln("    'inputFormat' : 'INPUT_FORMAT',")
        .appendln("    'outputFormat' : 'OUTPUT_FORMAT',")
        .appendln("    'compressed' : true,")
        .appendln("    'numBuckets' : 3,")
        .appendln("    'serdeInfo' : {")
        .appendln("      'name' : 'serde_info_name',")
        .appendln("      'serializationLib' : 'serialization_lib',")
        .appendln("      'parameters' : {")
        .appendln("        'serde_info_foo' : 'serde_info_bar'")
        .appendln("      }")
        .appendln("    },")
        .appendln("    'bucketCols' : [ 'b1', 'b2', 'b3' ],")
        .appendln("    'sortCols' : [ {")
        .appendln("      'col' : 'col',")
        .appendln("      'order' : 1")
        .appendln("    } ],")
        .appendln("    'parameters' : {")
        .appendln("      'serde_foo' : 'serde_bar'")
        .appendln("    },")
        .appendln("    'skewedInfo' : {")
        .appendln("      'skewedColNames' : [ 'col' ],")
        .appendln("      'skewedColValues' : [ [ 'x' ], [ 'y' ] ],")
        .appendln("      'skewedColValueLocationMaps' : {") // Note this serialization is JSON incompatible
        .appendln("        '[x]' : 'loc_x',")
        .appendln("        '[y]' : 'loc_y'")
        .appendln("      }")
        .appendln("    },")
        .appendln("    'storedAsSubDirectories' : true")
        .appendln("  },")
        .appendln("  'partitionKeys' : [ {")
        .appendln("    'name' : 'part',")
        .appendln("    'type' : 'string',")
        .appendln("    'comment' : 'comment'")
        .appendln("  } ],")
        .appendln("  'parameters' : {")
        .appendln("    'table_foo' : 'table_bar'")
        .appendln("  },")
        .appendln("  'viewOriginalText' : 'SELECT * FROM tbl',")
        .appendln("  'viewExpandedText' : 'SELECT col, part FROM tbl',")
        .appendln("  'tableType' : 'EXTERNAL',")
        .appendln("  'privileges' : {")
        .appendln("    'userPrivileges' : {")
        .appendln("      'user' : [ {")
        .appendln("        'privilege' : 'user_privilege',")
        .appendln("        'createTime' : 1526391934,")
        .appendln("        'grantor' : 'user',")
        .appendln("        'grantorType' : 'USER',")
        .appendln("        'grantOption' : true")
        .appendln("      } ]")
        .appendln("    },")
        .appendln("    'groupPrivileges' : {")
        .appendln("      'group' : [ {")
        .appendln("        'privilege' : 'group_privilege',")
        .appendln("        'createTime' : 1526391934,")
        .appendln("        'grantor' : 'group',")
        .appendln("        'grantorType' : 'GROUP',")
        .appendln("        'grantOption' : true")
        .appendln("      } ]")
        .appendln("    },")
        .appendln("    'rolePrivileges' : {")
        .appendln("      'role' : [ {")
        .appendln("        'privilege' : 'role_privilege',")
        .appendln("        'createTime' : 1526391934,")
        .appendln("        'grantor' : 'role',")
        .appendln("        'grantorType' : 'ROLE',")
        .appendln("        'grantOption' : true")
        .appendln("      } ]")
        .appendln("    }")
        .appendln("  },")
        .appendln("  'temporary' : true,")
        .appendln("  'rewriteEnabled' : true")
        .append("}")
        .replaceAll("'", "\"")
        .build());
  }

  @Test
  public void serializePartition() throws Exception {
    Partition partition = createPartition("a");
    String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(partition);
    assertThat(json).isEqualTo(new TextStringBuilder()
        .appendln("{")
        .appendln("  'values' : [ 'a' ],")
        .appendln("  'dbName' : 'test_db',")
        .appendln("  'tableName' : 'test_table',")
        .appendln("  'createTime' : 1526391934,")
        .appendln("  'lastAccessTime' : 1526458642,")
        .appendln("  'sd' : {")
        .appendln("    'cols' : [ {")
        .appendln("      'name' : 'col',")
        .appendln("      'type' : 'integer',")
        .appendln("      'comment' : 'comment'")
        .appendln("    } ],")
        .appendln("    'location' : 'hdfs://server:8020/foo/bar/',")
        .appendln("    'inputFormat' : 'INPUT_FORMAT',")
        .appendln("    'outputFormat' : 'OUTPUT_FORMAT',")
        .appendln("    'compressed' : true,")
        .appendln("    'numBuckets' : 3,")
        .appendln("    'serdeInfo' : {")
        .appendln("      'name' : 'serde_info_name',")
        .appendln("      'serializationLib' : 'serialization_lib',")
        .appendln("      'parameters' : {")
        .appendln("        'serde_info_foo' : 'serde_info_bar'")
        .appendln("      }")
        .appendln("    },")
        .appendln("    'bucketCols' : [ 'b1', 'b2', 'b3' ],")
        .appendln("    'sortCols' : [ {")
        .appendln("      'col' : 'col',")
        .appendln("      'order' : 1")
        .appendln("    } ],")
        .appendln("    'parameters' : {")
        .appendln("      'serde_foo' : 'serde_bar'")
        .appendln("    },")
        .appendln("    'skewedInfo' : {")
        .appendln("      'skewedColNames' : [ 'col' ],")
        .appendln("      'skewedColValues' : [ [ 'x' ], [ 'y' ] ],")
        .appendln("      'skewedColValueLocationMaps' : {") // Note this serialization is JSON incompatible
        .appendln("        '[x]' : 'loc_x',")
        .appendln("        '[y]' : 'loc_y'")
        .appendln("      }")
        .appendln("    },")
        .appendln("    'storedAsSubDirectories' : true")
        .appendln("  },")
        .appendln("  'parameters' : {")
        .appendln("    'partition_foo' : 'partition_bazz'")
        .appendln("  },")
        .appendln("  'privileges' : {")
        .appendln("    'userPrivileges' : {")
        .appendln("      'user' : [ {")
        .appendln("        'privilege' : 'user_privilege',")
        .appendln("        'createTime' : 1526391934,")
        .appendln("        'grantor' : 'user',")
        .appendln("        'grantorType' : 'USER',")
        .appendln("        'grantOption' : true")
        .appendln("      } ]")
        .appendln("    },")
        .appendln("    'groupPrivileges' : {")
        .appendln("      'group' : [ {")
        .appendln("        'privilege' : 'group_privilege',")
        .appendln("        'createTime' : 1526391934,")
        .appendln("        'grantor' : 'group',")
        .appendln("        'grantorType' : 'GROUP',")
        .appendln("        'grantOption' : true")
        .appendln("      } ]")
        .appendln("    },")
        .appendln("    'rolePrivileges' : {")
        .appendln("      'role' : [ {")
        .appendln("        'privilege' : 'role_privilege',")
        .appendln("        'createTime' : 1526391934,")
        .appendln("        'grantor' : 'role',")
        .appendln("        'grantorType' : 'ROLE',")
        .appendln("        'grantOption' : true")
        .appendln("      } ]")
        .appendln("    }")
        .appendln("  }")
        .append("}")
        .replaceAll("'", "\"")
        .build());
  }

  @Test
  public void serializeEnvironmentContext() throws Exception {
    EnvironmentContext context = createEnvironmentContext();
    String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(context);
    assertThat(json).isEqualTo(new TextStringBuilder()
        .appendln("{")
        .appendln("  'properties' : {")
        .appendln("    'context_key' : 'context_value'")
        .appendln("  }")
        .append("}")
        .replaceAll("'", "\"")
        .build());
  }

}
