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

import static org.apache.hadoop.hive.metastore.api.PrincipalType.GROUP;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.collect.ImmutableMap;

public class SerDeTestUtils {

  static final String DATABASE = "test_db";
  static final String TABLE = "test_table";
  static final String OWNER = "shunting_yard";
  static final int CREATE_TIME = 1526391934; // Tuesday, May 15, 2018 2:45:34 PM
  static final int LAST_ACCESS_TIME = 1526458642; // Wednesday, May 16, 2018 8:17:22 AM
  static final int RETENTION = 5;
  static final String VIEW_ORIGINAL_TEXT = "SELECT * FROM tbl";
  static final String VIEW_EXPANDED_TEXT = "SELECT col, part FROM tbl";
  static final String TABLE_TYPE = "EXTERNAL";
  static final String LOCATION = "hdfs://server:8020/foo/bar/";
  static final String INPUT_FORMAT = "INPUT_FORMAT";
  static final String OUTPUT_FORMAT = "OUTPUT_FORMAT"; // required
  static final int NUM_OF_BUCKETS = 3;
  static final String SERDE_INFO_NAME = "serde_info_name";
  static final String SERIALIZATION_LIB = "serialization_lib";

  static final List<FieldSchema> DATA_COLS = Arrays.asList(new FieldSchema("col", "integer", "comment"));
  static final List<FieldSchema> PARTITION_COLS = Arrays.asList(new FieldSchema("part", "string", "comment"));
  static final List<String> BUCKET_COLS = Arrays.asList("b1", "b2", "b3");
  static final List<Order> SORT_COLS = Arrays.asList(new Order("col", 1));
  static final List<String> SKEWED_COL_NAMES = Arrays.asList("col");
  static final List<List<String>> SKEWED_COL_VALUES = Arrays.asList(Arrays.asList("x"), Arrays.asList("y"));
  static final Map<List<String>, String> SKEWED_COL_VALUE_LOCATION_MAPS = ImmutableMap
      .<List<String>, String> builder()
      .put(Arrays.asList("x"), "loc_x")
      .put(Arrays.asList("y"), "loc_y")
      .build();

  static final List<PrivilegeGrantInfo> USER_PRIVILEGE = Arrays
      .asList(new PrivilegeGrantInfo("user_privilege", CREATE_TIME, "user", USER, true));
  static final List<PrivilegeGrantInfo> GROUP_PRIVILEGE = Arrays
      .asList(new PrivilegeGrantInfo("group_privilege", CREATE_TIME, "group", GROUP, true));
  static final List<PrivilegeGrantInfo> ROLE_PRIVILEGE = Arrays
      .asList(new PrivilegeGrantInfo("role_privilege", CREATE_TIME, "role", ROLE, true));

  private static StorageDescriptor storageDescriptor(FieldSchema... moreCols) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<>(DATA_COLS);
    if (moreCols != null) {
      Collections.addAll(cols, moreCols);
    }
    storageDescriptor.setCols(cols);
    storageDescriptor.setLocation(LOCATION);
    storageDescriptor.setInputFormat(INPUT_FORMAT);
    storageDescriptor.setOutputFormat(OUTPUT_FORMAT);
    storageDescriptor.setCompressed(true);
    storageDescriptor.setNumBuckets(NUM_OF_BUCKETS);
    storageDescriptor.setSerdeInfo(new SerDeInfo());
    storageDescriptor.getSerdeInfo().setName(SERDE_INFO_NAME);
    storageDescriptor.getSerdeInfo().setSerializationLib(SERIALIZATION_LIB);
    storageDescriptor.getSerdeInfo().setParameters(ImmutableMap.of("serde_info_foo", "serde_info_bar"));
    storageDescriptor.setBucketCols(BUCKET_COLS);
    storageDescriptor.setSortCols(SORT_COLS);
    storageDescriptor.setParameters(ImmutableMap.of("serde_foo", "serde_bar"));
    storageDescriptor.setSkewedInfo(new SkewedInfo());
    storageDescriptor.getSkewedInfo().setSkewedColNames(SKEWED_COL_NAMES);
    storageDescriptor.getSkewedInfo().setSkewedColValues(SKEWED_COL_VALUES);
    storageDescriptor.getSkewedInfo().setSkewedColValueLocationMaps(SKEWED_COL_VALUE_LOCATION_MAPS);
    storageDescriptor.setStoredAsSubDirectories(true);
    return storageDescriptor;
  }

  private static PrincipalPrivilegeSet privileges() {
    PrincipalPrivilegeSet privileges = new PrincipalPrivilegeSet(new HashMap<String, List<PrivilegeGrantInfo>>(),
        new HashMap<String, List<PrivilegeGrantInfo>>(), new HashMap<String, List<PrivilegeGrantInfo>>());
    privileges.getUserPrivileges().put("user", USER_PRIVILEGE);
    privileges.getGroupPrivileges().put("group", GROUP_PRIVILEGE);
    privileges.getRolePrivileges().put("role", ROLE_PRIVILEGE);
    return privileges;
  }

  public static Table createTable(FieldSchema... moreCols) {
    // Fully populated Table to make sure all fields are serde properly
    Table table = new Table();
    table.setDbName(DATABASE);
    table.setTableName(TABLE);
    table.setOwner(OWNER);
    table.setCreateTime(CREATE_TIME);
    table.setLastAccessTime(LAST_ACCESS_TIME);
    table.setRetention(RETENTION);
    table.setSd(storageDescriptor());
    table.setPartitionKeys(PARTITION_COLS);
    table.setParameters(ImmutableMap.of("table_foo", "table_bar"));
    table.setViewOriginalText(VIEW_ORIGINAL_TEXT);
    table.setViewExpandedText(VIEW_EXPANDED_TEXT);
    table.setTableType(TABLE_TYPE);
    table.setPrivileges(privileges());
    table.setTemporary(true);
    table.setRewriteEnabled(true);
    return table;
  }

  public static Partition createPartition(String value) {
    // Fully populated Partition to make sure all fields are serde properly
    Partition partition = new Partition();
    partition.setValues(Arrays.asList(value));
    partition.setDbName(DATABASE);
    partition.setTableName(TABLE);
    partition.setCreateTime(CREATE_TIME);
    partition.setLastAccessTime(LAST_ACCESS_TIME);
    partition.setSd(storageDescriptor());
    partition.setParameters(ImmutableMap.of("partition_foo", "partition_bazz"));
    partition.setPrivileges(privileges());
    return partition;
  }

  public static EnvironmentContext createEnvironmentContext() {
    // Fully populated EnvironmentContext to make sure all fields are serde properly
    EnvironmentContext context = new EnvironmentContext();
    context.setProperties(ImmutableMap.of("context_key", "context_value"));
    return context;
  }

}
