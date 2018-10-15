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
package com.hotels.shunting.yard.replicator.exec.messaging;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;

public class ApiaryEventHelper {
  private static final Logger log = LoggerFactory.getLogger(ApiaryEventHelper.class);
  private final MetaStoreClientFactory metaStoreClientFactory;

  public ApiaryEventHelper(MetaStoreClientFactory metaStoreClientFactory) {
    this.metaStoreClientFactory = metaStoreClientFactory;
  }

  public List<String> getPartitionKeys(String dbName, String tableName, String sourceMetastoreUris) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ConfVars.METASTOREURIS.varname, sourceMetastoreUris);

    HiveConf hiveConf = new HiveConfFactory(null, properties).newInstance();

    CloseableMetaStoreClient sourceClient = metaStoreClientFactory.newInstance(hiveConf, "source-table-client");

    Table sourceTable = null;
    try {
      sourceTable = sourceClient.getTable(dbName, tableName);
    } catch (MetaException e) {
      log.error(String.format("Could not connect to Hive Metastore: [%s]", sourceMetastoreUris), e);
      return null;
    } catch (NoSuchObjectException e) {
      log.error(String.format("Table: [%s.%s] not found in Source Hive Metastore", dbName, tableName), e);
      return null;
    } catch (TException e) {
      log.error(String.format("Error while trying to connect to Source Hive Metastore: [%s]", sourceMetastoreUris), e);
      return null;
    }

    List<String> partitionKeys = sourceTable
        .getPartitionKeys()
        .stream()
        .map(f -> f.getName())
        .collect(Collectors.toList());

    return partitionKeys;
  }

}
