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
package com.hotels.shunting.yard.replicator.exec.external;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Joiner;

import com.hotels.bdp.circustrain.core.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.core.conf.ReplicaTable;
import com.hotels.bdp.circustrain.core.conf.ReplicationMode;
import com.hotels.bdp.circustrain.core.conf.SourceCatalog;
import com.hotels.bdp.circustrain.core.conf.SourceTable;
import com.hotels.bdp.circustrain.core.conf.TableReplication;

public class CircusTrainConfig {

  private static final Joiner AND_JOINER = Joiner.on(" AND ");
  private static final Joiner OR_JOINER = Joiner.on(") OR (");

  private static String partCondition(FieldSchema partitionColumn, String partitionValue) {
    String type = partitionColumn.getType().toLowerCase();
    boolean needQuotes = type.equals("string") || type.equals("char") || type.equals("varchar") || type.equals("date");
    return new StringBuilder(partitionColumn.getName())
        .append("=")
        .append(needQuotes ? "'" : "")
        .append(partitionValue)
        .append(needQuotes ? "'" : "")
        .toString();
  }

  private static String createPartitionFilter(List<FieldSchema> partitionColumns, List<String>[] partitionValuesList) {
    List<String> partitionExpressions = new ArrayList<>(partitionValuesList.length);
    for (List<String> partitionValues : partitionValuesList) {
      List<String> partConditions = new ArrayList<>(partitionValues.size());
      for (int i = 0; i < partitionColumns.size(); i++) {
        partConditions.add(partCondition(partitionColumns.get(i), partitionValues.get(i)));
      }
      partitionExpressions.add(AND_JOINER.join(partConditions));
    }
    return "(" + OR_JOINER.join(partitionExpressions) + ")";
  }

  public static class Builder {
    private final SourceCatalog sourceCatalog = new SourceCatalog();
    private final ReplicaCatalog replicaCatalog = new ReplicaCatalog();
    private final Map<String, String> copierOptions = new LinkedHashMap<>();
    private final List<TableReplication> tableReplications = new ArrayList<>();

    private Builder() {
      sourceCatalog.setName("source");
      replicaCatalog.setName("replica");
    }

    public Builder sourceName(String sourceName) {
      sourceCatalog.setName(checkNotNull(sourceName, "sourceName is required"));
      return this;
    }

    public Builder sourceMetaStoreUri(String sourceMetaStoreUri) {
      sourceCatalog.setHiveMetastoreUris(checkNotNull(sourceMetaStoreUri, "sourceMetaStoreUri is required"));
      return this;
    }

    public Builder replicaName(String replicaName) {
      replicaCatalog.setName(checkNotNull(replicaName, "replicaName is required"));
      return this;
    }

    public Builder replicaMetaStoreUri(String replicaMetaStoreUri) {
      replicaCatalog.setHiveMetastoreUris(checkNotNull(replicaMetaStoreUri, "replicaMetaStoreUri is required"));
      return this;
    }

    public Builder copierOption(String key, String value) {
      copierOptions.put(checkNotNull(key, "key is required"), checkNotNull(value, "value is required"));
      return this;
    }

    public Builder replication(
        ReplicationMode replicationMode,
        String databaseName,
        String tableName,
        String replicaTableLocation) {
      return replication(replicationMode, databaseName, tableName, replicaTableLocation, null, null);
    }

    public Builder replication(
        ReplicationMode replicationMode,
        String databaseName,
        String tableName,
        String replicaTableLocation,
        List<FieldSchema> partitionColumns,
        List<String>[] partitionValues) {
      TableReplication tableReplication = new TableReplication();
      tableReplication.setReplicationMode(checkNotNull(replicationMode, "replicationMode is required"));

      SourceTable sourceTable = new SourceTable();
      sourceTable.setDatabaseName(checkNotNull(databaseName, "databaseName is required"));
      sourceTable.setTableName(checkNotNull(tableName, "tableName is required"));
      if (partitionColumns != null && partitionColumns.size() > 0 && partitionValues != null) {
        sourceTable.setPartitionFilter(createPartitionFilter(partitionColumns, partitionValues));
      } else {
        sourceTable.setGeneratePartitionFilter(true);
      }
      sourceTable.setPartitionLimit(Short.MAX_VALUE);
      tableReplication.setSourceTable(sourceTable);

      ReplicaTable replicaTable = new ReplicaTable();
      replicaTable.setDatabaseName(checkNotNull(databaseName, "databaseName is required"));
      replicaTable.setTableName(checkNotNull(tableName, "tableName is required"));
      replicaTable.setTableLocation(checkNotNull(replicaTableLocation, "replicaTableLocation is required"));
      tableReplication.setReplicaTable(replicaTable);

      tableReplications.add(tableReplication);
      return this;
    }

    public CircusTrainConfig build() {
      checkState(sourceCatalog.getName() != null, "sourceName is not set");
      checkState(sourceCatalog.getHiveMetastoreUris() != null, "sourceMetaStoreUri is not set");
      checkState(replicaCatalog.getName() != null, "replicaName is not set");
      checkState(replicaCatalog.getHiveMetastoreUris() != null, "replicaMetaStoreUri is not set");
      checkState(tableReplications.size() > 0, "tableReplications is not set");
      return new CircusTrainConfig(this);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private final SourceCatalog sourceCatalog;
  private final ReplicaCatalog replicaCatalog;
  private final Map<String, String> copierOptions;
  private final List<TableReplication> tableReplications;

  private CircusTrainConfig(Builder builder) {
    sourceCatalog = builder.sourceCatalog;
    replicaCatalog = builder.replicaCatalog;
    copierOptions = builder.copierOptions;
    tableReplications = builder.tableReplications;
  }

  public SourceCatalog getSourceCatalog() {
    return sourceCatalog;
  }

  public ReplicaCatalog getReplicaCatalog() {
    return replicaCatalog;
  }

  public Map<String, String> getCopierOptions() {
    return copierOptions;
  }

  public List<TableReplication> getTableReplications() {
    return tableReplications;
  }

}
