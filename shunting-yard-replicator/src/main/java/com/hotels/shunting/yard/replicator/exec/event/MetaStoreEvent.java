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
package com.hotels.shunting.yard.replicator.exec.event;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_TABLE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.event.EventType;

public class MetaStoreEvent {
  public static final String DELETE_DATA_PARAMETER = "delete_data_parameter";

  public static class Builder {
    private final EventType eventType;
    private final String databaseName;
    private final String tableName;
    private List<String> partitionColumns;
    private List<List<String>> partitionValues;
    private Map<String, String> parameters;
    private Map<String, String> environmentContext;

    private Builder(EventType eventType, String databaseName, String tableName) {
      checkNotNull(eventType, "eventType is required");
      checkNotNull(databaseName, "databaseName is required");
      checkNotNull(tableName, "tableName is required");
      this.eventType = eventType;
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    public Builder partitionColumns(List<String> partitionColumns) {
      checkState(this.partitionColumns == null, "partitionColumns has already been set");
      checkNotNull(partitionColumns, "partitionColumns is required");
      this.partitionColumns = ImmutableList.copyOf(partitionColumns);
      return this;
    }

    public Builder partitionValues(List<String> partitionValues) {
      checkState(partitionColumns != null, "partitionColumns is required");
      checkNotNull(partitionValues, "partitionValues is required");
      checkArgument(partitionColumns.size() == partitionValues.size(),
          "Number of partition values doesn't match the number of partition columns");
      if (this.partitionValues == null) {
        this.partitionValues = new ArrayList<>();
      }
      this.partitionValues.add(partitionValues);
      return this;
    }

    public Builder parameters(Map<String, String> parameters) {
      if (this.parameters == null) {
        this.parameters = new HashMap<>();
      }
      if (parameters != null) {
        this.parameters.putAll(parameters);
      }
      return this;
    }

    public Builder parameter(String key, String value) {
      if (parameters == null) {
        parameters = new HashMap<>();
      }
      parameters.put(key, value);
      return this;
    }

    public Builder environmentContext(Map<String, String> environmentContext) {
      if (this.environmentContext == null) {
        this.environmentContext = new HashMap<>();
      }
      if (environmentContext != null) {
        this.environmentContext.putAll(environmentContext);
      }
      return this;
    }

    public MetaStoreEvent build() {
      return new MetaStoreEvent(this);
    }
  }

  public static Builder builder(EventType eventType, String databaseName, String tableName) {
    return new Builder(eventType, databaseName, tableName);
  }

  private final EventType eventType;
  private final String databaseName;
  private final String tableName;
  private final List<String> partitionColumns;
  private final List<List<String>> partitionValues;
  private final Map<String, String> parameters;
  private final Map<String, String> environmentContext;

  private MetaStoreEvent(Builder builder) {
    eventType = builder.eventType;
    databaseName = builder.databaseName;
    tableName = builder.tableName;
    partitionColumns = builder.partitionColumns;
    partitionValues = builder.partitionValues == null ? null : ImmutableList.copyOf(builder.partitionValues);
    parameters = builder.parameters == null ? null : ImmutableMap.copyOf(builder.parameters);
    environmentContext = builder.environmentContext == null ? null : ImmutableMap.copyOf(builder.environmentContext);
  }

  public EventType getEventType() {
    return eventType;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<List<String>> getPartitionValues() {
    return partitionValues;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public Map<String, String> getEnvironmentContext() {
    return environmentContext;
  }

  public String getQualifiedTableName() {
    return String.join(".", getDatabaseName(), getTableName());
  }

  public boolean isDropEvent() {
    return eventType == ON_DROP_PARTITION || eventType == ON_DROP_TABLE;
  }

  public boolean isCascade() {
    if (environmentContext == null) {
      return false;
    }
    return Boolean.valueOf(environmentContext.get(StatsSetupConst.CASCADE));
  }

}
