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
package com.hotels.shunting.yard.common.event;

import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;

public class SerializableAlterPartitionEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  private Table table;
  private Partition oldPartition;
  private Partition newPartition;

  SerializableAlterPartitionEvent() {}

  public SerializableAlterPartitionEvent(AlterPartitionEvent event) {
    super(event);
    table = event.getTable();
    oldPartition = event.getOldPartition();
    newPartition = event.getNewPartition();
  }

  @Override
  public String getDatabaseName() {
    return table.getDbName();
  }

  @Override
  public String getTableName() {
    return table.getTableName();
  }

  public Table getTable() {
    return table;
  }

  public Partition getOldPartition() {
    return oldPartition;
  }

  public Partition getNewPartition() {
    return newPartition;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SerializableAlterPartitionEvent)) {
      return false;
    }
    SerializableAlterPartitionEvent other = (SerializableAlterPartitionEvent) obj;
    return super.equals(other)
        && Objects.equals(table, other.table)
        && Objects.equals(oldPartition, other.oldPartition)
        && Objects.equals(newPartition, other.newPartition);
  }

}
