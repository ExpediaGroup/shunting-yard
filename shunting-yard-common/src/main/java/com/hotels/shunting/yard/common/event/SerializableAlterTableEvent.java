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

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

public class SerializableAlterTableEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  private Table oldTable;
  private Table newTable;

  SerializableAlterTableEvent() {}

  public SerializableAlterTableEvent(AlterTableEvent event) {
    super(event);
    oldTable = event.getOldTable();
    newTable = event.getNewTable();
  }

  @Override
  public String getDatabaseName() {
    return newTable.getDbName();
  }

  @Override
  public String getTableName() {
    return newTable.getTableName();
  }

  public Table getOldTable() {
    return oldTable;
  }

  public Table getNewTable() {
    return newTable;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SerializableAlterTableEvent)) {
      return false;
    }
    SerializableAlterTableEvent other = (SerializableAlterTableEvent) obj;
    return super.equals(other) && Objects.equals(oldTable, other.oldTable) && Objects.equals(newTable, other.newTable);
  }

}
