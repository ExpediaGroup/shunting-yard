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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.events.InsertEvent;

public class SerializableInsertEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  private String databaseName;
  private String tableName;
  private Map<String, String> keyValues;
  private List<String> files;
  private List<String> fileChecksums;

  SerializableInsertEvent() {}

  public SerializableInsertEvent(InsertEvent event) {
    super(event);
    databaseName = event.getDb();
    tableName = event.getTable();
    keyValues = event.getPartitionKeyValues();
    files = event.getFiles();
    fileChecksums = event.getFileChecksums();
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public Map<String, String> getKeyValues() {
    return keyValues;
  }

  public List<String> getFiles() {
    return files;
  }

  public List<String> getFileChecksums() {
    return fileChecksums;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SerializableInsertEvent)) {
      return false;
    }
    SerializableInsertEvent other = (SerializableInsertEvent) obj;
    return super.equals(other)
        && Objects.equals(databaseName, other.databaseName)
        && Objects.equals(tableName, other.tableName)
        && Objects.equals(keyValues, other.keyValues)
        && Objects.equals(files, other.files)
        && Objects.equals(fileChecksums, other.fileChecksums);
  }

}
