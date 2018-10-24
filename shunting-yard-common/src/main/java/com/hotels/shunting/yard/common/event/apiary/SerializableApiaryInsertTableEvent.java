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
package com.hotels.shunting.yard.common.event.apiary;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.events.InsertEvent;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

public class SerializableApiaryInsertTableEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  /**
   * This class deliberately extends {@link SerializableListenerEvent} and not {@link SerializableApiaryListenerEvent}
   * as the sourceMetastoreUris is not available in the {@link InsertEvent}.
   */

  private String protocolVersion;
  private String dbName;
  private String tableName;
  private List<String> files;
  private List<String> fileChecksums;
  private Map<String, String> partitionKeyValues;

  SerializableApiaryInsertTableEvent() {}

  public SerializableApiaryInsertTableEvent(InsertEvent event) {
    super(event);
    dbName = event.getDb();
    tableName = event.getTable();
    files = event.getFiles();
    fileChecksums = event.getFileChecksums();
    partitionKeyValues = event.getPartitionKeyValues();
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  @Override
  public String getDbName() {
    return dbName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public List<String> getFiles() {
    return files;
  }

  public List<String> getFileChecksums() {
    return fileChecksums;
  }

  public Map<String, String> getPartitionKeyValues() {
    return partitionKeyValues;
  }

}
