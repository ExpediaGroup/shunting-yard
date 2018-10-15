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

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

/*
 * {
 *   \"protocolVersion\": \"1.0\",
 *   \"eventType\": \"INSERT\",
 *   \"dbName\": \"some_db\",
 *   \"tableName\": \"some_table\",
 *   \"files\": [\"file:/a/b.txt\",\"file:/a/c.txt\"],
 *   \"fileChecksums\": [\"123\",\"456\"],
 *   \"partitionKeyValues\": {\"load_date\": \"2013-03-24\",\"variant_code\": \"EN\"},
 *   \"sourceMetastoreUris\": \"thrift://host:9083\"
 * }
 *
 */

public class SerializableApiaryInsertTableEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  private String protocolVersion;
  private String dbName;
  private String tableName;
  private List<String> files;
  private List<String> fileChecksums;
  private Map<String, String> keyValues;
  private String sourceMetastoreUris;

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public String getDbName() {
    return dbName;
  }

  @Override
  public String getDatabaseName() {
    return dbName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public String getSourceMetastoreUris() {
    return sourceMetastoreUris;
  }

  public List<String> getFiles() {
    return files;
  }

  public List<String> getFileChecksums() {
    return fileChecksums;
  }

  public Map<String, String> getKeyValues() {
    return keyValues;
  }

}
