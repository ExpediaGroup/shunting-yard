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
package com.hotels.bdp.circus.train.event.common.messaging;

public class Message {

  public static class Builder {
    private String database;
    private String table;
    private long timestamp = System.currentTimeMillis();
    private byte[] payload;

    private Builder() {}

    private static String checkEmpty(String string, String message) {
      if (string == null || string.trim().isEmpty()) {
        throw new IllegalArgumentException(message);
      }
      return string.trim();
    }

    private static <T> T checkNull(T object, String message) {
      if (object == null) {
        throw new IllegalArgumentException(message);
      }
      return object;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder table(String table) {
      this.table = table;
      return this;
    }

    public Builder timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder payload(byte[] payload) {
      this.payload = payload;
      return this;
    }

    public Message build() {
      return new Message(checkEmpty(database, "Parameter 'database' is required"),
          checkEmpty(table, "Parameter 'table' is required"), timestamp,
          checkNull(payload, "Parameter 'payload' is required"));
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private final String database;
  private final String table;
  private final long timestamp;
  private final byte[] payload;

  private Message(String database, String table, long timestamp, byte[] payload) {
    this.database = database;
    this.table = table;
    this.timestamp = timestamp;
    this.payload = payload;
  }

  public String getQualifiedTableName() {
    return String.format("%s.%s", database, table);
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public byte[] getPayload() {
    return payload;
  }

}
