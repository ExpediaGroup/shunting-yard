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
package com.hotels.shunting.yard.common.receiver.thrift;

import static com.hotels.shunting.yard.common.receiver.thrift.ThriftListenerUtils.toObjectPairs;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterTableEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.receiver.ShuntingYardMetaStoreEventListener;

public class ThriftShuntingYardMetaStoreEventListener implements ShuntingYardMetaStoreEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftShuntingYardMetaStoreEventListener.class);

  private static final String CIRCUS_TRAIN_REPLICATION_EVENT_PREFIX = "cte";
  private static final String CIRCUS_TRAIN_REPLICATION_EVENT_PROPERTY = "com.hotels.bdp.circustrain.replication.event";

  private final CloseableMetaStoreClient metaStoreClient;
  private final EventIdFactory eventIdFactory = EventIdFactory.DEFAULT;

  public ThriftShuntingYardMetaStoreEventListener(CloseableMetaStoreClient metaStoreClient) {
    this.metaStoreClient = metaStoreClient;
  }

  private String newEventId() {
    return eventIdFactory.newEventId(CIRCUS_TRAIN_REPLICATION_EVENT_PREFIX);
  }

  private String tagReplication(Table table) {
    String eventId = newEventId();
    addEventId(table.getParameters(), eventId);
    return eventId;
  }

  private String tagReplication(Partition partition) {
    String eventId = newEventId();
    addEventId(partition.getParameters(), eventId);
    return eventId;
  }

  private String tagReplication(List<Partition> partitions) {
    String eventId = newEventId();
    for (Partition partition : partitions) {
      addEventId(partition.getParameters(), eventId);
    }
    return eventId;
  }

  private void addEventId(Map<String, String> parameters, String eventId) {
    parameters.put(CIRCUS_TRAIN_REPLICATION_EVENT_PROPERTY, eventId);
  }

  private boolean ifExists() {
    return true;
  }

  public boolean canReplicate(Table table) {
    return canReplicate(table.getDbName(), table.getTableName());
  }

  public boolean canReplicate(Partition partition) {
    return canReplicate(partition.getDbName(), partition.getTableName());
  }

  private boolean canReplicate(String dbName, String tableName) {
    try {
      Table table = metaStoreClient.getTable(dbName, tableName);
      return table.getParameters().get(CIRCUS_TRAIN_REPLICATION_EVENT_PROPERTY) != null;
    } catch (NoSuchObjectException e) {
      return true;
    } catch (TException e) {
      throw new ShuntingYardException(
          String.format("Cannot check whether table %s.%s can be replicated", dbName, tableName), e);
    }
  }

  @Override
  public void onCreateTable(SerializableCreateTableEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping create table: {}.{}", event.getTable().getDbName(), event.getTable().getTableName());
      return;
    }
    tagReplication(event.getTable());
    try {
      metaStoreClient.createTable(event.getTable());
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to create table", e);
    }
  }

  @Override
  public void onDropTable(SerializableDropTableEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping drop table: {}.{}", event.getTable().getDbName(), event.getTable().getTableName());
      return;
    }
    // Tagging is not needed here
    try {
      metaStoreClient.dropTable(event.getTable().getDbName(), event.getTable().getTableName(), event.getDeleteData(),
          ifExists());
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to drop table", e);
    }
  }

  @Override
  public void onAlterTable(SerializableAlterTableEvent event) {
    if (!canReplicate(event.getOldTable())) {
      LOG.info("Skipping alter table {}.{}", event.getOldTable().getDbName(), event.getOldTable().getTableName());
      return;
    }
    tagReplication(event.getNewTable());
    try {
      metaStoreClient.alter_table_with_environmentContext(event.getOldTable().getDbName(),
          event.getOldTable().getTableName(), event.getNewTable(), event.getEnvironmentContext());
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to alter table", e);
    }
  }

  @Override
  public void onAddPartition(SerializableAddPartitionEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping add partition on table: {}.{}", event.getTable().getDbName(), event.getTable().getTableName());
      return;
    }
    tagReplication(event.getPartitions());
    try {
      metaStoreClient.add_partitions(event.getPartitions());
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to add partitions", e);
    }
  }

  @Override
  public void onDropPartition(SerializableDropPartitionEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping drop partition on table: {}.{}", event.getTable().getDbName(),
          event.getTable().getTableName());
      return;
    }
    // Tagging is not needed here
    try {
      metaStoreClient.dropPartitions(event.getTable().getDbName(), event.getTable().getTableName(),
          toObjectPairs(event.getTable(), event.getPartitions()), event.getDeleteData(), ifExists(), false);
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to drop partitions", e);
    }
  }

  @Override
  public void onAlterPartition(SerializableAlterPartitionEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping alter partition on table: {}.{}", event.getTable().getDbName(),
          event.getTable().getTableName());
      return;
    }
    tagReplication(event.getNewPartition());
    try {
      metaStoreClient.alter_partition(event.getTable().getDbName(), event.getTable().getTableName(),
          event.getNewPartition(), event.getEnvironmentContext());
    } catch (Exception e) {
      throw new ShuntingYardException("Unable to alter partition", e);
    }
  }

  @Override
  public void onInsert(SerializableInsertEvent event) {
    LOG.info("Ignoring insert event on table: {}.{}", event.getDatabaseName(), event.getTableName());
  }

}
