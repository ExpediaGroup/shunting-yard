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
package com.hotels.shunting.yard.replicator.exec.receiver;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.shunting.yard.common.receiver.thrift.ThriftListenerUtils.toObjectPairs;

import java.util.Arrays;

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
import com.hotels.shunting.yard.replicator.exec.launcher.CircusTrainRunner;

public class ReplicationCircusTrainMetaStoreEventListener implements ShuntingYardMetaStoreEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationCircusTrainMetaStoreEventListener.class);

  private final CloseableMetaStoreClient metaStoreClient;
  private final ContextFactory contextFactory;
  private final CircusTrainRunner circusTrainRunner;

  public ReplicationCircusTrainMetaStoreEventListener(
      CloseableMetaStoreClient metaStoreClient,
      ContextFactory contextFactory,
      CircusTrainRunner circusTrainRunner) {
    this.metaStoreClient = metaStoreClient;
    this.contextFactory = contextFactory;
    this.circusTrainRunner = circusTrainRunner;
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
      return table.getParameters().get(REPLICATION_EVENT.parameterName()) != null;
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

    Context context = contextFactory.createContext(event, event.getTable());
    circusTrainRunner.run(context);
  }

  @Override
  public void onDropTable(SerializableDropTableEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping drop table: {}.{}", event.getTable().getDbName(), event.getTable().getTableName());
      return;
    }

    try {
      metaStoreClient.dropTable(event.getTable().getDbName(), event.getTable().getTableName(), event.getDeleteData(),
          ifExists());
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot drop table", e);
    }
  }

  @Override
  public void onAlterTable(SerializableAlterTableEvent event) {
    if (!canReplicate(event.getOldTable())) {
      LOG.info("Skipping alter table {}.{}", event.getOldTable().getDbName(), event.getOldTable().getTableName());
      return;
    }

    Context context = contextFactory.createContext(event, event.getNewTable());
    circusTrainRunner.run(context);

    // TODO dodge code: we update here and the CT updates again
    try {
      Table newReplicaTable = metaStoreClient.getTable(event.getNewTable().getDbName(),
          event.getNewTable().getTableName());
      // This will make sure the partitions are updated if the cascade option was
      metaStoreClient.alter_table_with_environmentContext(newReplicaTable.getDbName(), newReplicaTable.getTableName(),
          newReplicaTable, event.getEnvironmentContext());
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot alter table", e);
    }
  }

  @Override
  public void onAddPartition(SerializableAddPartitionEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping add partition on table: {}.{}", event.getTable().getDbName(), event.getTable().getTableName());
      return;
    }

    Context context = contextFactory.createContext(event, event.getTable(), event.getPartitions());
    circusTrainRunner.run(context);
  }

  @Override
  public void onDropPartition(SerializableDropPartitionEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping drop partition on table: {}.{}", event.getTable().getDbName(),
          event.getTable().getTableName());
      return;
    }

    try {
      metaStoreClient.dropPartitions(event.getTable().getDbName(), event.getTable().getTableName(),
          toObjectPairs(event.getTable(), event.getPartitions()), event.getDeleteData(), ifExists(), false);
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot drop partitions", e);
    }
  }

  @Override
  public void onAlterPartition(SerializableAlterPartitionEvent event) {
    if (!canReplicate(event.getTable())) {
      LOG.info("Skipping alter partition on table: {}.{}", event.getTable().getDbName(),
          event.getTable().getTableName());
      return;
    }

    Context context = contextFactory.createContext(event, event.getTable(), Arrays.asList(event.getNewPartition()));
    circusTrainRunner.run(context);
  }

  @Override
  public void onInsert(SerializableInsertEvent event) {
    LOG.info("Ignoring insert event on table: {}.{}", event.getDatabaseName(), event.getTableName());
  }

}
