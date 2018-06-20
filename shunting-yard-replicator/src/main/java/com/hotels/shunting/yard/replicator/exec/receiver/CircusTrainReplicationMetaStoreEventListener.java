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
import static com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent.DELETE_DATA_PARAMETER;

import java.util.List;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.launcher.CircusTrainRunner;

public class CircusTrainReplicationMetaStoreEventListener implements ReplicationMetaStoreEventListener {
  private static final Logger log = LoggerFactory.getLogger(CircusTrainReplicationMetaStoreEventListener.class);

  private final CloseableMetaStoreClient metaStoreClient;
  private final ContextFactory contextFactory;
  private final CircusTrainRunner circusTrainRunner;

  public CircusTrainReplicationMetaStoreEventListener(
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

  public boolean canReplicate(String dbName, String tableName) {
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
  public void onEvent(MetaStoreEvent event) {
    if (!canReplicate(event.getDatabaseName(), event.getTableName())) {
      log.info("Skipping event {} on table {}: table does not seem to be a replica", event.getEventType(),
          event.getQualifiedTableName());
      return;
    }

    switch (event.getEventType()) {
    case ON_DROP_TABLE:
      onDropTable(event);
      break;
    case ON_DROP_PARTITION:
      onDropPartition(event);
      break;
    case ON_CREATE_TABLE:
    case ON_ALTER_TABLE:
    case ON_ADD_PARTITION:
    case ON_ALTER_PARTITION:
    case ON_INSERT:
      replicate(event);
      break;
    default:
      log.info("Ignoring event {} on table {}", event.getEventType(), event.getQualifiedTableName());
      break;
    }

  }

  private void replicate(MetaStoreEvent event) {
    Context context = contextFactory.createContext(event);
    circusTrainRunner.run(context);

    // TODO dodge code: we update here and the CT updates again
    if (isCascade(event)) {
      try {
        Table newReplicaTable = metaStoreClient.getTable(event.getDatabaseName(), event.getTableName());
        // This will make sure the partitions are updated if the cascade option was
        metaStoreClient.alter_table_with_environmentContext(event.getDatabaseName(), event.getTableName(),
            newReplicaTable, new EnvironmentContext(event.getEnvironmentContext()));
      } catch (Exception e) {
        log.warn("SuthingYard Replication could not propagate the CASCADE operation", e);
      }
    }
  }

  private boolean isCascade(MetaStoreEvent event) {
    if (event.getEnvironmentContext() == null) {
      return false;
    }
    return Boolean.valueOf(event.getEnvironmentContext().get(StatsSetupConst.CASCADE));
  }

  private void onDropTable(MetaStoreEvent event) {
    try {
      metaStoreClient.dropTable(event.getDatabaseName(), event.getTableName(), deleteData(event), ifExists());
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot drop table", e);
    }
  }

  private void onDropPartition(MetaStoreEvent event) {
    try {
      for (List<String> partitionValues : event.getPartitionValues()) {
        metaStoreClient.dropPartition(event.getDatabaseName(), event.getTableName(), partitionValues,
            deleteData(event));
      }
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot drop partitions", e);
    }
  }

  private boolean deleteData(MetaStoreEvent event) {
    return Boolean.valueOf(event.getParameters().get(DELETE_DATA_PARAMETER));
  }

}
