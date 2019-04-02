/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

import java.util.List;

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
    if (!canReplicate(event.getReplicaDatabaseName(), event.getReplicaTableName())) {
      log
          .info("Skipping event {} on table {}: table does not seem to be a replica", event.getEventType(),
              event.getQualifiedReplicaTableName());
      return;
    }

    switch (event.getEventType()) {
    case DROP_TABLE:
      onDropTable(event);
      break;
    case DROP_PARTITION:
      onDropPartition(event);
      break;
    case ADD_PARTITION:
    case ALTER_PARTITION:
    case ALTER_TABLE:
    case CREATE_TABLE:
    case INSERT:
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

    // We update here because CT won't cascade the operation
    if (event.isCascade()) {
      try {
        Table newReplicaTable = metaStoreClient.getTable(event.getReplicaDatabaseName(), event.getReplicaTableName());
        // This will make sure the partitions are updated if the cascade option was set
        metaStoreClient
            .alter_table_with_environmentContext(event.getReplicaDatabaseName(), event.getReplicaTableName(),
                newReplicaTable, new EnvironmentContext(event.getEnvironmentContext()));
      } catch (Exception e) {
        log.warn("ShuntingYard Replication could not propagate the CASCADE operation", e);
      }
    }
  }

  private void onDropTable(MetaStoreEvent event) {
    try {
      metaStoreClient
          .dropTable(event.getReplicaDatabaseName(), event.getReplicaTableName(), event.isDeleteData(), ifExists());
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot drop table", e);
    }
  }

  private void onDropPartition(MetaStoreEvent event) {
    try {
      for (List<String> partitionValues : event.getPartitionValues()) {
        metaStoreClient
            .dropPartition(event.getReplicaDatabaseName(), event.getReplicaTableName(), partitionValues,
                event.isDeleteData());
      }
    } catch (Exception e) {
      throw new ShuntingYardException("Cannot drop partitions", e);
    }
  }

}
