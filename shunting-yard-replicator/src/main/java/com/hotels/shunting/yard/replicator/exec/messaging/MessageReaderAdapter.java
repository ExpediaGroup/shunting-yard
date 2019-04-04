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
package com.hotels.shunting.yard.replicator.exec.messaging;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.InsertTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageProperty;

import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.shunting.yard.replicator.exec.conf.ShuntingYardTableReplicationsMap;
import com.hotels.shunting.yard.replicator.exec.conf.ct.ShuntingYardTableReplication;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

public class MessageReaderAdapter implements MetaStoreEventReader {
  private static final Logger log = LoggerFactory.getLogger(MessageReaderAdapter.class);

  private final MessageReader messageReader;
  private final String sourceHiveMetastoreUris;
  private final ShuntingYardTableReplicationsMap shuntingYardReplications;

  public MessageReaderAdapter(
      MessageReader messageReader,
      String sourceHiveMetastoreUris,
      ShuntingYardTableReplicationsMap shuntingYardReplications) {
    this.messageReader = messageReader;
    this.sourceHiveMetastoreUris = sourceHiveMetastoreUris;
    this.shuntingYardReplications = shuntingYardReplications;
  }

  @Override
  public void close() throws IOException {
    messageReader.close();
  }

  @Override
  public Optional<MetaStoreEvent> read() {
    Optional<MessageEvent> event = messageReader.read();
    if (event.isPresent()) {
      MessageEvent messageEvent = event.get();
      deleteMessage(messageEvent);
      return Optional.of(map(messageEvent.getEvent()));
    } else {
      return Optional.empty();
    }
  }

  private void deleteMessage(MessageEvent event) {
    try {
      String receiptHandle = event.getMessageProperties().get(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE);
      messageReader.delete(receiptHandle);
      log.debug("Message deleted successfully");
    } catch (Exception e) {
      log.error("Could not delete message from queue: ", e);
    }
  }

  private MetaStoreEvent map(ListenerEvent listenerEvent) {
    String replicaDatabaseName = listenerEvent.getDbName();
    String replicaTableName = listenerEvent.getTableName();

    ShuntingYardTableReplication tableReplication = shuntingYardReplications
        .getTableReplication(listenerEvent.getDbName(), listenerEvent.getTableName());

    if (tableReplication != null) {
      replicaDatabaseName = tableReplication.getReplicaDatabaseName();
      replicaTableName = tableReplication.getReplicaTableName();
    }

    MetaStoreEvent.Builder builder = MetaStoreEvent
        .builder(listenerEvent.getEventType(), listenerEvent.getDbName(), listenerEvent.getTableName(),
            replicaDatabaseName, replicaTableName)
        .parameters(listenerEvent.getTableParameters())
        .parameter(METASTOREURIS.varname, sourceHiveMetastoreUris)
        .environmentContext(
            listenerEvent.getEnvironmentContext() != null ? listenerEvent.getEnvironmentContext().getProperties()
                : null);

    EventType eventType = listenerEvent.getEventType();

    switch (eventType) {
    case ADD_PARTITION:
      AddPartitionEvent addPartition = (AddPartitionEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(addPartition.getPartitionKeys().keySet()));
      builder.partitionValues(addPartition.getPartitionValues());
      break;
    case ALTER_PARTITION:
      AlterPartitionEvent alterPartition = (AlterPartitionEvent) listenerEvent;
      if (alterPartition.getPartitionLocation() != null) {
        if (alterPartition.getPartitionLocation().equals(alterPartition.getOldPartitionLocation())) {
          builder.replicationMode(ReplicationMode.METADATA_UPDATE);
        }
      }
      builder.partitionColumns(new ArrayList<>(alterPartition.getPartitionKeys().keySet()));
      builder.partitionValues(alterPartition.getPartitionValues());
      break;
    case DROP_PARTITION:
      DropPartitionEvent dropPartition = (DropPartitionEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(dropPartition.getPartitionKeys().keySet()));
      builder.partitionValues(dropPartition.getPartitionValues());
      builder.deleteData(true);
      break;
    case INSERT:
      InsertTableEvent insertTable = (InsertTableEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(insertTable.getPartitionKeyValues().keySet()));
      builder.partitionValues(new ArrayList<>(insertTable.getPartitionKeyValues().values()));
      break;
    case ALTER_TABLE:
      AlterTableEvent alterTable = (AlterTableEvent) listenerEvent;
      if (alterTable.getTableLocation() != null) {
        if (alterTable.getTableLocation().equals(alterTable.getOldTableLocation())) {
          builder.replicationMode(ReplicationMode.METADATA_UPDATE);
        }
      }
      break;
    case DROP_TABLE:
      builder.deleteData(true);
      break;

    default:
      // Handle Non-Partition events
      break;
    }
    return builder.build();
  }

}
