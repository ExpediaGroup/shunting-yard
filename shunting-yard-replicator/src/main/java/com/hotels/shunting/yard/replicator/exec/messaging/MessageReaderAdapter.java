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

import com.expedia.apiary.extensions.receiver.common.MessageReader;
import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.InsertTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

public class MessageReaderAdapter implements MetaStoreEventReader {

  private final MessageReader messageReader;
  private final String sourceHiveMetastoreUris;

  public MessageReaderAdapter(MessageReader messageReader, String sourceHiveMetastoreUris) {
    this.messageReader = messageReader;
    this.sourceHiveMetastoreUris = sourceHiveMetastoreUris;
  }

  @Override
  public void close() throws IOException {
    messageReader.close();
  }

  @Override
  public Optional<MetaStoreEvent> read() {
    Optional<ListenerEvent> event = messageReader.read();
    if (event.isPresent()) {
      return Optional.of(map(event.get()));
    } else {
      return Optional.empty();
    }
  }

  private MetaStoreEvent map(ListenerEvent listenerEvent) {
    MetaStoreEvent.Builder builder = MetaStoreEvent
        .builder(listenerEvent.getEventType(), listenerEvent.getDbName(), listenerEvent.getTableName())
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
