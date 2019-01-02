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

import com.hotels.shunting.yard.common.event.AddPartitionEvent;
import com.hotels.shunting.yard.common.event.AlterPartitionEvent;
import com.hotels.shunting.yard.common.event.DropPartitionEvent;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.InsertTableEvent;
import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
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
  public boolean hasNext() {
    return messageReader.hasNext();
  }

  @Override
  public MetaStoreEvent next() {
    return map(messageReader.next());
  }

  private MetaStoreEvent map(ListenerEvent listenerEvent) {
    MetaStoreEvent.Builder builder = MetaStoreEvent
        .builder(listenerEvent.getEventType(), listenerEvent.getDbName(), listenerEvent.getTableName())
        .parameters(listenerEvent.getParameters())
        .parameter(METASTOREURIS.varname, sourceHiveMetastoreUris)
        .environmentContext(
            listenerEvent.getEnvironmentContext() != null ? listenerEvent.getEnvironmentContext().getProperties()
                : null);

    EventType eventType = listenerEvent.getEventType();

    switch (eventType) {
    case ADD_PARTITION: {
      AddPartitionEvent addPartition = (AddPartitionEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(addPartition.getPartitionKeys().keySet()));
      builder.partitionValues(addPartition.getPartitionValues());
      break;
    }
    case ALTER_PARTITION: {
      AlterPartitionEvent alterPartition = (AlterPartitionEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(alterPartition.getPartitionKeys().keySet()));
      builder.partitionValues(alterPartition.getPartitionValues());
      break;
    }
    case DROP_PARTITION: {
      DropPartitionEvent dropPartition = (DropPartitionEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(dropPartition.getPartitionKeys().keySet()));
      builder.partitionValues(dropPartition.getPartitionValues());
      builder.deleteData(true);
      break;
    }
    case INSERT: {
      InsertTableEvent insertTable = (InsertTableEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(insertTable.getPartitionKeyValues().keySet()));
      builder.partitionValues(new ArrayList<>(insertTable.getPartitionKeyValues().values()));
      break;
    }
    case DROP_TABLE: {
      builder.deleteData(true);
      break;
    }

    default:
      // Handle Non-Partition events
      break;
    }
    return builder.build();
  }

}
