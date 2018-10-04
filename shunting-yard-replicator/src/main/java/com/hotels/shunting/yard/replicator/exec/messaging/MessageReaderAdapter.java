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
package com.hotels.shunting.yard.replicator.exec.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

public class MessageReaderAdapter implements MetaStoreEventReader {

  private final MessageReader messageReader;

  public MessageReaderAdapter(MessageReader messageReader) {
    this.messageReader = messageReader;
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

  private MetaStoreEvent map(SerializableListenerEvent listenerEvent) {
    MetaStoreEvent.Builder builder = MetaStoreEvent
        .builder(listenerEvent.getEventType(), listenerEvent.getDatabaseName(), listenerEvent.getTableName())
        .parameters(listenerEvent.getParameters())
        .environmentContext(
            listenerEvent.getEnvironmentContext() != null ? listenerEvent.getEnvironmentContext().getProperties()
                : null);

    switch (listenerEvent.getEventType()) {
    case ON_ADD_PARTITION: {
      SerializableAddPartitionEvent addPartition = (SerializableAddPartitionEvent) listenerEvent;
      addPartitionColumns(builder, addPartition.getTable());
      addPartitionValues(builder, addPartition.getPartitions());
      break;
    }
    case ON_ALTER_PARTITION: {
      SerializableAlterPartitionEvent alterPartition = (SerializableAlterPartitionEvent) listenerEvent;
      addPartitionColumns(builder, alterPartition.getTable());
      builder.partitionValues(alterPartition.getNewPartition().getValues());
      break;
    }
    case ON_DROP_PARTITION: {
      SerializableDropPartitionEvent dropPartition = (SerializableDropPartitionEvent) listenerEvent;
      addPartitionColumns(builder, dropPartition.getTable());
      addPartitionValues(builder, dropPartition.getPartitions());
      builder.deleteData(dropPartition.getDeleteData());
      break;
    }
    case ON_DROP_TABLE: {
      SerializableDropTableEvent dropTable = (SerializableDropTableEvent) listenerEvent;
      builder.deleteData(dropTable.getDeleteData());
      break;
    }
    case ON_INSERT: {
      SerializableInsertEvent insert = (SerializableInsertEvent) listenerEvent;
      builder.partitionColumns(new ArrayList<>(insert.getKeyValues().keySet()));
      builder.partitionValues(new ArrayList<>(insert.getKeyValues().values()));
      break;
    }
    default:
      // Ignore non-partition events
      break;
    }
    return builder.build();
  }

  private void addPartitionColumns(MetaStoreEvent.Builder builder, Table table) {
    builder.partitionColumns(table.getPartitionKeys().stream().map(f -> f.getName()).collect(Collectors.toList()));
  }

  private void addPartitionValues(MetaStoreEvent.Builder builder, List<Partition> partitions) {
    partitions.stream().map(p -> p.getValues()).forEach(pl -> builder.partitionValues(pl));
  }

}
