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
package com.hotels.bdp.circus.train.event.common.receiver;

import org.apache.hadoop.hive.metastore.api.MetaException;

import com.hotels.bdp.circus.train.event.common.event.SerializableAddPartitionEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableAlterPartitionEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableAlterTableEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableCreateTableEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableDropPartitionEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableDropTableEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableInsertEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableListenerEvent;

/**
 * A listener interface for processing {@link SerializableListenerEvent}s
 */
public interface CircusTrainMetaStoreEventListener {

  /**
   * @param event create table event.
   * @throws MetaException
   */
  void onCreateTable(SerializableCreateTableEvent event);

  /**
   * @param event drop table event.
   * @throws MetaException
   */
  void onDropTable(SerializableDropTableEvent event);

  /**
   * @param event alter table event
   * @throws MetaException
   */
  void onAlterTable(SerializableAlterTableEvent event);

  /**
   * @param event add partition event
   * @throws MetaException
   */
  public void onAddPartition(SerializableAddPartitionEvent event);

  /**
   * @param event drop partition event
   * @throws MetaException
   */
  void onDropPartition(SerializableDropPartitionEvent event);

  /**
   * @param event alter partition event
   * @throws MetaException
   */
  void onAlterPartition(SerializableAlterPartitionEvent event);

  /**
   * This will be called when an insert is executed that does not cause a partition to be added. If an insert causes a
   * partition to be added it will cause {@link #onAddPartition} to be called instead.
   *
   * @param event insert event
   * @throws MetaException
   */
  void onInsert(SerializableInsertEvent event);

}
