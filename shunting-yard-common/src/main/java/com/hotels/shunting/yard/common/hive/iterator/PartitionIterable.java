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
package com.hotels.shunting.yard.common.hive.iterator;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.common.HCatUtil;

import com.expedia.hdw.common.hive.metastore.CloseableMetaStoreClient;
import com.expedia.hdw.common.hive.metastore.MetaStoreClientFactory;
import com.expedia.hdw.common.hive.metastore.MetaStoreException;

public class PartitionIterable implements Iterable<Partition>, Configurable {

  private Configuration conf;
  private final Table table;
  private final MetaStoreClientFactory clientFactory;

  public PartitionIterable(Configuration conf, Table table) {
    this.conf = conf;
    this.table = table;
    clientFactory = new MetaStoreClientFactory();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Iterator<Partition> iterator() {
    int batchSize = getConf().getInt(METASTORE_BATCH_RETRIEVE_MAX.varname, METASTORE_BATCH_RETRIEVE_MAX.defaultIntVal);

    CloseableMetaStoreClient client;
    try {
      client = clientFactory.newCloseableInstance(HCatUtil.getHiveConf(getConf()));
    } catch (MetaStoreException | IOException e) {
      throw new RuntimeException("Unable to create CloseableMetaStoreClient", e);
    }

    try {
      return new PartitionIterator(client, table, (short) batchSize);
    } catch (Exception e) {
      throw new RuntimeException("Unable to create PartitionIterator", e);
    }
  }

}
