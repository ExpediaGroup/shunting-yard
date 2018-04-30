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
package com.hotels.bdp.circus.train.event.receiver.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.base.Supplier;

import com.expedia.hdw.common.hive.metastore.CloseableMetaStoreClient;
import com.expedia.hdw.common.hive.metastore.MetaStoreClientFactory;

/*
 * Based on Circus Train
 */
public class DefaultMetaStoreClientSupplier implements Supplier<CloseableMetaStoreClient> {

  private final MetaStoreClientFactory metaStoreClientFactory;
  private final HiveConf hiveConf;

  public DefaultMetaStoreClientSupplier(HiveConf hiveConf, MetaStoreClientFactory metaStoreClientFactory) {
    this.hiveConf = hiveConf;
    this.metaStoreClientFactory = metaStoreClientFactory;
  }

  @Override
  public CloseableMetaStoreClient get() {
    return metaStoreClientFactory.newCloseableInstance(hiveConf);
  }

}
