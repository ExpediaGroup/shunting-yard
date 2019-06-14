/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.replicator.metastore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expediagroup.shuntingyard.replicator.metastore.DefaultMetaStoreClientSupplier;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaStoreClientSupplierTest {

  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock CloseableMetaStoreClient metaStoreClient;

  private final HiveConf hiveConf = new HiveConf();
  private DefaultMetaStoreClientSupplier supplier;

  @Before
  public void init() {
    supplier = new DefaultMetaStoreClientSupplier(hiveConf, metaStoreClientFactory);
  }

  @Test
  public void get() {
    when(metaStoreClientFactory.newInstance(eq(hiveConf), anyString())).thenReturn(metaStoreClient);
    assertThat(supplier.get()).isSameAs(metaStoreClient);
  }

  @Test(expected = NullPointerException.class)
  public void bubbleUpExceptions() {
    when(metaStoreClientFactory.newInstance(eq(hiveConf), anyString())).thenThrow(NullPointerException.class);
    supplier.get();
  }

}
