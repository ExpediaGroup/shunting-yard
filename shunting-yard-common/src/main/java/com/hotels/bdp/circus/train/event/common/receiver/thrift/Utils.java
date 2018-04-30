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
package com.hotels.bdp.circus.train.event.common.receiver.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public class Utils {

  public static List<ObjectPair<Integer, byte[]>> toObjectPairs(Table table, List<Partition> partitions) {
    List<ObjectPair<Integer, byte[]>> pairs = new ArrayList<>(partitions.size());
    for (Partition partition : partitions) {
      Map<String, String> partitionSpec = Warehouse.makeSpecFromValues(table.getPartitionKeys(), partition.getValues());
      ExprNodeGenericFuncDesc partitionExpression;
      try {
        partitionExpression = new ExpressionBuilder(table, partitionSpec).build();
      } catch (SemanticException e) {
        throw new RuntimeException("Unable to build expression", e);
      }
      ObjectPair<Integer, byte[]> serializedPartitionExpression = new ObjectPair<>(partitionSpec.size(),
          SerializationUtilities.serializeExpressionToKryo(partitionExpression));
      pairs.add(serializedPartitionExpression);
    }
    return pairs;
  }

}
