/**
 * Copyright (C) 2016-2018 Expedia Inc. and Apache Hive authors.
 *
 * Copied from Hive 2.3.0:
 *
 *     https://github.com/apache/hive/blob/rel/release-2.3.0/hcatalog/webhcat/java-client/src/main/java/org/
 *     apache/hive/hcatalog/api/HCatClientHMSImpl.java#L521
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
package com.hotels.shunting.yard.common.receiver.thrift;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Helper class to help build ExprDesc tree to represent the partitions to be dropped. Note: At present, the
 * ExpressionBuilder only constructs partition predicates where partition-keys equal specific values, and logical-AND
 * expressions. E.g. ( dt = '20150310' AND region = 'US' ) This only supports the partition-specs specified by the Map
 * argument of: {@link org.apache.hive.hcatalog.api.HCatClient#dropPartitions(String, String, Map, boolean)}
 */
class ExpressionBuilder {

  private final Map<String, PrimitiveTypeInfo> partColumnTypesMap = Maps.newHashMap();
  private final Map<String, String> partSpecs;

  public ExpressionBuilder(Table table, Map<String, String> partSpecs) {
    this.partSpecs = partSpecs;
    for (FieldSchema partField : table.getPartitionKeys()) {
      partColumnTypesMap
          .put(partField.getName().toLowerCase(), TypeInfoFactory.getPrimitiveTypeInfo(partField.getType()));
    }
  }

  private PrimitiveTypeInfo getTypeFor(String partColumn) {
    return partColumnTypesMap.get(partColumn.toLowerCase());
  }

  private Object getTypeAppropriateValueFor(PrimitiveTypeInfo type, String value) {
    ObjectInspectorConverters.Converter converter = ObjectInspectorConverters
        .getConverter(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo),
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(type));

    return converter.convert(value);
  }

  public ExprNodeGenericFuncDesc equalityPredicate(String partColumn, String value) throws SemanticException {

    PrimitiveTypeInfo partColumnType = getTypeFor(partColumn);
    ExprNodeColumnDesc partColumnExpr = new ExprNodeColumnDesc(partColumnType, partColumn, null, true);
    ExprNodeConstantDesc valueExpr = new ExprNodeConstantDesc(partColumnType,
        getTypeAppropriateValueFor(partColumnType, value));

    return binaryPredicate("=", partColumnExpr, valueExpr);
  }

  public ExprNodeGenericFuncDesc binaryPredicate(String function, ExprNodeDesc lhs, ExprNodeDesc rhs)
    throws SemanticException {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getFunctionInfo(function).getGenericUDF(), Lists.newArrayList(lhs, rhs));
  }

  public ExprNodeGenericFuncDesc build() throws SemanticException {
    ExprNodeGenericFuncDesc resultExpr = null;

    for (Map.Entry<String, String> partSpec : partSpecs.entrySet()) {
      String column = partSpec.getKey();
      String value = partSpec.getValue();
      ExprNodeGenericFuncDesc partExpr = equalityPredicate(column, value);

      resultExpr = (resultExpr == null ? partExpr : binaryPredicate("and", resultExpr, partExpr));
    }

    return resultExpr;
  }
} // class ExpressionBuilder;
