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
package com.hotels.shunting.yard.common.io.jackson;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocol;

enum Fields implements TFieldIdEnum {
  IGNORE;

  @Override
  public short getThriftFieldId() {
    return 0;
  }

  @Override
  public String getFieldName() {
    return null;
  }
}

class InvalidTBase implements TBase<InvalidTBase, Fields> {
  private static final long serialVersionUID = 1L;

  @Override
  public int compareTo(InvalidTBase o) {
    return 0;
  }

  @Override
  public void read(TProtocol iprot) throws TException {}

  @Override
  public void write(TProtocol oprot) throws TException {}

  @Override
  public Fields fieldForId(int fieldId) {
    return null;
  }

  @Override
  public boolean isSet(Fields field) {
    return false;
  }

  @Override
  public Object getFieldValue(Fields field) {
    return null;
  }

  @Override
  public void setFieldValue(Fields field, Object value) {}

  @Override
  public TBase<InvalidTBase, Fields> deepCopy() {
    return null;
  }

  @Override
  public void clear() {}
}
