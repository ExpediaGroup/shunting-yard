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

import java.io.IOException;

import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.replicator.exec.receiver.TableSelector;

public class FilteringMessageReader implements MessageReader {

  private final MessageReader delegate;
  private final TableSelector tableSelector;
  private ListenerEvent current;

  public FilteringMessageReader(MessageReader delegate, TableSelector tableSelector) {
    this.delegate = delegate;
    this.tableSelector = tableSelector;
  }

  @Override
  public boolean hasNext() {
    while (delegate.hasNext()) {
      current = delegate.next();
      if (tableSelector.canProcess(current)) {
        return true;
      }
    }
    current = null;
    return false;
  }

  @Override
  public ListenerEvent next() {
    return current;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

}
