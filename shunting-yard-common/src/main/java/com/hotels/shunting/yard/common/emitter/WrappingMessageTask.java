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
package com.hotels.shunting.yard.common.emitter;

import static com.hotels.shunting.yard.common.emitter.EmitterUtils.error;
import static com.hotels.shunting.yard.common.emitter.EmitterUtils.success;

import java.util.Objects;

import com.hotels.shunting.yard.common.messaging.MessageTask;

class WrappingMessageTask implements MessageTask {

  private final MessageTask task;

  WrappingMessageTask(MessageTask task) {
    this.task = task;
  }

  @Override
  public void run() {
    try {
      task.run();
      success();
    } catch (Exception e) {
      error(e);
    }
  }

  // For testing
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    WrappingMessageTask other = (WrappingMessageTask) obj;
    return Objects.equals(task, other.task);
  }

}
