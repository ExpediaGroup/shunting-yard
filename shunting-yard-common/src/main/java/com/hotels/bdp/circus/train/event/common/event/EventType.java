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
package com.hotels.bdp.circus.train.event.common.event;

/**
 * To make processing event in the receiver easier.
 */
public enum EventType {
  ON_CREATE_TABLE,
  ON_ALTER_TABLE,
  ON_DROP_TABLE,
  ON_ADD_PARTITION,
  ON_ALTER_PARTITION,
  ON_DROP_PARTITION,
  ON_INSERT;
}
