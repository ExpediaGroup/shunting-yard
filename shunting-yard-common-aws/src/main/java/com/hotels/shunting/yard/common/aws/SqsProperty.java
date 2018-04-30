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
package com.hotels.shunting.yard.common.aws;

import com.amazonaws.regions.Regions;

public enum SqsProperty {
  QUEUE("queue", null),
  REGION("region", Regions.US_WEST_2.getName()),
  GROUP_ID("group.id", null),
  WAIT_TIME_SECONDS("wait.time.seconds", 10),
  AWS_ACCESS_KEY("aws.access.key", null),
  AWS_SECRET_KEY("aws.secret.key", null);

  private static final String PROPERTY_PREFIX = "com.hotels.bdp.circus.train.event.common.aws.sqs.";

  private final String unPrefixedKey;
  private final Object defaultValue;

  private SqsProperty(String unPrefixedKey, Object defaultValue) {
    this.unPrefixedKey = unPrefixedKey;
    this.defaultValue = defaultValue;
  }

  public String key() {
    return new StringBuffer(PROPERTY_PREFIX).append(unPrefixedKey).toString();
  }

  public String unPrefixedKey() {
    return unPrefixedKey;
  }

  public Object defaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return key();
  }

}
