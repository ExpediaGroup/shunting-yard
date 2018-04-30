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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import static com.hotels.shunting.yard.common.aws.SqsProperty.AWS_ACCESS_KEY;
import static com.hotels.shunting.yard.common.aws.SqsProperty.AWS_SECRET_KEY;
import static com.hotels.shunting.yard.common.aws.SqsProperty.GROUP_ID;
import static com.hotels.shunting.yard.common.aws.SqsProperty.QUEUE;
import static com.hotels.shunting.yard.common.aws.SqsProperty.REGION;
import static com.hotels.shunting.yard.common.aws.SqsProperty.WAIT_TIME_SECONDS;

import org.junit.Test;

import com.hotels.shunting.yard.common.aws.SqsProperty;

public class SqsPropertyTest {

  private static <T> Object asObject(T t) {
    return t;
  }

  private static String prefixedKey(String key) {
    return "com.hotels.bdp.circus.train.event.common.aws.sqs." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(SqsProperty.values().length, is(6));
  }

  @Test
  public void queue() {
    assertThat(QUEUE.unPrefixedKey(), is("queue"));
    assertThat(QUEUE.key(), is(prefixedKey("queue")));
    assertThat(QUEUE.defaultValue(), is(nullValue()));
  }

  @Test
  public void region() {
    assertThat(REGION.unPrefixedKey(), is("region"));
    assertThat(REGION.key(), is(prefixedKey("region")));
    assertThat(REGION.defaultValue(), is(asObject("us-west-2")));
  }

  @Test
  public void groupId() {
    assertThat(GROUP_ID.unPrefixedKey(), is("group.id"));
    assertThat(GROUP_ID.key(), is(prefixedKey("group.id")));
    assertThat(GROUP_ID.defaultValue(), is(nullValue()));
  }

  @Test
  public void waitTimeSeconds() {
    assertThat(WAIT_TIME_SECONDS.unPrefixedKey(), is("wait.time.seconds"));
    assertThat(WAIT_TIME_SECONDS.key(), is(prefixedKey("wait.time.seconds")));
    assertThat(WAIT_TIME_SECONDS.defaultValue(), is(asObject(10)));
  }

  @Test
  public void awsAccessKey() {
    assertThat(AWS_ACCESS_KEY.unPrefixedKey(), is("aws.access.key"));
    assertThat(AWS_ACCESS_KEY.key(), is(prefixedKey("aws.access.key")));
    assertThat(AWS_ACCESS_KEY.defaultValue(), is(nullValue()));
  }

  @Test
  public void awsSecretKey() {
    assertThat(AWS_SECRET_KEY.unPrefixedKey(), is("aws.secret.key"));
    assertThat(AWS_SECRET_KEY.key(), is(prefixedKey("aws.secret.key")));
    assertThat(AWS_SECRET_KEY.defaultValue(), is(nullValue()));
  }

}
