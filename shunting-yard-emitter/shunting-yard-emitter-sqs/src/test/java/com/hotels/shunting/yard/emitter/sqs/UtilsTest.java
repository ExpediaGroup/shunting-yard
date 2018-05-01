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
package com.hotels.shunting.yard.emitter.sqs;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.shunting.yard.emitter.sqs.SqsProperty.GROUP_ID;
import static com.hotels.shunting.yard.emitter.sqs.SqsProperty.QUEUE;
import static com.hotels.shunting.yard.emitter.sqs.SqsProperty.REGION;
import static com.hotels.shunting.yard.emitter.sqs.Utils.groupId;
import static com.hotels.shunting.yard.emitter.sqs.Utils.queue;
import static com.hotels.shunting.yard.emitter.sqs.Utils.region;
import static com.hotels.shunting.yard.emitter.sqs.Utils.stringProperty;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UtilsTest {

  public @Rule ExpectedException exception = ExpectedException.none();

  private final Configuration conf = new Configuration();

  @Test
  public void stringPropertyReturnsConfValue() {
    conf.set(REGION.key(), "prop");
    assertThat(stringProperty(conf, REGION), is("prop"));
  }

  @Test
  public void stringPropertyReturnsDefaultValue() {
    assertThat(stringProperty(conf, REGION), is((String) REGION.defaultValue()));
  }

  @Test
  public void stringPropertyReturnsNull() {
    assertThat(stringProperty(conf, QUEUE), is(nullValue()));
  }

  @Test
  public void queueIsNotNull() {
    conf.set(QUEUE.key(), "queue");
    assertThat(queue(conf), is("queue"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void queueIsNull() {
    conf.set(QUEUE.key(), null);
    queue(conf);
  }

  @Test
  public void regionIsNotNull() {
    conf.set(REGION.key(), "region");
    assertThat(region(conf), is("region"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void regionIsNull() {
    conf.set(REGION.key(), null);
    region(conf);
  }

  @Test
  public void groupIdIsNotNull() {
    conf.set(GROUP_ID.key(), "group");
    assertThat(groupId(conf), is("group"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void groupIdIsNull() {
    conf.set(GROUP_ID.key(), null);
    groupId(conf);
  }

}
