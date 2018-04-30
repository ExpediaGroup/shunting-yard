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
package com.hotels.shunting.yard.receiver.kinesis;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.MAX_RECORDS;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.STREAM;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.WORKER_ID;
import static com.hotels.shunting.yard.receiver.kinesis.Utils.intProperty;
import static com.hotels.shunting.yard.receiver.kinesis.Utils.stringProperty;

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
    conf.set(WORKER_ID.key(), "prop");
    assertThat(stringProperty(conf, WORKER_ID), is("prop"));
  }

  @Test
  public void stringPropertyReturnsDefaultValue() {
    assertThat(stringProperty(conf, WORKER_ID), is((String) WORKER_ID.defaultValue()));
  }

  @Test
  public void stringPropertyReturnsNull() {
    assertThat(stringProperty(conf, STREAM), is(nullValue()));
  }

  @Test(expected = NullPointerException.class)
  public void booleanPropertyThrowsNullPointerException() {
    // TOPIC is not an boolean property but its default value is null
    intProperty(conf, STREAM);
  }

  @Test
  public void intPropertyReturnsConfValue() {
    conf.set(MAX_RECORDS.key(), "100");
    assertThat(intProperty(conf, MAX_RECORDS), is(100));
  }

  @Test
  public void intPropertyReturnsDefaultValue() {
    assertThat(intProperty(conf, MAX_RECORDS), is((int) MAX_RECORDS.defaultValue()));
  }

  @Test(expected = NullPointerException.class)
  public void intPropertyThrowsNullPointerException() {
    // TOPIC is not an int property but its default value is null
    intProperty(conf, STREAM);
  }

}
