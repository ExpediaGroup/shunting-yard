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
package com.hotels.shunting.yard.emitter.kafka;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.ACKS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BUFFER_MEMORY;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.RETRIES;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.TOPIC;
import static com.hotels.shunting.yard.emitter.kafka.Utils.intProperty;
import static com.hotels.shunting.yard.emitter.kafka.Utils.longProperty;
import static com.hotels.shunting.yard.emitter.kafka.Utils.stringProperty;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UtilsTest {

  public @Rule ExpectedException exception = ExpectedException.none();

  private Configuration conf = new Configuration();

  @Test
  public void stringPropertyReturnsConfValue() {
    conf.set(ACKS.key(), "prop");
    assertThat(stringProperty(conf, ACKS), is("prop"));
  }

  @Test
  public void stringPropertyReturnsDefaultValue() {
    assertThat(stringProperty(conf, ACKS), is((String) ACKS.defaultValue()));
  }

  @Test
  public void stringPropertyReturnsNull() {
    assertThat(stringProperty(conf, TOPIC), is(nullValue()));
  }

  @Test
  public void intPropertyReturnsConfValue() {
    conf.set(RETRIES.key(), "100");
    assertThat(intProperty(conf, RETRIES), is(100));
  }

  @Test
  public void intPropertyReturnsDefaultValue() {
    assertThat(intProperty(conf, RETRIES), is((int) RETRIES.defaultValue()));
  }

  @Test(expected = NullPointerException.class)
  public void intPropertyThrowsNullPointerException() {
    // TOPIC is not an int property but its default value is null
    intProperty(conf, TOPIC);
  }

  @Test
  public void longPropertyReturnsConfValue() {
    conf.set(BUFFER_MEMORY.key(), "5000");
    assertThat(longProperty(conf, BUFFER_MEMORY), is(5000L));
  }

  @Test
  public void longPropertyReturnsDefaultValue() {
    assertThat(longProperty(conf, BUFFER_MEMORY), is((long) BUFFER_MEMORY.defaultValue()));
  }

  @Test(expected = NullPointerException.class)
  public void longPropertyThrowsNullPointerException() {
    // TOPIC is not a long property but its default value is null
    longProperty(conf, TOPIC);
  }

}
