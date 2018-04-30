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
package com.hotels.bdp.circus.train.event.receiver.kafka;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circus.train.event.receiver.kafka.KafkaConsumerProperty.CLIENT_ID;
import static com.hotels.bdp.circus.train.event.receiver.kafka.KafkaConsumerProperty.ENABLE_AUTO_COMMIT;
import static com.hotels.bdp.circus.train.event.receiver.kafka.KafkaConsumerProperty.RETRY_BACKOFF_MS;
import static com.hotels.bdp.circus.train.event.receiver.kafka.KafkaConsumerProperty.SESSION_TIMEOUT_MS;
import static com.hotels.bdp.circus.train.event.receiver.kafka.KafkaConsumerProperty.TOPIC;
import static com.hotels.bdp.circus.train.event.receiver.kafka.Utils.booleanProperty;
import static com.hotels.bdp.circus.train.event.receiver.kafka.Utils.intProperty;
import static com.hotels.bdp.circus.train.event.receiver.kafka.Utils.longProperty;
import static com.hotels.bdp.circus.train.event.receiver.kafka.Utils.stringProperty;

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
    conf.set(CLIENT_ID.key(), "prop");
    assertThat(stringProperty(conf, CLIENT_ID), is("prop"));
  }

  @Test
  public void stringPropertyReturnsDefaultValue() {
    assertThat(stringProperty(conf, CLIENT_ID), is((String) CLIENT_ID.defaultValue()));
  }

  @Test
  public void stringPropertyReturnsNull() {
    assertThat(stringProperty(conf, TOPIC), is(nullValue()));
  }

  @Test
  public void booleanPropertyReturnsConfValue() {
    conf.set(ENABLE_AUTO_COMMIT.key(), "false");
    assertThat(booleanProperty(conf, ENABLE_AUTO_COMMIT), is(false));
  }

  @Test
  public void booleanPropertyReturnsDefaultValue() {
    assertThat(booleanProperty(conf, ENABLE_AUTO_COMMIT), is((boolean) ENABLE_AUTO_COMMIT.defaultValue()));
  }

  @Test(expected = NullPointerException.class)
  public void booleanPropertyThrowsNullPointerException() {
    // TOPIC is not an boolean property but its default value is null
    intProperty(conf, TOPIC);
  }

  @Test
  public void intPropertyReturnsConfValue() {
    conf.set(SESSION_TIMEOUT_MS.key(), "100");
    assertThat(intProperty(conf, SESSION_TIMEOUT_MS), is(100));
  }

  @Test
  public void intPropertyReturnsDefaultValue() {
    assertThat(intProperty(conf, SESSION_TIMEOUT_MS), is((int) SESSION_TIMEOUT_MS.defaultValue()));
  }

  @Test(expected = NullPointerException.class)
  public void intPropertyThrowsNullPointerException() {
    // TOPIC is not an int property but its default value is null
    intProperty(conf, TOPIC);
  }

  @Test
  public void longPropertyReturnsConfValue() {
    conf.set(RETRY_BACKOFF_MS.key(), "5000");
    assertThat(longProperty(conf, RETRY_BACKOFF_MS), is(5000L));
  }

  @Test
  public void longPropertyReturnsDefaultValue() {
    assertThat(longProperty(conf, RETRY_BACKOFF_MS), is((long) RETRY_BACKOFF_MS.defaultValue()));
  }

  @Test(expected = NullPointerException.class)
  public void longPropertyThrowsNullPointerException() {
    // TOPIC is not a long property but its default value is null
    longProperty(conf, TOPIC);
  }

}
