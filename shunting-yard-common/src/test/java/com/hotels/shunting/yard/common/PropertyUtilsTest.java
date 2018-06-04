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
package com.hotels.shunting.yard.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.common.PropertyUtils.booleanProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.intProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.longProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.stringProperty;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyUtilsTest {

  private static final String STRING_PROPERTY = "property.string";
  private static final String BOOLEAN_PROPERTY = "property.boolean";
  private static final String INT_PROPERTY = "property.int";
  private static final String LONG_PROPERTY = "property.long";

  public @Rule ExpectedException exception = ExpectedException.none();

  private @Mock Property property;

  private final Configuration conf = new Configuration();

  @Before
  public void init() {
    conf.set(STRING_PROPERTY, "string");
    conf.setBoolean(BOOLEAN_PROPERTY, true);
    conf.setInt(INT_PROPERTY, 1024);
    conf.setLong(LONG_PROPERTY, 18000L);
  }

  @Test
  public void stringPropertyReturnsConfValue() {
    when(property.key()).thenReturn(STRING_PROPERTY);
    when(property.defaultValue()).thenReturn("default");
    assertThat(stringProperty(conf, property)).isEqualTo("string");
  }

  @Test
  public void stringPropertyReturnsDefaultValue() {
    when(property.key()).thenReturn("unset");
    when(property.defaultValue()).thenReturn("default");
    assertThat(stringProperty(conf, property)).isEqualTo("default");
  }

  @Test
  public void stringPropertyReturnsNull() {
    when(property.key()).thenReturn("unset");
    assertThat(stringProperty(conf, property)).isNull();
  }

  @Test
  public void booleanPropertyPropertyReturnsConfValue() {
    when(property.key()).thenReturn(BOOLEAN_PROPERTY);
    when(property.defaultValue()).thenReturn(false);
    assertThat(booleanProperty(conf, property)).isTrue();
  }

  @Test
  public void booleanPropertyPropertyReturnsDefaultValue() {
    when(property.key()).thenReturn("unset");
    when(property.defaultValue()).thenReturn(false);
    assertThat(booleanProperty(conf, property)).isFalse();
  }

  @Test(expected = NullPointerException.class)
  public void booleanPropertyPropertyThrowsNullPointerException() {
    when(property.key()).thenReturn("unset");
    booleanProperty(conf, property);
  }

  @Test
  public void intPropertyReturnsConfValue() {
    when(property.key()).thenReturn(INT_PROPERTY);
    when(property.defaultValue()).thenReturn(100);
    assertThat(intProperty(conf, property)).isEqualTo(1024);
  }

  @Test
  public void intPropertyReturnsDefaultValue() {
    when(property.key()).thenReturn("unset");
    when(property.defaultValue()).thenReturn(100);
    assertThat(intProperty(conf, property)).isEqualTo(100);
  }

  @Test(expected = NullPointerException.class)
  public void intPropertyThrowsNullPointerException() {
    when(property.key()).thenReturn("unset");
    intProperty(conf, property);
  }

  @Test
  public void longPropertyReturnsConfValue() {
    when(property.key()).thenReturn(LONG_PROPERTY);
    when(property.defaultValue()).thenReturn(1234567890L);
    assertThat(longProperty(conf, property)).isEqualTo(18000L);
  }

  @Test
  public void longPropertyReturnsDefaultValue() {
    when(property.key()).thenReturn("unset");
    when(property.defaultValue()).thenReturn(1234567890L);
    assertThat(longProperty(conf, property)).isEqualTo(1234567890L);
  }

  @Test(expected = NullPointerException.class)
  public void longPropertyThrowsNullPointerException() {
    when(property.key()).thenReturn("unset");
    longProperty(conf, property);
  }

}
