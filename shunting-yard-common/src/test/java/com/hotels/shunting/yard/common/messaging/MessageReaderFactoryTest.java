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
package com.hotels.shunting.yard.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;

@RunWith(MockitoJUnitRunner.class)
public class MessageReaderFactoryTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private @Mock Configuration conf;
  private @Mock MetaStoreEventSerDe metaStoreEventSerDe;

  private final MessageReaderFactory factory = MessageReaderFactory.DEFAULT;

  @Test
  public void compliant() {
    MessageReader reader = factory.newInstance(CompliantMessageReader.class.getName(), conf, metaStoreEventSerDe);
    assertThat(reader).isNotNull().isExactlyInstanceOf(CompliantMessageReader.class);
  }

  @Test
  public void nonCompliant() {
    expectedException.expect(ShuntingYardException.class);
    expectedException.expectCause(instanceOf(NoSuchMethodException.class));
    factory.newInstance(NonCompliantMessageReader.class.getName(), conf, metaStoreEventSerDe);
  }

  @Test
  public void bogus() {
    expectedException.expect(ShuntingYardException.class);
    factory.newInstance(BogusMessageReader.class.getName(), conf, metaStoreEventSerDe);
  }

  @Test
  public void messageReaderClassNotFound() {
    expectedException.expect(ShuntingYardException.class);
    expectedException.expectCause(instanceOf(ClassNotFoundException.class));
    factory.newInstance("com.hotels.shunting.yard.common.messaging.UnknownMessageReader", conf, metaStoreEventSerDe);
  }
}
