/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.common;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.shuntingyard.common.Preconditions.checkNotNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PreconditionsTest {

  public @Rule ExpectedException exception = ExpectedException.none();

  @Test
  public void checkNotNullSucceeds() {
    assertThat(checkNotNull("", "message")).isSameAs("");
  }

  @Test
  public void checkNotNullFails() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("message");
    checkNotNull(null, "message");
  }

}
