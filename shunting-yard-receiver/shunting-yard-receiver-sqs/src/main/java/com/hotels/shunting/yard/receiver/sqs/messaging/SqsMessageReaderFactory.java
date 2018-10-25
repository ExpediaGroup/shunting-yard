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
package com.hotels.shunting.yard.receiver.sqs.messaging;

import org.apache.hadoop.conf.Configuration;

import com.hotels.shunting.yard.common.io.jackson.ApiarySqsMessageSerDe;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.common.messaging.MessageReaderFactory;

public class SqsMessageReaderFactory implements MessageReaderFactory {

  @Override
  public MessageReader newInstance(Configuration conf, ApiarySqsMessageSerDe sqsMessageSerde) {
    return new SqsMessageReader(conf, sqsMessageSerde);
  }

}
