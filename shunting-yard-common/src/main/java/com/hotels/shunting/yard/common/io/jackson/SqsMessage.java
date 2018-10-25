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
package com.hotels.shunting.yard.common.io.jackson;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

import com.fasterxml.jackson.annotation.JsonProperty;

@NotThreadSafe
public class SqsMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  @JsonProperty("Type")
  private String type;

  @JsonProperty("MessageId")
  private String messageId;

  @JsonProperty("TopicArn")
  private String topicArn;

  @JsonProperty("Message")
  private String message;

  @JsonProperty("Timestamp")
  private String timestamp;

  @JsonProperty("SignatureVersion")
  private String signatureVersion;

  @JsonProperty("Signature")
  private String signature;

  @JsonProperty("SigningCertURL")
  private String signingCertURL;

  @JsonProperty("UnsubscribeURL")
  private String unsubscribeURL;

  SqsMessage() {}

  public SqsMessage(
      String type,
      String messageId,
      String topicArn,
      String message,
      String timestamp,
      String signatureVersion,
      String signature,
      String signingCertURL,
      String unsubscribeURL) {
    this.type = type;
    this.messageId = messageId;
    this.topicArn = topicArn;
    this.message = message;
    this.timestamp = timestamp;
    this.signatureVersion = signatureVersion;
    this.signature = signature;
    this.signingCertURL = signingCertURL;
    this.unsubscribeURL = unsubscribeURL;
  }

  public String getType() {
    return type;
  }

  public String getMessageId() {
    return messageId;
  }

  public String getTopicArn() {
    return topicArn;
  }

  public String getMessage() {
    return message;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getSignatureVersion() {
    return signatureVersion;
  }

  public String getSignature() {
    return signature;
  }

  public String getSigningCertURL() {
    return signingCertURL;
  }

  public String getUnsubscribeURL() {
    return unsubscribeURL;
  }

}
