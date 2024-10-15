/*
 * Copyright 2020-2024 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

/**
 * SubscriptionTokenEntity for tests
 *
 * @author Michael Reiche
 */
@Document
public class SubscriptionToken {
  private @Id
  @GeneratedValue(strategy = GenerationStrategy.UNIQUE)
  String id;
  private @Version
  long version;
  private @Field
  String subscriptionType;
  private @Field
  String userName;
  private @Field
  String appId;
  private @Field
  String deviceId;
  private @Field
  long subscriptionDate;

  public SubscriptionToken(
      String id,
      long version,
      String subscriptionType,
      String userName,
      String appId,
      String deviceId,
      long subscriptionDate) {
    this.id = id;
    this.version = version;
    this.subscriptionType = subscriptionType;
    this.userName = userName;
    this.appId = appId;
    this.deviceId = deviceId;
    this.subscriptionDate = subscriptionDate;
  }

  public void setType(String type) {
    type = type;
  }

  public long getVersion() {
      return version;
  }

  public String getId() {
      return id;
  }
}
