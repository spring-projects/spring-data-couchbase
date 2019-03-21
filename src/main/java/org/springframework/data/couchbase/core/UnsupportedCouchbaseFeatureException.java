/*
 * Copyright 2012-2015 the original author or authors
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

package org.springframework.data.couchbase.core;

import com.couchbase.client.java.util.features.CouchbaseFeature;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessException;

/**
 * A {@link NonTransientDataAccessException} that denotes that a particular feature is expected
 * on the server side but is not available.
 */
public class UnsupportedCouchbaseFeatureException extends InvalidDataAccessApiUsageException {

  private final CouchbaseFeature feature;

  public UnsupportedCouchbaseFeatureException(String msg, CouchbaseFeature feature) {
    super(msg);
    this.feature = feature;
  }

  public UnsupportedCouchbaseFeatureException(String msg, CouchbaseFeature feature, Throwable cause) {
    super(msg, cause);
    this.feature = feature;
  }

  /**
   * @return the {@link CouchbaseFeature} that was missing (could be null if not
   * a registered CouchbaseFeature, in which case see {@link #getMessage()}).
   */
  public CouchbaseFeature getFeature() {
    return feature;
  }
}
