/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import org.springframework.data.annotation.Persistent;

/**
 * Identifies a domain object to be persisted to Couchbase.
 *
 * @author Michael Nitschinger
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Document {

  /**
   * An optional expiry time for the document. Default is no expiry.
   */
  int expiry() default 0;

  boolean touchOnRead() default false;

  /**
   * An optional time unit for the document's {@link #expiry()}, if set. Default is {@link TimeUnit#SECONDS}.
   */
  TimeUnit expiryUnit() default TimeUnit.SECONDS;

}
