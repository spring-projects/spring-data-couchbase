/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Defines the callback which will be wrapped and executed on a bucket.
 *
 * @author Michael Nitschinger
 */
public interface BucketCallback<T> {

  /**
   * The enclosed body will be executed on the connected bucket.
   *
   * @return the result of the enclosed execution.
   * @throws TimeoutException if the enclosed operation timed out.
   * @throws ExecutionException if the result could not be retrieved because of a thrown exception before.
   * @throws InterruptedException if the enclosed operation was interrupted.
   */
  T doInBucket() throws TimeoutException, ExecutionException, InterruptedException;

}
