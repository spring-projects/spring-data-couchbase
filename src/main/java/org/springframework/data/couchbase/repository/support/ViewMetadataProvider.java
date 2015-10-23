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
package org.springframework.data.couchbase.repository.support;

import org.springframework.data.couchbase.core.query.View;

/**
 * Interface to abstract {@link ViewMetadataProvider} that provides {@link View}s to be used for query execution.
 *
 * @author David Harrigan.
 */
public interface ViewMetadataProvider {

  /**
   * Returns the {@link View} to be used.
   *
   * @return the View, or null if the method hasn't been annotated with @View.
   */
  View getView();

}
