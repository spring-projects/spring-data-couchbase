/*
 * Copyright 2012-2020 the original author or authors
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

package org.springframework.data.couchbase.core.mapping;

/**
 * Marker Interface to identify either a {@link CouchbaseDocument} or a {@link CouchbaseList}.
 * <p/>
 * This interface will be extended in the future to refactor the needed infrastructure into the common interface.
 *
 * @author Michael Nitschinger
 */
public interface CouchbaseStorable {
}
