/*
 * Copyright 2021 the original author or authors
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
package org.springframework.data.couchbase.core.support;

import com.couchbase.client.java.kv.ExistsOptions;

/**
 * Interface for operations that take ExistsOptions
 *
 * @author Michael Reiche
 * @param <T> - the entity class
 */
public interface WithExistsOptions<T> {
	/**
	 * Specify options
	 *
	 * @param options -  exists options
	 */
	Object withOptions(ExistsOptions options);
}
