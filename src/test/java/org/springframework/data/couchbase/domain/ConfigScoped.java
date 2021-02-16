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

package org.springframework.data.couchbase.domain;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;

/**
 * Configuration that uses a scope. This is a separate class as it is difficult to debug if you forget to unset the
 * scopeName and the config is used for non-collection operations.
 *
 * @Author Michael Reiche
 */
@Configuration
@EnableCouchbaseRepositories
@EnableCouchbaseAuditing // this activates auditing
public class ConfigScoped extends Config {

	static String scopeName = null;

	@Override
	protected String getScopeName() {
		return scopeName;
	}

	public static void setScopeName(String scopeName) {
		ConfigScoped.scopeName = scopeName;
	}
}
