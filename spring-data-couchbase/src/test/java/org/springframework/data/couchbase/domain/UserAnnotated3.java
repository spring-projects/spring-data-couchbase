/*
 * Copyright 2020 the original author or authors
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

import java.util.concurrent.TimeUnit;

import org.springframework.data.couchbase.core.mapping.Document;

/**
 * Annotated User entity for tests
 *
 * @author Michael Reiche
 */

@Document(expiry=1, expiryUnit = TimeUnit.SECONDS)
public class UserAnnotated3 extends User {

	public UserAnnotated3(String id, String firstname, String lastname) {
		super(id, firstname, lastname);
	}
}
