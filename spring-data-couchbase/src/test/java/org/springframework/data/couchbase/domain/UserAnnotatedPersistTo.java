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

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.core.mapping.Document;

import java.io.Serializable;

/**
 * Annotated User entity for tests
 *
 * @author Tigran Babloyan
 */

@Document(persistTo = PersistTo.ACTIVE)
public class UserAnnotatedPersistTo extends User implements Serializable {

	public UserAnnotatedPersistTo(String id, String firstname, String lastname) {
		super(id, firstname, lastname);
	}
}
