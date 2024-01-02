/*
 * Copyright 2012-2024 the original author or authors
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

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.core.mapping.Document;

import java.util.UUID;

/**
 * Person entity for tests.
 *
 * @author Tigran Babloyan
 */
@Document(durabilityLevel = DurabilityLevel.MAJORITY)
public class PersonWithDurability2 extends Person {
	public PersonWithDurability2() {
		setId(UUID.randomUUID());
		setMiddlename("Nick");
	}

	public PersonWithDurability2(String firstname, String lastname) {
		this();
		setFirstname(firstname);
		setLastname(lastname);
		isNew(true);
	}
}