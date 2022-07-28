/*
 * Copyright 2012-2022 the original author or authors
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

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.couchbase.core.mapping.Document;

/**
 * Person entity without a an @Version property
 *
 * @author Michael Reiche
 */
@Document
public class PersonWithoutVersion extends AbstractEntity {
	Optional<String> firstname;
	Optional<String> lastname;

	public PersonWithoutVersion() {
		firstname = Optional.empty();
		lastname = Optional.empty();
	}

	public PersonWithoutVersion(String firstname, String lastname) {
		this.firstname = Optional.of(firstname);
		this.lastname = Optional.of(lastname);
		setId(UUID.randomUUID());
	}

	public PersonWithoutVersion(UUID id, String firstname, String lastname) {
		this.firstname = Optional.of(firstname);
		this.lastname = Optional.of(lastname);
		setId(id);
	}
}
