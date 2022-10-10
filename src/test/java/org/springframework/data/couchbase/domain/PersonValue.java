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

import lombok.Value;
import lombok.With;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

/**
 * PersonValue entity for tests
 *
 * @author Michael Reiche
 */

@Value
@Document
public class PersonValue {
	@Id @GeneratedValue(strategy = GenerationStrategy.UNIQUE)
	@With String id;
  @Version
	@With
	long version;
	@Field String firstname;
	@Field String lastname;

	public PersonValue(String id, long version, String firstname, String lastname) {
		this.id = id;
		this.version = version;
		this.firstname = firstname;
		this.lastname = lastname;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("PersonValue : {");
		sb.append(" id : " + getId());
		sb.append(", version : " + version);
		sb.append(", firstname : " + firstname);
		sb.append(", lastname : " + lastname);
		sb.append(" }");
		return sb.toString();
	}

}
