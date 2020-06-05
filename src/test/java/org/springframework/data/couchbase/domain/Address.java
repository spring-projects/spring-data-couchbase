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

import org.springframework.data.couchbase.core.mapping.Document;

/**
 * nested domain object for manual testing
 * 
 * @author Michael Reiche
 */
@Document
public class Address extends AbstractEntity {

	String street;

	public Address() {}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ street : ");
		sb.append(getStreet());
		sb.append(" }");
		return sb.toString();
	}
}
