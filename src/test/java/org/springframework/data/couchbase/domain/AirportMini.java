/*
 * Copyright 2017-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.domain;

import java.util.Objects;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * AirportMini entity
 *
 * @author Michael Reiche
 */
@Document
public class AirportMini extends ComparableEntity {

	@Id private String id;
	private String iata;
	private Address address;

	@PersistenceConstructor
		public AirportMini(final String id, final String iata) {
			this.id = id;
			this.iata = iata;
		}

	public String getId() {
		return id;
	}

	public String getIata() {
		return iata;
	}

	public void setIata(String iata) {
		this.iata = iata;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, iata);
	}

}
