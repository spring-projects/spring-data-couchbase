/*
 * Copyright 2017-2021 the original author or authors.
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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * Airport entity
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@Document
public class Airport extends ComparableEntity {
	@Id String id;

	String iata;

	String icao;

	@Version Number version;

	@PersistenceConstructor
	public Airport(String id, String iata, String icao) {
		this.id = id;
		this.iata = iata;
		this.icao = icao;
	}

	public String getId() {
		return id;
	}

	public String getIata() {
		return iata;
	}

	public String getIcao() {
		return icao;
	}

	public Airport withId(String id) {
		return new Airport(id, this.iata, this.icao);
	}

	public Airport withIcao(String icao) {
		return new Airport(this.getId(), this.iata, icao);
	}

	public Airport clearVersion() {
		version = Long.valueOf(0);
		return this;
	}
}
