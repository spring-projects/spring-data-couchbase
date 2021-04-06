/*
 * Copyright 2017-2019 the original author or authors.
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

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * Airport entity
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@Document
@TypeAlias("airport")
public class Airport extends ComparableEntity {
	@Id String id;

	String iata;

	String icao;

	@CreatedBy private String createdBy;

	@Version long version;


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

	public String getCreatedBy() {
		return createdBy;
	}
}
