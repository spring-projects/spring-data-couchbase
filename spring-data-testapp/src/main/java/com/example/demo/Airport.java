/*
 * Copyright 2022 the original author or authors.
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

package com.example.demo;

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Airport entity
 *
 * @author Michael Reiche
 */
@Document
@TypeAlias("airport")
public class Airport {
	@Id String id;
	String iata;
	String icao;
	LocalDateTime openDate;
	Long gates;
	String _class;
	@Version Number version;
	@CreatedBy private String createdBy;

	public Airport() {}

	//@PersistenceConstructor
	public Airport(String id, String iata, String icao) {
		this.id = id;
		this.iata = iata;
		this.icao = icao;
		this.openDate = null; // LocalDateTime.now();
		this.gates = Long.valueOf(200);
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

	public Long getGates() {
		return gates;
	}

	public String get_class() {
		return _class;
	}

	public LocalDateTime getOpenDate() {
		return openDate;
	}

	public Airport clearVersion() {
		version = Long.valueOf(0);
		return this;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{ ");
		sb.append(" id: " + id);
		sb.append(" iata: " + iata);
		sb.append(" icao: " + icao);
		sb.append(" gates: " + gates);
		sb.append(" open: "+openDate);
		// sb.append(" date: "+openDate);
		sb.append(" }");
		return sb.toString();
	}
}
