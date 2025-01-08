/*
 * Copyright 2022-2025 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.annotation.PersistenceCreator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A non-Enum object that has @JsonValue annotated method.
 *
 * @author Michael Reiche
 */
public class AirportJsonValuedObject {

	public AirportJsonValuedObject() {}

	@PersistenceCreator
	@JsonCreator
	public AirportJsonValuedObject(String theValue) {
		this.theValue = reverseMap.get(theValue);
		if (this.theValue == null) {
			throw new RuntimeException(
					"invalid value for " + this.getClass().getName() + " : " + theValue + ". Must be one of " + reverseMap);
		}
	}

	public String theValue;
	static Map<String, String> map = Map.of("first", "1st", "second", "2nd");
	static Map<String, String> reverseMap = new HashMap<>();
	static {
		map.entrySet().stream().map((entry) -> reverseMap.put(entry.getValue(), entry.getKey()))
				.collect(Collectors.toList());

	}

	@JsonValue
	public String getMapped() {
		return map.get(theValue);
	}
}
