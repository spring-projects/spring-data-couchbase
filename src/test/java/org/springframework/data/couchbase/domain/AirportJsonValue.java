/*
 * Copyright 2022-present the original author or authors.
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

import jakarta.validation.constraints.Max;

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Expiration;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * AirportJsonValue entity. From Airport.
 *
 * @author Michael Reiche
 */
@Document
@TypeAlias("airport")
public class AirportJsonValue extends ComparableEntity {
	@Id String key;

	String iata;

	@JsonFormat(pattern = "abc") String icao;

	ETurbulenceCategory turbulence1;
	ETurbulenceCategory turbulence2;
	EITurbulenceCategory turbulence3;
	EJsonCreatorTurbulenceCategory turbulence4;
	EBTurbulenceCategory turbulence5 ;

	@Version Number version;
	@CreatedBy private String createdBy;
	@Expiration private long expiration;
	@Max(2) long size;

	public AirportJsonValuedObject jvObject;

	{
		this.jvObject = new AirportJsonValuedObject();
	}

	public AirportJsonValue() {}

	@PersistenceCreator
	public AirportJsonValue(String key, String iata, String icao) {
		this.key = key;
		this.iata = iata;
		this.icao = icao;
	}

	public String getId() {
		return key;
	}

	public String getIata() {
		return iata;
	}

	public String getIcao() {
		return icao;
	}

	public long getExpiration() {
		return expiration;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public void setTurbulence1(ETurbulenceCategory turbulence1) {
		this.turbulence1 = turbulence1;
	}

	public ETurbulenceCategory getTurbulence1() {
		return turbulence1;
	}

	public void setTurbulence2(ETurbulenceCategory turbulence) {
		this.turbulence2 = turbulence;
	}

	public ETurbulenceCategory getTurbulence2() {
		return turbulence2;
	}

	public void setTurbulence3(EITurbulenceCategory turbulence3) {
		this.turbulence3 = turbulence3;
	}

	public EITurbulenceCategory getTurbulence3() {
		return turbulence3;
	}

	public void setTurbulence4(EJsonCreatorTurbulenceCategory turbulence4) {
		this.turbulence4 = turbulence4;
	}

	public EJsonCreatorTurbulenceCategory getTurbulence4() {
		return turbulence4;
	}

	public void setTurbulence5(EBTurbulenceCategory turbulence5) {
		this.turbulence5 = turbulence5;
	}

	public EBTurbulenceCategory getTurbulence5() {
		return turbulence5;
	}

	public void setJvObject(AirportJsonValuedObject jvObject) {
		this.jvObject = jvObject;
	}

	public AirportJsonValuedObject getJvObject() {
		return jvObject;
	}
}
