/*
 * Copyright 2012-present the original author or authors
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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.core.mapping.Document;

@Document
/**
 * @author Michael Reiche
 */
public class BigAirline extends Airline {
	BigInteger airlineNumber = new BigInteger("88881234567890123456"); // less than 63 bits, otherwise query truncates
	BigDecimal airlineDecimal = new BigDecimal("888812345678901.23"); // less than 53 bits in mantissa

	@PersistenceConstructor
	public BigAirline(String id, String name, String hqCountry, Number airlineNumber, Number airlineDecimal) {
		super(id, name, hqCountry);
		this.airlineNumber = airlineNumber != null && !airlineNumber.equals("")
				? new BigInteger(airlineNumber.toString())
				: this.airlineNumber;
		this.airlineDecimal = airlineDecimal != null && !airlineDecimal.equals("")
				? new BigDecimal(airlineDecimal.toString())
				: this.airlineDecimal;
	}

	public BigInteger getAirlineNumber() {
		return airlineNumber;
	}

	public BigDecimal getAirlineDecimal() {
		return airlineDecimal;
	}

}
