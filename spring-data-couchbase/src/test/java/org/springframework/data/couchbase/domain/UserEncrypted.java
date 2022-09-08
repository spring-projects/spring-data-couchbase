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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

import com.couchbase.client.java.encryption.annotation.Encrypted;

/**
 * UserEncrypted entity for tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

@Document
@TypeAlias(AbstractingTypeMapper.Type.ABSTRACTUSER)
public class UserEncrypted extends AbstractUser implements Serializable {

	public UserEncrypted() {
		this.subtype = AbstractingTypeMapper.Type.USER;
	}

	public UserEncrypted(final String lastname, final String encryptedField) {
		this();
		this.id = UUID.randomUUID().toString();
		this.lastname = lastname;
		this.encryptedField = encryptedField;
	}

	@PersistenceConstructor
	public UserEncrypted(final String id, final String firstname, final String lastname) {
		this();
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
	}

	public UserEncrypted(final String id, final String firstname, final String lastname, final String encryptedField) {
		this();
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.encryptedField = encryptedField;
	}

	@Version protected long version;
	@Encrypted public String encryptedField;
	@Encrypted public Integer encInteger = 1;
	@Encrypted public Long encLong = Long.valueOf(1);
	@Encrypted public Boolean encBoolean = Boolean.TRUE;
	@Encrypted public BigInteger encBigInteger = new BigInteger("123");
	@Encrypted public BigDecimal encBigDecimal = new BigDecimal("456");
	@Encrypted public UUID encUUID = UUID.randomUUID();
	@Encrypted public DateTime encDateTime = DateTime.now(DateTimeZone.UTC);
	@Encrypted public Date encDate = Date.from(Instant.now());
	@Encrypted public Address encAddress = new Address();

	public Date plainDate = Date.from(Instant.now());
	public DateTime plainDateTime = DateTime.now(DateTimeZone.UTC);

	public List nicknames = List.of("Happy", "Sleepy");


	public Address homeAddress = null;
	public List<Address> addresses = new ArrayList<>();

	public String getLastname() {
		return lastname;
	}

	public long getVersion() {
		return version;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public void setHomeAddress(Address address) {
		this.homeAddress = address;
	}

	public void setEncAddress(Address address) {
		this.encAddress = address;
	}

	public void addAddress(Address address) {
		this.addresses.add(address);
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId(), firstname, lastname);
	}

}
