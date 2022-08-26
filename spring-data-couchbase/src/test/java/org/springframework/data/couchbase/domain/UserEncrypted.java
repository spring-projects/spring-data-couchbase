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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.couchbase.client.java.encryption.annotation.Encrypted;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * UserEncrypted entity for tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

@Document
@TypeAlias(AbstractingTypeMapper.Type.ABSTRACTUSER)
public class UserEncrypted extends AbstractUser implements Serializable {

	public UserEncrypted(){
		this.subtype = AbstractingTypeMapper.Type.USER;
	}

	public UserEncrypted( final String lastname, final String encryptedField) {
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
	@Transient protected String transientInfo;
	@CreatedBy protected String createdBy;
	@CreatedDate protected long createdDate;
	@LastModifiedBy protected String lastModifiedBy;
	@LastModifiedDate protected long lastModifiedDate;
	@Encrypted public String encryptedField;
	@Encrypted public Integer encInteger=1;
	@Encrypted public Long encLong=Long.valueOf(1);
	@Encrypted public Boolean encBoolean = Boolean.TRUE;

	List nicknames = List.of("Happy", "Sleepy");
	Address homeAddress = new Address();
	List<Address> addresses = new ArrayList<>();

	@Encrypted
	Address encAddress = new Address();

	public String getLastname() {
		return lastname;
	}

	public long getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public long getLastModifiedDate() {
		return lastModifiedDate;
	}

	public String getLastModifiedBy() {
		return lastModifiedBy;
	}


	public String getEncryptedField() {
		return encryptedField;
	}


	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public void setHomeAddress(Address address){
		this.homeAddress = address;
	}

	public void setEncAddress(Address address){
		this.encAddress = address;
	}

	public void addAddress(Address address){
		this.addresses.add(address);
	}
	@Override
	public int hashCode() {
		return Objects.hash(getId(), firstname, lastname);
	}

	public String getTransientInfo() {
		return transientInfo;
	}

	public void setTransientInfo(String something) {
		transientInfo = something;
	}

}
