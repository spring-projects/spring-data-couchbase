/*
 * Copyright 2012-2025 the original author or authors
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
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.couchbase.client.java.query.QueryScanConsistency;

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
		this._class = "abstractuser";
		this.subtype = AbstractingTypeMapper.Type.USER;
	}

	public String _class; // cheat a little so that will work with Java SDK

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

	static DateTime NOW_DateTime = DateTime.now(DateTimeZone.UTC);
	static Date NOW_Date = Date.from(Instant.now());
	@Version protected long version;

	@Encrypted(migration = Encrypted.Migration.FROM_UNENCRYPTED) public String encryptedField;

	@Encrypted public boolean encboolean;
	@Encrypted public boolean[] encbooleans;
	@Encrypted public Boolean encBoolean;
	@Encrypted public Boolean[] encBooleans;

	@Encrypted public long enclong;
	@Encrypted public long[] enclongs;
	@Encrypted public Long encLong;
	@Encrypted public Long[] encLongs;

	@Encrypted public short encshort;
	@Encrypted public short[] encshorts;
	@Encrypted public Short encShort;
	@Encrypted public Short[] encShorts;

	@Encrypted public int encinteger;
	@Encrypted public int[] encintegers;
	@Encrypted public Integer encInteger;
	@Encrypted public Integer[] encIntegers;

	@Encrypted public byte encbyte;
	public byte[] plainbytes;
	@Encrypted public byte[] encbytes;
	@Encrypted public Byte encByte;
	@Encrypted public Byte[] encBytes;

	@Encrypted public float encfloat;
	@Encrypted public float[] encfloats;
	@Encrypted public Float encFloat;
	@Encrypted public Float[] encFloats;

	@Encrypted public double encdouble;
	@Encrypted public double[] encdoubles;
	@Encrypted public Double encDouble;
	@Encrypted public Double[] encDoubles;

	@Encrypted public char encchar='x'; // need to initialize as char(0) is not legal
	@Encrypted public char[] encchars;
	@Encrypted public Character encCharacter;
	@Encrypted public Character[] encCharacters;

	@Encrypted public String encString;
	@Encrypted public String[] encStrings;

	@Encrypted public Date encDate;
	@Encrypted public Date[] encDates;

	@Encrypted public Locale encLocal;
	@Encrypted public Locale[] encLocales;

	@Encrypted public QueryScanConsistency encEnum;
	@Encrypted public QueryScanConsistency[] encEnums;

	@Encrypted public Class<?> clazz;

	@Encrypted public BigInteger encBigInteger;
	@Encrypted public BigDecimal encBigDecimal;
	@Encrypted public UUID encUUID;
	@Encrypted public DateTime encDateTime;

	@Encrypted public ETurbulenceCategory turbulence;
	@Encrypted public Address encAddress = new Address();

	public Date plainDate;
	public DateTime plainDateTime;

	public List nicknames;

	public Address homeAddress = null;
	public List<AddressWithEncStreet> addresses = new ArrayList<>();

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

	public void addAddress(AddressWithEncStreet address) {
		this.addresses.add(address);
	}

	public UserEncrypted withClass(String _class) {
		this._class = _class;
		return this;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId(), firstname, lastname);
	}

	public void initSimpleTypes() {

		encboolean = false;
		encbooleans = new boolean[] { true, false };
		encBoolean = true;
		encBooleans = new Boolean[] { true, false };

		enclong = 1;
		enclongs = new long[] { 1, 2 };
		encLong = Long.valueOf(1);
		encLongs = new Long[] { Long.valueOf(1), Long.valueOf(2) };

		encshort = 1;
		encshorts = new short[] { 3, 4 };
		encShort = 5;
		encShorts = new Short[] { 6, 7 };

		encinteger = 1;
		encintegers = new int[] { 2, 3 };
		encInteger = 4;
		encIntegers = new Integer[] { 5, 6 };

		encbyte = 32;
		encbytes = new byte[] { 1, 2, 3, 4 };
		plainbytes = new byte[] { 1, 2, 3, 4 };
		encByte = 48;
		encBytes = new Byte[] { 4, 5, 6, 7 };

		encfloat = 1;
		encfloats = new float[] { 1, 2 };
		encFloat = Float.valueOf("1.1");
		encFloats = new Float[] { encFloat };

		encdouble = 1.2;
		encdoubles = new double[] { 3.4, 5.6 };
		encDouble = 7.8;
		encDoubles = new Double[] { 9.10, 11.12 };

		encchar = 'a';
		encchars = new char[] { 'b', 'c', 'd' };
		encCharacter = 'a';
		encCharacters = new Character[] { 'a', 'b' };

		encString = "myS\"tring";
		encStrings = new String[] { "mySt\"ring" };

		encDate = NOW_Date;
		encDates = new Date[] { NOW_Date };

		encLocal = Locale.US;
		encLocales = new Locale[] { Locale.US };

		encEnum = QueryScanConsistency.NOT_BOUNDED;
		encEnums = new QueryScanConsistency[] { QueryScanConsistency.NOT_BOUNDED };

		encBigInteger = new BigInteger("123");
		encBigDecimal = new BigDecimal("456");

		encUUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

		//clazz = String.class;

		encDateTime = NOW_DateTime;
		plainDate = NOW_Date;
		plainDateTime = NOW_DateTime;

		nicknames = List.of("Happy", "Sleepy");

		turbulence = ETurbulenceCategory.T10;

	}
}
