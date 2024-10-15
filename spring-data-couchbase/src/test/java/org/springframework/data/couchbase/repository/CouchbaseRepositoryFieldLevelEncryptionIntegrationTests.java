/*
 * Copyright 2012-2024 the original author or authors
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

package org.springframework.data.couchbase.repository;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.AddressWithEncStreet;
import org.springframework.data.couchbase.domain.ETurbulenceCategory;
import org.springframework.data.couchbase.domain.TestEncrypted;
import org.springframework.data.couchbase.domain.UserEncrypted;
import org.springframework.data.couchbase.domain.UserEncryptedRepository;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.encryption.AeadAes256CbcHmacSha512Provider;
import com.couchbase.client.encryption.DefaultCryptoManager;
import com.couchbase.client.encryption.Keyring;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

/**
 * Repository KV tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@SpringJUnitConfig(CouchbaseRepositoryFieldLevelEncryptionIntegrationTests.Config.class)
@DirtiesContext
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryFieldLevelEncryptionIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired UserEncryptedRepository userEncryptedRepository;
	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired CouchbaseTemplate couchbaseTemplate;

	@BeforeEach
	public void beforeEach() {
		super.beforeEach();
		List<UserEncrypted> users = couchbaseTemplate.findByQuery(UserEncrypted.class).withConsistency(REQUEST_PLUS).all();
		for (UserEncrypted user : users) {
			couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId());
			try { // may have also used upperCased-id
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId().toUpperCase());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
		couchbaseTemplate.removeByQuery(UserEncrypted.class).all();
		couchbaseTemplate.findByQuery(UserEncrypted.class).withConsistency(REQUEST_PLUS).all();
	}

	@Test
	void javaSDKEncryption() {

	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindByTestId() {
		boolean cleanAfter = true;
		TestEncrypted user = new TestEncrypted(UUID.randomUUID().toString());
		user.initSimpleTypes();
		couchbaseTemplate.save(user);
		TestEncrypted writeSpringReadSpring = couchbaseTemplate.findById(TestEncrypted.class).one(user.id);
		System.err.println(user);
		System.err.println(writeSpringReadSpring);
		assertEquals(user.toString(), writeSpringReadSpring.toString());

		TestEncrypted writeSpringReadSDK = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.id).contentAs(TestEncrypted.class);
		writeSpringReadSDK.setId(user.id);
		assertEquals(user.toString(), writeSpringReadSDK.toString());

		clientFactory.getCluster().bucket(config().bucketname()).defaultCollection().insert(user.getId().toUpperCase(),
				user);
		TestEncrypted writeSDKReadSDK = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.getId().toUpperCase()).contentAs(TestEncrypted.class);
		writeSDKReadSDK.setId(user.getId());
		assertEquals(user.toString(), writeSDKReadSDK.toString());

		TestEncrypted writeSDKReadSpring = couchbaseTemplate.findById(TestEncrypted.class).one(user.getId().toUpperCase());
		writeSDKReadSpring.setId(user.getId());

		assertEquals(user.toString(), writeSDKReadSpring.toString());
		if (cleanAfter) {
			try {
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void writeSpring_readSpring() {
		boolean cleanAfter = true;
		UserEncrypted user = new UserEncrypted(UUID.randomUUID().toString(), "writeSpring_readSpring", "l", "hel\"lo");
		AddressWithEncStreet address = new AddressWithEncStreet(); // plaintext address with encrypted street
		address.setEncStreet("Olcott Street");
		address.setCity("Santa Clara");
		user.addAddress(address);
		user.setHomeAddress(null);
		Address encAddress = new Address(); // encrypted address with plaintext street.
		encAddress.setStreet("Castro St");
		encAddress.setCity("Mountain View");
		encAddress.setTurbulence(ETurbulenceCategory.T10);
		user.setEncAddress(encAddress);

		user.initSimpleTypes();
		// save the user with spring
		assertFalse(userEncryptedRepository.existsById(user.getId()));
		userEncryptedRepository.save(user);
		// read the user with Spring
		Optional<UserEncrypted> writeSpringReadSpring = userEncryptedRepository.findById(user.getId());
		assertTrue(writeSpringReadSpring.isPresent());
		writeSpringReadSpring.ifPresent(u -> assertEquals(user, u));

		if (cleanAfter) {
			try {
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void writeSpring_readSDK() {
		boolean cleanAfter = true;
		UserEncrypted user = new UserEncrypted(UUID.randomUUID().toString(), "writeSpring_readSDK", "l", "hel\"lo");
		AddressWithEncStreet address = new AddressWithEncStreet(); // plaintext address with encrypted street
		address.setEncStreet("Olcott Street");
		address.setCity("Santa Clara");
		user.addAddress(address);
		user.setHomeAddress(null);
		Address encAddress = new Address(); // encrypted address with plaintext street.
		encAddress.setStreet("Castro St");
		encAddress.setCity("Mountain View");
		user.setEncAddress(encAddress);

		user.initSimpleTypes();
		// save the user with spring
		assertFalse(userEncryptedRepository.existsById(user.getId()));
		userEncryptedRepository.save(user);

		// read user with SDK
		UserEncrypted writeSpringReadSDK = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.getId()).contentAs(UserEncrypted.class);
		writeSpringReadSDK.setId(user.getId());
		writeSpringReadSDK.setVersion(user.getVersion());
		assertEquals(user, writeSpringReadSDK);

		if (cleanAfter) {
			try {
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void writeSDK_readSpring() {
		boolean cleanAfter = true;
		UserEncrypted user = new UserEncrypted(UUID.randomUUID().toString(), "writeSDK_readSpring", "l", "hel\"lo");
		AddressWithEncStreet address = new AddressWithEncStreet(); // plaintext address with encrypted street
		address.setEncStreet("Olcott Street");
		address.setCity("Santa Clara");
		user.addAddress(address);
		user.setHomeAddress(null);
		Address encAddress = new Address(); // encrypted address with plaintext street.
		encAddress.setStreet("Castro St");
		encAddress.setCity("Mountain View");
		user.setEncAddress(encAddress);

		user.initSimpleTypes();

		// save the user with the SDK
		assertFalse(userEncryptedRepository.existsById(user.getId().toUpperCase()));
		clientFactory.getCluster().bucket(config().bucketname()).defaultCollection().insert(user.getId().toUpperCase(),
				user);

		Optional<UserEncrypted> writeSDKReadSpring = userEncryptedRepository.findById(user.getId().toUpperCase());
		assertTrue(writeSDKReadSpring.isPresent());
		writeSDKReadSpring.get().setId(user.getId());
		writeSDKReadSpring.get().setVersion(user.getVersion());
		writeSDKReadSpring.ifPresent(u -> assertEquals(user, u));

		if (cleanAfter) {
			try {
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId().toUpperCase());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void writeSDK_readSDK() {
		boolean cleanAfter = true;
		UserEncrypted user = new UserEncrypted(UUID.randomUUID().toString(), "writeSDK_readSDK", "l", "hel\"lo");
		AddressWithEncStreet address = new AddressWithEncStreet(); // plaintext address with encrypted street
		address.setEncStreet("Olcott Street");
		address.setCity("Santa Clara");
		user.addAddress(address);
		user.setHomeAddress(null);
		Address encAddress = new Address(); // encrypted address with plaintext street.
		encAddress.setStreet("Castro St");
		encAddress.setCity("Mountain View");
		user.setEncAddress(encAddress);

		user.clazz = String.class; // not supported by SDK, but not support by UserEncrypted.toString() either.
		user.initSimpleTypes();
		// write the user with the SDK
		assertFalse(userEncryptedRepository.existsById(user.getId().toUpperCase()));
		clientFactory.getCluster().bucket(config().bucketname()).defaultCollection().insert(user.getId().toUpperCase(),
				user);

		// read the user with the SDK
		UserEncrypted writeSDKReadSDK = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.getId().toUpperCase()).contentAs(UserEncrypted.class);
		writeSDKReadSDK.setId(user.getId());
		writeSDKReadSDK.setVersion(user.getVersion());
		assertEquals(user.clazz, writeSDKReadSDK.clazz);
		writeSDKReadSDK.clazz = null; // null these out as UserEncrypted.toString() doesn't support them.
		user.clazz = null;
		assertEquals(user, writeSDKReadSDK);

		if (cleanAfter) {
			try {
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId().toUpperCase());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void testFromMigration() {
		boolean cleanAfter = true;
		UserEncrypted user = new UserEncrypted(UUID.randomUUID().toString(), "testFromMigration", "l",
				"migrating from unencrypted");
		JsonObject jo = JsonObject.jo();
		jo.put("firstname", user.getFirstname());
		jo.put("lastname", user.getLastname());
		jo.put("encryptedField", user.encryptedField);
		jo.put("_class", user._class);

		// save it unencrypted
		clientFactory.getCluster().bucket(config().bucketname()).defaultCollection().insert(user.getId(), jo);
		JsonObject migration = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.getId()).contentAsObject();
		assertEquals("migrating from unencrypted", migration.get("encryptedField"));
		assertNull(migration.get(CryptoManager.DEFAULT_ENCRYPTER_ALIAS + "encryptedField"));

		// it will be retrieved successfully
		Optional<UserEncrypted> found = userEncryptedRepository.findById(user.getId());
		assertTrue(found.isPresent());
		user.setVersion(found.get().getVersion());
		found.ifPresent(u -> assertEquals(user, u));
		// save it encrypted
		UserEncrypted saved = userEncryptedRepository.save(user);
		// it will be retrieved successfully
		Optional<UserEncrypted> foundEnc = userEncryptedRepository.findById(user.getId());
		assertTrue(foundEnc.isPresent());
		user.setVersion(foundEnc.get().getVersion());
		foundEnc.ifPresent(u -> assertEquals(user, u));

		// retrieve it without decrypting
		JsonObject encrypted = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.getId()).contentAsObject();
		assertEquals("myKey",
				((JsonObject) encrypted.get(CryptoManager.DEFAULT_ENCRYPTED_FIELD_NAME_PREFIX + "encryptedField")).get("kid"));
		assertNull(encrypted.get("encryptedField"));

		if (cleanAfter) {
			couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId());
			try { // may have also used upperCased-id
				couchbaseTemplate.removeById(UserEncrypted.class).one(user.getId().toUpperCase());
			} catch (DataRetrievalFailureException iae) {
				// ignore
			}
		}
	}

	static class Config extends org.springframework.data.couchbase.domain.Config {

		@Override
		public ObjectMapper couchbaseObjectMapper() {
			ObjectMapper om = super.couchbaseObjectMapper();
			om.registerModule(new JodaModule()); // to test joda mapping
			return om;
		}

		@Override
		protected CryptoManager cryptoManager() {
			Map<String, byte[]> keyMap = new HashMap();
			keyMap.put("myKey", new byte[64] /* all zeroes */);
			Keyring keyring = Keyring.fromMap(keyMap);
			AeadAes256CbcHmacSha512Provider provider = AeadAes256CbcHmacSha512Provider.builder().keyring(keyring)
					/*.securityProvider(secProvider)*/.build();
			return new WrappingCryptoManager(DefaultCryptoManager.builder().decrypter(provider.decrypter())
					.defaultEncrypter(provider.encrypterForKey("myKey")).build());
		}

		public class WrappingCryptoManager implements CryptoManager {
			CryptoManager cryptoManager;

			public WrappingCryptoManager(CryptoManager cryptoManager) {
				this.cryptoManager = cryptoManager;
			}

			@Override
			public Map<String, Object> encrypt(byte[] plaintext, String encrypterAlias) {
				Map<String, Object> encryptedNode = cryptoManager.encrypt(plaintext, encrypterAlias);
				return encryptedNode;
			}

			@Override
			public byte[] decrypt(Map<String, Object> encryptedNode) {
				byte[] result = cryptoManager.decrypt(encryptedNode);
				return result;
			}

		}

	}
}
