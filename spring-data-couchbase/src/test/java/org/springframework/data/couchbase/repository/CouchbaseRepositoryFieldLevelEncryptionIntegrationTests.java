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

package org.springframework.data.couchbase.repository;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.PersonValueRepository;
import org.springframework.data.couchbase.domain.UserEncrypted;
import org.springframework.data.couchbase.domain.UserEncryptedRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.encryption.AeadAes256CbcHmacSha512Provider;
import com.couchbase.client.encryption.Decrypter;
import com.couchbase.client.encryption.DefaultCryptoManager;
import com.couchbase.client.encryption.Encrypter;
import com.couchbase.client.encryption.EncryptionResult;
import com.couchbase.client.encryption.Keyring;
import com.couchbase.client.java.env.ClusterEnvironment;

/**
 * Repository KV tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@SpringJUnitConfig(CouchbaseRepositoryFieldLevelEncryptionIntegrationTests.Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryFieldLevelEncryptionIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired UserEncryptedRepository userEncryptedRepository;
	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired PersonValueRepository personValueRepository;
	@Autowired CouchbaseTemplate couchbaseTemplate;

	@BeforeEach
	public void beforeEach() {
		super.beforeEach();
		couchbaseTemplate.removeByQuery(UserEncrypted.class).withConsistency(REQUEST_PLUS).all();
		couchbaseTemplate.findByQuery(UserEncrypted.class).withConsistency(REQUEST_PLUS).all();
	}

	@Test
	void javaSDKEncryption() {

	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindById() {
		UserEncrypted user = new UserEncrypted(UUID.randomUUID().toString(), "saveAndFindById", "l", "hello");
		Address address = new Address(); // plaintext address with encrypted street
		// address.setEncStreet("Olcott Street");
		address.setStreet("Castro Street");
		address.setCity("Santa Clara");
		user.addAddress(address);
		user.setHomeAddress(null);
		// cannot set encrypted fields within encrypted objects (i.e. setEncAddress())
		Address encAddress = new Address(); // encrypted address with plaintext street.
		encAddress.setStreet("Castro St");
		encAddress.setCity("Mountain View");
		user.setEncAddress(encAddress);
		assertFalse(userEncryptedRepository.existsById(user.getId()));
		DateTime beforeDateTime = user.plainDateTime.plus(1).minus(1);
		assertEquals(user.plainDateTime, beforeDateTime);
		System.err.println("before: "+beforeDateTime);
		userEncryptedRepository.save(user);
		DateTime afterDateTime = user.plainDateTime.plus(1).minus(1);
		assertEquals(beforeDateTime, afterDateTime);
		System.err.println("afterDateTime: "+afterDateTime);
		Optional<UserEncrypted> found = userEncryptedRepository.findById(user.getId());
		assertTrue(found.isPresent());
		System.err.println("Found: "+found.get());
		found.ifPresent(u -> assertEquals(user, u));
		assertTrue(userEncryptedRepository.existsById(user.getId()));

		clientFactory.getCluster().bucket(config().bucketname()).defaultCollection().insert(user.getId().toUpperCase(), user);
		UserEncrypted sdkUser = clientFactory.getCluster().bucket(config().bucketname()).defaultCollection()
				.get(user.getId()).contentAs(UserEncrypted.class);
		System.err.println("user:   : " + user);
		sdkUser.setId(user.getId());
		sdkUser.setVersion(user.getVersion());
		//assertTrue(user.encDateTime.equals( sdkUser.encDateTime));
		System.err.println("sdkUser : " + sdkUser);
		assertEquals(user.plainDateTime, found.get().plainDateTime);
		assertEquals(user.plainDateTime, sdkUser.plainDateTime);
		assertEquals(user.encDateTime, found.get().encDateTime);
		assertEquals(user.encDateTime, sdkUser.encDateTime);
		assertEquals(user, sdkUser);
		// userEncryptedRepository.delete(user);
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Override
		public ObjectMapper couchbaseObjectMapper(CryptoManager cryptoManager){
			ObjectMapper om = super.couchbaseObjectMapper(cryptoManager);
			om.registerModule(new JodaModule());
			return om;
		}

		@Override
		protected void configureEnvironment(ClusterEnvironment.Builder builder) {
			if (config().isUsingCloud()) {
				builder.securityConfig(
						SecurityConfig.builder().trustManagerFactory(InsecureTrustManagerFactory.INSTANCE).enableTls(true));
			}
			CryptoManager cryptoManager = cryptoManager();
			builder.cryptoManager(cryptoManager).build();
		}

		@Override
		protected CryptoManager cryptoManager() {

			Decrypter decrypter = new Decrypter() {
				@Override
				public String algorithm() {
					return "myAlg";
				}

				@Override
				public byte[] decrypt(EncryptionResult encrypted) {
					return Base64.getDecoder().decode(encrypted.getString("ciphertext"));
				}
			};

			Encrypter encrypter = new Encrypter() {
				@Override
				public EncryptionResult encrypt(byte[] plaintext) {
					return EncryptionResult
							.fromMap(mapOf("alg", "myAlg", "ciphertext", Base64.getEncoder().encodeToString(plaintext)));
				}
			};
			Map<String, byte[]> keyMap = new HashMap();
			keyMap.put("myKey",
					new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, });
			Keyring keyring = Keyring.fromMap(keyMap);
			// Provider secProvider;
			AeadAes256CbcHmacSha512Provider provider = AeadAes256CbcHmacSha512Provider.builder().keyring(keyring)
					/*.securityProvider(secProvider)*/.build();
			return DefaultCryptoManager.builder().decrypter(provider.decrypter())
					.defaultEncrypter(provider.encrypterForKey("myKey")).build();
		}

		byte[] hmacMe(String cbc_secret_key, String cbc_api_message) {
			try {
				return hmac("hmacSHA256", cbc_secret_key.getBytes("utf-8"), cbc_api_message.getBytes("utf-8"));
			} catch (UnsupportedEncodingException | NoSuchAlgorithmException | InvalidKeyException ue) {
				return null;
			}
		}

		static byte[] hmac(String algorithm, byte[] key, byte[] message)
				throws NoSuchAlgorithmException, InvalidKeyException {
			Mac mac = Mac.getInstance(algorithm);
			mac.init(new SecretKeySpec(key, algorithm));
			return mac.doFinal(message);
		}

	}

}
