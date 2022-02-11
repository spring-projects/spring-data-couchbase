/*
 * Copyright 2017-2022 the original author or authors.
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

package org.springframework.data.couchbase.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.domain.AbstractUser;
import org.springframework.data.couchbase.domain.AbstractUserRepository;
import org.springframework.data.couchbase.domain.OtherUser;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Abstract Repository tests
 *
 * @author Michael Reiche
 */
@SpringJUnitConfig(CouchbaseAbstractRepositoryIntegrationTests.Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class CouchbaseAbstractRepositoryIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired AbstractUserRepository abstractUserRepository;

	@Test
	void saveAndFindAbstract() {
		// User extends AbstractUser
		// OtherUser extends Abstractuser
		AbstractUser user = null;
		{
			user = new User(UUID.randomUUID().toString(), "userFirstname", "userLastname");
			assertEquals(User.class, user.getClass());
			abstractUserRepository.save(user);
			{
				// Queries on repositories for abstract entities must be @Query and not include
				// #{#n1ql.filter} (i.e. _class = <classname> ) as the classname will not match any document
				AbstractUser found = abstractUserRepository.myFindById(user.getId());
				assertEquals(user, found);
				assertEquals(user.getClass(), found.getClass());
			}
			{
				Optional<AbstractUser> found = abstractUserRepository.findById(user.getId());
				assertEquals(user, found.get());
			}
			abstractUserRepository.delete(user);
		}
		{
			user = new OtherUser(UUID.randomUUID().toString(), "userFirstname", "userLastname");
			assertEquals(OtherUser.class, user.getClass());
			abstractUserRepository.save(user);
			{
				AbstractUser found = abstractUserRepository.myFindById(user.getId());
				assertEquals(user, found);
				assertEquals(user.getClass(), found.getClass());
			}
			{
				Optional<AbstractUser> found = abstractUserRepository.findById(user.getId());
				assertEquals(user, found.get());
			}
			abstractUserRepository.delete(user);
		}

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

	}

}
