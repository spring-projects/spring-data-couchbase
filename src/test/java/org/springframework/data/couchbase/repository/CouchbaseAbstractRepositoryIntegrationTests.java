/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.domain.AbstractUser;
import org.springframework.data.couchbase.domain.AbstractUserRepository;
import org.springframework.data.couchbase.domain.AbstractingMappingCouchbaseConverter;
import org.springframework.data.couchbase.domain.OtherUser;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.env.ClusterEnvironment;

/**
 * Abstract Repository tests
 *
 * @author Michael Reiche
 */
@SpringJUnitConfig(CouchbaseAbstractRepositoryIntegrationTests.Config.class)
@DirtiesContext
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class CouchbaseAbstractRepositoryIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired AbstractUserRepository abstractUserRepository;

	@Test
	void saveAndFindAbstract() {
		// User extends AbstractUser
		// OtherUser extends Abstractuser

		{
			User concreteUser = null;
			{
				concreteUser = new User(UUID.randomUUID().toString(), "userFirstname", "userLastname");
				assertEquals(User.class, concreteUser.getClass());
				concreteUser = abstractUserRepository.save(concreteUser); // this will now have version set
				// Queries on repositories for abstract entities must be @Query and not include
				// #{#n1ql.filter} (i.e. _class = <classname> ) as the classname will not match any document
				AbstractUser found = abstractUserRepository.myFindById(concreteUser.getId());
				assertEquals(concreteUser, found);
				assertEquals(concreteUser.getClass(), found.getClass());
			}
			{
				Optional<AbstractUser> found = abstractUserRepository.findById(concreteUser.getId());
				assertEquals(concreteUser, found.get());
			}
			{
				List<AbstractUser> found = abstractUserRepository.findByFirstname(concreteUser.getFirstname());
				assertEquals(1, found.size(), "should have found one user");
				assertEquals(concreteUser, found.get(0));
			}
			abstractUserRepository.delete(concreteUser);
		}
		{
			AbstractUser abstractUser = new OtherUser(UUID.randomUUID().toString(), "userFirstname", "userLastname");
			assertEquals(OtherUser.class, abstractUser.getClass());
			abstractUserRepository.save(abstractUser);
			{
				// not going to find this one as using the type _class = AbstractUser ???
				AbstractUser found = abstractUserRepository.myFindById(abstractUser.getId());
				assertEquals(abstractUser, found);
				assertEquals(abstractUser.getClass(), found.getClass());
			}
			{
				Optional<AbstractUser> found = abstractUserRepository.findById(abstractUser.getId());
				assertEquals(abstractUser, found.get());
			}
			{
				List<AbstractUser> found = abstractUserRepository.findByFirstname(abstractUser.getFirstname());
				assertEquals(1, found.size(), "should have found one user");
				assertEquals(abstractUser, found.get(0));
			}
			abstractUserRepository.delete(abstractUser);
		}

	}

	static class Config extends org.springframework.data.couchbase.domain.Config {
		/**
		 * This uses a CustomMappingCouchbaseConverter instead of MappingCouchbaseConverter, which in turn uses
		 * AbstractTypeMapper which has special mapping for AbstractUser
		 */
		@Override
		@Bean(name = "mappingCouchbaseConverter")
		public MappingCouchbaseConverter mappingCouchbaseConverter(CouchbaseMappingContext couchbaseMappingContext,
				CouchbaseCustomConversions couchbaseCustomConversions /* there is a customConversions() method bean  */) {
			// MappingCouchbaseConverter relies on a SimpleInformationMapper
			// that has an getAliasFor(info) that just returns getType().getName().
			// Our CustomMappingCouchbaseConverter uses a TypeBasedCouchbaseTypeMapper that will
			// use the DocumentType annotation
			MappingCouchbaseConverter converter = new AbstractingMappingCouchbaseConverter(couchbaseMappingContext,
					typeKey(),
					couchbaseCustomConversions);
			return converter;
		}

	}

}
