/*
 * Copyright 2017-2019 the original author or authors.
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

import com.couchbase.client.java.Bucket;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.ReactiveIntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactory;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

/**
 * @author David Kelly
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = ReactiveIntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleReactiveCouchbaseRepositoryListener.class)
public class SimpleReactiveCouchbaseRepositoryDeleteAllIntegrationTests {
    // We do these tests here, rather in the simple crud tests, so we don't interact
    // with those tests that make assumptions about what documents should be
    // in the repo.
    @Rule
    public TestName testName = new TestName();

    @Autowired
    private Bucket client;

    @Autowired
    private ReactiveRepositoryOperationsMapping operationsMapping;

    @Autowired
    private IndexManager indexManager;

    private ReactiveUserRepository repository;

    private long getCount() {
        return repository.count().block().longValue();
    }

    @Before
    public void setup() throws Exception {
        ReactiveRepositoryFactorySupport factory = new ReactiveCouchbaseRepositoryFactory(operationsMapping, indexManager);
        repository = getRepositoryWithRetry(factory, ReactiveUserRepository.class);
    }

    @Test
    public void simpleDeleteAll() {
        String key = "my_unique_user_key";
        ReactiveUser instance = new ReactiveUser(key, "foobar", 22);
        repository.save(instance).block();

        // we put a doc in, lets be sure the count reflects that there is
        // at least one doc in there...
        assertTrue(getCount() > 0L);

        repository.deleteAll().block();

        try {
            getCount();
            fail("count returned documents!");
        } catch (NoSuchElementException e) {
        }
     }
}
