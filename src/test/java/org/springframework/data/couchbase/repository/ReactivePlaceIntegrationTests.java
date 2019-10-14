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

import static junit.framework.TestCase.assertNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

/**
 * @author David Kelly
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = ReactiveIntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleReactiveCouchbaseRepositoryListener.class)
public class ReactivePlaceIntegrationTests {
    @Rule
    public TestName testName = new TestName();

    @Autowired
    private Bucket client;

    @Autowired
    private ReactiveRepositoryOperationsMapping operationsMapping;

    @Autowired
    private IndexManager indexManager;

    private ReactivePlaceRepository repository;

    @Before
    public void setup() throws Exception {
        ReactiveRepositoryFactorySupport factory = new ReactiveCouchbaseRepositoryFactory(operationsMapping, indexManager);
        repository = getRepositoryWithRetry(factory, ReactivePlaceRepository.class);
    }

    @Test
    public void shouldReturnGeneratedId() {
        ReactivePlace place = new ReactivePlace("somePlace");
        assertNull(place.getId());
        ReactivePlace returned = repository.save(place).block();
        assertThat(returned.getId()).isNotNull();
    }
}
