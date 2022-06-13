/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.couchbase.transactions;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.couchbase.util.Util.assertInAnnotationTransaction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.data.domain.Persistable;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.test.context.transaction.TestTransaction;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Michael Reiche
 */


@ExtendWith({ SpringExtension.class })
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
@SpringJUnitConfig(TransactionsConfig.class)
public class CouchbaseTemplateTransactionIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseTemplate template;

	List<AfterTransactionAssertion<? extends Persistable<?>>> assertionList;

	@BeforeEach
	public void setUp() {
		TransactionTestUtil.assertNotInTransaction();
		assertionList = new CopyOnWriteArrayList<>();
	}

	@BeforeTransaction
	public void beforeTransaction() {
		template.removeByQuery(Assassin.class).withConsistency(REQUEST_PLUS).all();
		Collection<Assassin> a = template.findByQuery(Assassin.class).withConsistency(REQUEST_PLUS).all();
	}

	@AfterTransaction
	public void verifyDbState() {
		Collection<Assassin> p = template.findByQuery(Assassin.class).withConsistency(REQUEST_PLUS).all();
		System.out.println("assassins: " + p);
		assertionList.forEach(it -> {

			boolean isPresent = template.findById(Assassin.class).one(it.getId().toString()) != null; // (Filters.eq("_id",
			// it.getId())) != 0;

			assertThat(isPresent).isEqualTo(it.shouldBePresent())
					.withFailMessage(String.format("After transaction entity %s should %s.", it.getPersistable(),
							it.shouldBePresent() ? "be present" : "NOT be present"));
		});
	}

	@Test
	@Rollback(false)
	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	@Disabled
	/*
	java.lang.IllegalStateException: Direct programmatic use of the Couchbase PlatformTransactionManager is not supported
	at org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager.getTransaction(CouchbaseSimpleCallbackTransactionManager.java:271)
	at org.springframework.test.context.transaction.TransactionContext.startTransaction(TransactionContext.java:103)
	at org.springframework.test.context.transaction.TransactionalTestExecutionListener.beforeTestMethod(TransactionalTestExecutionListener.java:231)
	at org.springframework.test.context.TestContextManager.beforeTestMethod(TestContextManager.java:293)
	 */
	public void shouldOperateCommitCorrectly() {
		assert(TestTransaction.isActive());
		Assassin hu = new Assassin("hu", "Hu Gibbet");
		template.insertById(Assassin.class).one(hu);
		assertAfterTransaction(hu).isPresent();
	}

	@Test
	@Disabled // JCBC-1951 - run it twice second time fails.  Recreate bucket, it succeeds
	@Rollback(true)
	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public void shouldOperateRollbackCorrectly() {
		assert(TestTransaction.isActive());
		Assassin vi = new Assassin("vi", "Viridiana Sovari");
		try {
			template.removeById(Assassin.class).one(vi.getId()); // could be something that is not an Assassin
		} catch (DataRetrievalFailureException dnfe) {}
		template.insertById(Assassin.class).one(vi);
		assertAfterTransaction(vi).isNotPresent();
	}

	@Test
	@Transactional(BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	@Disabled // JCBC-1951 - run it twice, second time fails. Recreate bucket, it succeeds
	@Rollback(true)
	public void shouldBeAbleToViewChangesDuringTransaction(){
		assert(TestTransaction.isActive());
		Assassin durzo = new Assassin("durzo", "Durzo Blint");
		template.insertById(Assassin.class).one(durzo);
		Assassin retrieved = template.findById(Assassin.class).one(durzo.getId());
		assertThat(retrieved).isEqualTo(durzo);
		assertAfterTransaction(durzo).isNotPresent();
	}

	// --- Just some helpers and tests entities

	private AfterTransactionAssertion assertAfterTransaction(Assassin assassin) {
		assertInAnnotationTransaction(false);
		AfterTransactionAssertion<Assassin> assertion = new AfterTransactionAssertion<>(assassin);
		assertionList.add(assertion);
		return assertion;
	}

	@Data
	@AllArgsConstructor
	@Document
	static class Assassin implements Persistable<String> {

		@Id String id;
		String name;

		@Override
		public boolean isNew() {
			return id == null;
		}
	}
}
