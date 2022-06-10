package org.springframework.data.couchbase.transactions;

import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;

// @RequiredArgsConstructor
class PersonServiceReactive {

	final ReactiveCouchbaseOperations personOperationsRx;
	final ReactiveCouchbaseTransactionManager managerRx;
	final CouchbaseOperations personOperations;
	final TransactionalOperator transactionalOperator;

	public PersonServiceReactive(CouchbaseOperations ops,
			/* CouchbaseCallbackTransactionManager mgr, */ ReactiveCouchbaseOperations opsRx,
			ReactiveCouchbaseTransactionManager mgrRx) {
		personOperations = ops;
		personOperationsRx = opsRx;
		managerRx = mgrRx;
		transactionalOperator = TransactionalOperator.create(managerRx, new DefaultTransactionDefinition());
		return;
	}

	public Mono<Person> savePersonErrors(Person person) {
		return personOperationsRx.insertById(Person.class).one(person) //
				.<Person> flatMap(it -> Mono.error(new SimulateFailureException())) //
				.as(transactionalOperator::transactional);
	}

	public Mono<Person> savePerson(Person person) {
		return personOperationsRx.insertById(Person.class).one(person) //
				.flatMap(Mono::just) //
				.as(transactionalOperator::transactional);
	}

	public Mono<Long> countDuringTx(Person person) {
		return personOperationsRx.save(person) //
				.then(personOperationsRx.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count()) //
				.as(transactionalOperator::transactional);
	}

	public Flux<CouchbasePersonTransactionReactiveIntegrationTests.EventLog> saveWithLogs(Person person) {
		return Flux
				.merge(
						personOperationsRx.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(
								new ObjectId().toString(), "beforeConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "beforeInsert")),
						personOperationsRx.save(person),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterInsert"))) //
				.thenMany(personOperationsRx.findByQuery(CouchbasePersonTransactionReactiveIntegrationTests.EventLog.class)
						.withConsistency(REQUEST_PLUS).all()) //
				.as(transactionalOperator::transactional);
	}

	public Flux<Void> saveWithErrorLogs(Person person) {
		return Flux
				.merge(
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "beforeConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "beforeInsert")),
						personOperationsRx.save(person),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterInsert"))) //
				.<Void> flatMap(it -> Mono.error(new SimulateFailureException())) //
				.as(transactionalOperator::transactional);
	}

	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Flux<Person> declarativeSavePerson(Person person) {
		return transactionalOperator.execute(reactiveTransaction -> personOperationsRx.save(person));
	}

	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Flux<Person> declarativeSavePersonErrors(Person person) {
		Person p = personOperations.insertById(Person.class).one(person);
		Person pp = personOperations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all().get(0);
		SimulateFailureException.throwEx(); // so the following lines is not flagged as unreachable
		return Flux.just(p);
	}
}
