package org.springframework.data.couchbase.transactions;

import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.springframework.data.couchbase.util.Util.assertInAnnotationTransaction;

@Service
@Component
@EnableTransactionManagement
class PersonService {

  final CouchbaseOperations personOperations;
  final CouchbaseSimpleCallbackTransactionManager manager; // final ReactiveCouchbaseTransactionManager manager;
  final ReactiveCouchbaseOperations personOperationsRx;
  final ReactiveCouchbaseTransactionManager managerRx;

  public PersonService(CouchbaseOperations ops, CouchbaseSimpleCallbackTransactionManager mgr,
                       ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
    personOperations = ops;
    manager = mgr;
    personOperationsRx = opsRx;
    managerRx = mgrRx;
    return;
  }

  public Person savePersonErrors(Person person) {
    assertInAnnotationTransaction(false);
    TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
        new DefaultTransactionDefinition());

    return personOperationsRx.insertById(Person.class)
        .one(person)//
        .<Person>flatMap(it -> Mono.error(new SimulateFailureException()))//
        .as(transactionalOperator::transactional)
        .block();
  }

  public Person savePerson(Person person) {
    assertInAnnotationTransaction(false);
    TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
        new DefaultTransactionDefinition());
    return personOperationsRx.insertById(Person.class)
        .one(person)//
        .as(transactionalOperator::transactional)
        .block();
  }

  public Long countDuringTx(Person person) {
    assertInAnnotationTransaction(false);
    TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
        new DefaultTransactionDefinition());
    return personOperationsRx.insertById(Person.class)
        .one(person)//
        .then(personOperationsRx.findByQuery(Person.class)
            .withConsistency(REQUEST_PLUS)
            .count())
        .as(transactionalOperator::transactional)
        .block();
  }

  public List<CouchbasePersonTransactionIntegrationTests.EventLog> saveWithLogs(Person person) {
    assertInAnnotationTransaction(false);
    TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
        new DefaultTransactionDefinition());

    return Flux
        .merge(personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "beforeConvert")),
            //
            personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "afterConvert")),
            //
            personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "beforeInsert")),
            //
            personOperationsRx.insertById(Person.class)
                .one(person),
            //
            personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "afterInsert"))) //
        .thenMany(personOperationsRx.findByQuery(CouchbasePersonTransactionIntegrationTests.EventLog.class)
            .withConsistency(REQUEST_PLUS)
            .all()) //
        .as(transactionalOperator::transactional)
        .collectList()
        .block();

  }

  public List<CouchbasePersonTransactionIntegrationTests.EventLog> saveWithErrorLogs(Person person) {
    assertInAnnotationTransaction(false);
    TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
        new DefaultTransactionDefinition());

    return Flux
        .merge(personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "beforeConvert")),
            //
            personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "afterConvert")),
            //
            personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "beforeInsert")),
            //
            personOperationsRx.insertById(Person.class)
                .one(person),
            //
            personOperationsRx.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
                .one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(),
                    "afterInsert"))) //
        .thenMany(personOperationsRx.findByQuery(CouchbasePersonTransactionIntegrationTests.EventLog.class)
            .withConsistency(REQUEST_PLUS)
            .all()) //
        .<CouchbasePersonTransactionIntegrationTests.EventLog>flatMap(it -> Mono.error(new SimulateFailureException()))
        .as(transactionalOperator::transactional)
        .collectList()
        .block();

  }

  // org.springframework.beans.factory.NoUniqueBeanDefinitionException:
  // No qualifying bean of type 'org.springframework.transaction.TransactionManager' available: expected single
  // matching bean but found 2: reactiveCouchbaseTransactionManager,couchbaseTransactionManager
  @Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
  public Person declarativeSavePerson(Person person) {
    assertInAnnotationTransaction(true);
    return personOperations.insertById(Person.class)
        .one(person);
  }

  @Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
  public Person declarativeSavePersonErrors(Person person) {
    assertInAnnotationTransaction(true);
    Person p = personOperations.insertById(Person.class)
        .one(person); //
    SimulateFailureException.throwEx();
    return p;
  }

  /**
   * to execute while ThreadReplaceloop() is running should force a retry
   *
   * @param person
   * @return
   */
  @Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
  public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
    assertInAnnotationTransaction(true);
    System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
    Person p = personOperations.findById(Person.class)
        .one(person.getId()
            .toString());
    return personOperations.replaceById(Person.class)
        .one(p.withFirstName(person.getFirstname()));
  }

  /**
   * The ReactiveCouchbaseTransactionManager does not retry on write-write conflict. Instead it will throw
   * RetryTransactionException to execute while ThreadReplaceloop() is running should force a retry
   *
   * @param person
   * @return
   */
  @Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
  public Mono<Person> declarativeFindReplacePersonReactive(Person person, AtomicInteger tryCount) {
    assertInAnnotationTransaction(true);
    System.err.println("declarativeFindReplacePersonReactive try: " + tryCount.incrementAndGet());
    return personOperationsRx.findById(Person.class)
        .one(person.getId()
            .toString())
        .flatMap(p -> personOperationsRx.replaceById(Person.class)
            .one(p.withFirstName(person.getFirstname())));
  }

  /**
   * to execute while ThreadReplaceloop() is running should force a retry
   *
   * @param person
   * @return
   */
  @Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
  public Person declarativeFindReplacePerson(Person person, AtomicInteger tryCount) {
    assertInAnnotationTransaction(true);
    System.err.println("declarativeFindReplacePerson try: " + tryCount.incrementAndGet());
    Person p = personOperations.findById(Person.class)
        .one(person.getId()
            .toString());
    return personOperations.replaceById(Person.class)
        .one(p);
  }

  @Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER) // doesn't retry
  public Mono<Person> declarativeSavePersonReactive(Person person) {
    assertInAnnotationTransaction(true);
    return personOperationsRx.insertById(Person.class)
        .one(person);
  }

  @Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
  public Mono<Person> declarativeSavePersonErrorsReactive(Person person) {
    assertInAnnotationTransaction(true);
    Mono<Person> p = personOperationsRx.insertById(Person.class)
        .one(person); //
    SimulateFailureException.throwEx();
    return p;
  }

}
