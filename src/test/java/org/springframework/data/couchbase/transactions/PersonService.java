package org.springframework.data.couchbase.transactions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
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
import static org.springframework.data.couchbase.util.JavaIntegrationTests.couchbaseTemplate;
import static org.springframework.data.couchbase.util.JavaIntegrationTests.throwSimulateFailureException;
import static org.springframework.data.couchbase.util.Util.assertInAnnotationTransaction;

@Service
@Component
@EnableTransactionManagement
class PersonService {

  final CouchbaseOperations personOperations;
  final ReactiveCouchbaseOperations personOperationsRx;
  final ReactiveCouchbaseTransactionManager managerRx;

  public PersonService(CouchbaseOperations ops, ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
    personOperations = ops;
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
  @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
  public Person declarativeSavePerson(Person person) {
    assertInAnnotationTransaction(true);
    return personOperations.insertById(Person.class)
        .one(person);
  }

  @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
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
  @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
  public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
    assertInAnnotationTransaction(true);
    System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
    Person p = personOperations.findById(Person.class)
        .one(person.getId()
            .toString());
    ReplaceLoopThread.updateOutOfTransaction(personOperations, person, tryCount.get());
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
        .map((p) -> ReplaceLoopThread.updateOutOfTransaction(personOperations, p, tryCount.get()))
        .flatMap(p -> personOperationsRx.replaceById(Person.class)
            .one(p.withFirstName(person.getFirstname())));
  }

  /**
   *
   * @param person
   * @return
   */
  /*

  todo fails with :
  Internal transaction error
TransactionOperationFailedException {cause:com.couchbase.client.core.error.CasMismatchException, retry:true, autoRollback:true}
	at com.couchbase.client.core.error.transaction.TransactionOperationFailedException$Builder.build(TransactionOperationFailedException.java:136)
	at org.springframework.data.couchbase.core.TransactionalSupport.retryTransactionOnCasMismatch(TransactionalSupport.java:92)
	at org.springframework.data.couchbase.core.ReactiveReplaceByIdOperationSupport$ReactiveReplaceByIdSupport.lambda$one$1(ReactiveReplaceByIdOperationSupport.java:121)
...
	Suppressed: java.lang.Exception: #block terminated with an error
		at reactor.core.publisher.BlockingSingleSubscriber.blockingGet(BlockingSingleSubscriber.java:99)
		at reactor.core.publisher.Mono.block(Mono.java:1707)
		at org.springframework.data.couchbase.core.ExecutableReplaceByIdOperationSupport$ExecutableReplaceByIdSupport.one(ExecutableReplaceByIdOperationSupport.java:79)
		at org.springframework.data.couchbase.transactions.PersonService.declarativeFindReplacePerson(PersonService.java:214)
		at org.springframework.data.couchbase.transactions.PersonService$$FastClassBySpringCGLIB$$ade51a93.invoke(<generated>)
		at org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:218)
		at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:793)
		at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)
		at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.proceed(CglibAopProxy.java:763)
		at org.springframework.transaction.interceptor.TransactionInterceptor$1.proceedWithInvocation(TransactionInterceptor.java:123)
		at org.springframework.transaction.interceptor.TransactionAspectSupport.invokeWithinTransaction(TransactionAspectSupport.java:388)
		at org.springframework.transaction.interceptor.TransactionInterceptor.invoke(TransactionInterceptor.java:119)
		at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:186)
		at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.proceed(CglibAopProxy.java:763)
		at org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:708)
		at org.springframework.data.couchbase.transactions.PersonService$$EnhancerBySpringCGLIB$$3982573.declarativeFindReplacePerson(<generated>)
		at org.springframework.data.couchbase.transactions.CouchbasePersonTransactionIntegrationTests.replaceWithCasConflictResolvedViaRetryAnnotated(CouchbasePersonTransactionIntegrationTests.java:368)
		...
Caused by: com.couchbase.client.core.error.CasMismatchException: Document has been concurrently modified on the server
	at org.springframework.data.couchbase.core.TransactionalSupport.retryTransactionOnCasMismatch(TransactionalSupport.java:90)
	... 45 more

   */
  @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
  public Person declarativeFindReplacePerson(Person person, AtomicInteger tryCount) {
    assertInAnnotationTransaction(true);
    System.err.println("declarativeFindReplacePerson try: " + tryCount.incrementAndGet());
    Person p = personOperations.findById(Person.class)
        .one(person.getId()
            .toString());
    ReplaceLoopThread.updateOutOfTransaction(personOperations, person, tryCount.get());
    return personOperations.replaceById(Person.class)
        .one(p.withFirstName(person.getFirstname()));
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
