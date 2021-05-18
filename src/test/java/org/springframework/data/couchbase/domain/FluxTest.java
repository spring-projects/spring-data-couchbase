package org.springframework.data.couchbase.domain;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.AfterAll;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringJUnitConfig(FluxTest.Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class FluxTest extends JavaIntegrationTests {

	@BeforeAll
	public static void beforeEverything() {
		/**
		 * The couchbaseTemplate inherited from JavaIntegrationTests uses org.springframework.data.couchbase.domain.Config
		 * It has typeName = 't' (instead of _class). Don't use it.
		 */
		ApplicationContext ac = new AnnotationConfigApplicationContext(FluxTest.Config.class);
		couchbaseTemplate = (CouchbaseTemplate) ac.getBean(BeanNames.COUCHBASE_TEMPLATE);
		reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(BeanNames.REACTIVE_COUCHBASE_TEMPLATE);
		collection = couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection();
		rCollection = couchbaseTemplate.getCouchbaseClientFactory().getBucket().reactive().defaultCollection();
		for (String k : keyList) {
			couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection().upsert(k,
					JsonObject.create().put("x", k));
		}
	}

	@AfterAll
	public static void afterEverthing() {
		couchbaseTemplate.removeByQuery(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		couchbaseTemplate.findByQuery(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
	}

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
	}

	static List<String> keyList = Arrays.asList("a", "b", "c", "d", "e");
	static Collection collection;
	static ReactiveCollection rCollection;
	@Autowired ReactiveAirportRepository airportRepository; // intellij flags "Could not Autowire", but it runs ok.

	AtomicInteger rCat = new AtomicInteger(0);
	AtomicInteger rFlat = new AtomicInteger(0);

	@Test
	public void concatMapCB() throws Exception {
		System.out.println("Start concatMapCB");
		System.out.println("\n******** Using concatMap() *********");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.concatMap(item -> cbGet(item)
						/* rCollection.get(item) */.doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.decrementAndGet())));
		System.out.println(concat.sequential().collectList().block());
	}

	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	public void cbse() {
		LinkedList<LinkedList<Airport>> listOfLists = new LinkedList<>();
		Airport a = new Airport(UUID.randomUUID().toString(), "iata", "lowp");
		String last = null;
		for (int i = 0; i < 5; i++) {
			LinkedList<Airport> list = new LinkedList<>();
			for (int j = 0; j < 10; j++) {
				list.add(a.withId(UUID.randomUUID().toString()));
				last = a.getId();
			}
			listOfLists.add(list);
		}
		Flux<Object> af = Flux.fromIterable(listOfLists).concatMap(catalogToStore -> Flux.fromIterable(catalogToStore)
				.parallel(4).runOn(Schedulers.parallel()).concatMap((entity) -> airportRepository.save(entity)));
		List<Object> saved = af.collectList().block();
		System.out.println("results.size() : " + saved.size());

		String statement = "select * from `" + /*config().bucketname()*/ "_default" + "` where META().id >= '" + last + "'";
		System.out.println("statement: " + statement);
		try {
			QueryResult qr = couchbaseTemplate.getCouchbaseClientFactory().getScope().query(statement,
					QueryOptions.queryOptions().profile(QueryProfile.PHASES));
			List<RemoveResult> rr = couchbaseTemplate.removeByQuery(Airport.class)
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)).all();
			System.out.println(qr.metaData().profile().get());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		List<Airport> airports = airportRepository.findAll().collectList().block();
		assertEquals(0, airports.size(), "should have been all deleted");
	}

	@Test
	public void flatMapCB() throws Exception {
		System.out.println("Start flatMapCB");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.flatMap(item -> cbGet(item) /* rCollection.get(item) */
						.doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.decrementAndGet())));
		System.out.println(concat.sequential().collectList().block());
	}

	@Test
	public void flatMapSyncCB() throws Exception {
		System.out.println("Start flatMapSyncCB");
		System.out.println("\n******** Using flatSyncMap() *********");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.flatMap(item -> Flux.just(cbGetSync(item) /* collection.get(item) */));
		System.out.println(concat.sequential().collectList().block());
		;
	}

	@Test
	public void flatMapVsConcatMapCB2() throws Exception {
		System.out.println("Start flatMapCB2");
		System.out.println("\n******** Using flatMap() *********");
		ParallelFlux<GetResult> flat = Flux.fromIterable(keyList).parallel(1).runOn(Schedulers.parallel())
				.flatMap(item -> rCollection.get(item).doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.getAndDecrement())));
		System.out.println(flat.sequential().collectList().block());
		System.out.println("Start concatMapCB");
		System.out.println("\n******** Using concatMap() *********");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.concatMap(item -> cbGet(item).doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.getAndDecrement())));
		System.out.println(concat.sequential().collectList().block());
		;
	}

	static Random r = new Random();

	static void sleep(long sleepMs) {
		try {
			int random = Math.abs(r.nextInt() % 1000);
			Thread.sleep(sleepMs * random);
		} catch (InterruptedException e) {}
	}

	AtomicInteger cbCount = new AtomicInteger();

	Mono<GetResult> cbGet(String id) {
		// System.out.println(" =" + id);
		return rCollection.get(id);
	}

	GetResult cbGetSync(String id) {
		// System.out.println(id + " +" + rCat.incrementAndGet());
		GetResult result = collection.get(id);
		// System.out.println(id + " -" + rCat.getAndDecrement());
		return result;
	}

	static String tab(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(" ");
		return sb.toString();
	}

	@Configuration
	@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
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
