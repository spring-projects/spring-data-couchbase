/*
 * Copyright 2025-present the original author or authors
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
package org.springframework.data.couchbase.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchRow;

/**
 * Integration tests for FTS via CouchbaseTemplate.
 *
 * @author Emilien Bevierre
 *
 * @since 6.2
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
@DirtiesContext
class CouchbaseTemplateSearchIntegrationTests extends JavaIntegrationTests {

	private static final String INDEX_NAME = "sd-fts-test-index";

	@Autowired CouchbaseTemplate couchbaseTemplate;
	@Autowired ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeAll
	static void setupFtsIndex() {
		callSuperBeforeAll(new Object() {});
		Cluster cluster = Cluster.connect(connectionString(), username(), password());
		try {
			cluster.bucket(bucketName()).waitUntilReady(Duration.ofSeconds(30));
			SearchIndex searchIndex = new SearchIndex(INDEX_NAME, bucketName());
			cluster.searchIndexes().upsertIndex(searchIndex);
			waitForFtsIndex(cluster, INDEX_NAME);
		} finally {
			logCluster(cluster, "setupFtsIndex");
		}
	}

	@AfterAll
	static void tearDownFtsIndex() {
		Cluster cluster = Cluster.connect(connectionString(), username(), password());
		try {
			cluster.searchIndexes().dropIndex(INDEX_NAME);
		} catch (Exception e) {
			LOGGER.warn("Failed to drop FTS index: {}", e.getMessage());
		} finally {
			logCluster(cluster, "tearDownFtsIndex");
		}
		callSuperAfterAll(new Object() {});
	}

	/**
	 * Inserts a document via the raw SDK to get the MutationResult (with mutation tokens),
	 * then builds SearchOptions with consistentWith so FTS waits for index catch-up.
	 */
	private InsertedDoc insertRawDoc(String id, String firstname, String lastname) {
		Collection col = couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection();
		JsonObject content = JsonObject.create()
				.put("id", id)
				.put("firstname", firstname)
				.put("lastname", lastname)
				.put("t", "abstractuser"); // typeKey used by Config
		MutationResult result = col.insert(id, content);
		MutationState ms = MutationState.from(result.mutationToken().get());
		return new InsertedDoc(id, ms);
	}

	private void removeRawDoc(String id) {
		try {
			couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection().remove(id);
		} catch (Exception e) {
			LOGGER.warn("Cleanup failed for {}: {}", id, e.getMessage());
		}
	}

	private SearchOptions consistentOptions(MutationState ms) {
		return SearchOptions.searchOptions().consistentWith(ms);
	}

	@Test
	void searchAllReturnsHydratedEntities() {
		InsertedDoc doc = insertRawDoc("fts-all-" + UUID.randomUUID(), "allsearchfn", "allsearchln");
		try {
			List<User> results = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("allsearchfn")))
					.all();

			assertFalse(results.isEmpty(), "Expected at least one result");
			User found = results.stream().filter(u -> doc.id.equals(u.getId())).findFirst().orElse(null);
			assertNotNull(found, "Expected to find inserted user by id");
			assertEquals("allsearchfn", found.getFirstname());
			assertEquals("allsearchln", found.getLastname());
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void searchFirstReturnsOneEntity() {
		InsertedDoc doc = insertRawDoc("fts-first-" + UUID.randomUUID(), "firstsearchfn", "firstsearchln");
		try {
			User result = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("firstsearchfn")))
					.firstValue();

			assertNotNull(result, "Expected first() to return a result");
		} finally {
			removeRawDoc(doc.id);
		}
	}

    /// Uses ConsistentWith to avoid flakiness from eventual consistency.
	@Test
	void searchOneReturnsExactlyOne() {
		String unique = "uniquefts" + UUID.randomUUID().toString().replace("-", "");
		InsertedDoc doc = insertRawDoc("fts-one-" + UUID.randomUUID(), unique, "onesearchln");
		try {
			User result = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString(unique)))
					.oneValue();

			assertNotNull(result, "Expected exactly one result");
			assertEquals(doc.id, result.getId());
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void searchCountReturnsPositive() {
		InsertedDoc doc = insertRawDoc("fts-count-" + UUID.randomUUID(), "countsearchfn", "countsearchln");
		try {
			long count = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("countsearchfn")))
					.count();

			assertTrue(count > 0, "Expected count > 0");
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void searchExistsReturnsTrue() {
		InsertedDoc doc = insertRawDoc("fts-exists-" + UUID.randomUUID(), "existssearchfn", "existssearchln");
		try {
			boolean exists = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("existssearchfn")))
					.exists();

			assertTrue(exists, "Expected exists to be true");
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void searchRawRowsReturnsScoresAndIds() {
		InsertedDoc doc = insertRawDoc("fts-rows-" + UUID.randomUUID(), "rawrowsfn", "rawrowsln");
		try {
			List<SearchRow> rows = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("rawrowsfn")))
					.rows();

			assertFalse(rows.isEmpty(), "Expected at least one SearchRow");
			SearchRow row = rows.stream().filter(r -> doc.id.equals(r.id())).findFirst().orElse(null);
			assertNotNull(row, "Expected to find our document in raw rows");
			assertTrue(row.score() > 0, "Expected a positive score");
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void reactiveSearchAllReturnsHydratedEntities() {
		InsertedDoc doc = insertRawDoc("fts-rx-all-" + UUID.randomUUID(), "rxallsearchfn", "rxallsearchln");
		try {
			List<User> results = reactiveCouchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("rxallsearchfn")))
					.all()
					.collectList()
					.block();

			assertNotNull(results);
			assertFalse(results.isEmpty(), "Expected at least one reactive result");
			assertTrue(results.stream().anyMatch(u -> doc.id.equals(u.getId())),
					"Expected to find the inserted user");
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void reactiveSearchCountReturnsPositive() {
		InsertedDoc doc = insertRawDoc("fts-rx-count-" + UUID.randomUUID(), "rxcountsearchfn", "rxcountsearchln");
		try {
			Long count = reactiveCouchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("rxcountsearchfn")))
					.count()
					.block();

			assertNotNull(count);
			assertTrue(count > 0, "Expected reactive count > 0");
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void reactiveSearchRowsReturnsResults() {
		InsertedDoc doc = insertRawDoc("fts-rx-rows-" + UUID.randomUUID(), "rxrowssearchfn", "rxrowssearchln");
		try {
			List<SearchRow> rows = reactiveCouchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.queryString("rxrowssearchfn")))
					.rows()
					.collectList()
					.block();

			assertNotNull(rows);
			assertFalse(rows.isEmpty(), "Expected at least one reactive SearchRow");
			assertTrue(rows.stream().anyMatch(r -> doc.id.equals(r.id())),
					"Expected to find our document in reactive raw rows");
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void searchWithMatchQuery() {
		InsertedDoc doc = insertRawDoc("fts-match-" + UUID.randomUUID(), "matchqueryfn", "matchqueryln");
		try {
			List<User> results = couchbaseTemplate.findBySearch(User.class)
					.withIndex(INDEX_NAME)
					.withOptions(consistentOptions(doc.ms))
					.matching(SearchRequest.create(SearchQuery.match("matchqueryfn")))
					.all();

			assertFalse(results.isEmpty(), "MatchQuery should find the document");
			assertTrue(results.stream().anyMatch(u -> doc.id.equals(u.getId())));
		} finally {
			removeRawDoc(doc.id);
		}
	}

	@Test
	void searchNoResults() {
		// Search for a term that definitely doesn't exist
		String nonsense = "zzzznonexistent" + UUID.randomUUID().toString().replace("-", "");
		List<User> results = couchbaseTemplate.findBySearch(User.class)
				.withIndex(INDEX_NAME)
				.matching(SearchRequest.create(SearchQuery.queryString(nonsense)))
				.all();

		assertTrue(results.isEmpty(), "Expected no results for nonsense query");
	}

	@Test
	void searchNoResultsCountIsZero() {
		String nonsense = "zzzznonexistent" + UUID.randomUUID().toString().replace("-", "");
		long count = couchbaseTemplate.findBySearch(User.class)
				.withIndex(INDEX_NAME)
				.matching(SearchRequest.create(SearchQuery.queryString(nonsense)))
				.count();

		assertEquals(0, count, "Expected count == 0 for nonsense query");
	}

	@Test
	void searchNoResultsExistsIsFalse() {
		String nonsense = "zzzznonexistent" + UUID.randomUUID().toString().replace("-", "");
		boolean exists = couchbaseTemplate.findBySearch(User.class)
				.withIndex(INDEX_NAME)
				.matching(SearchRequest.create(SearchQuery.queryString(nonsense)))
				.exists();

		assertFalse(exists, "Expected exists == false for nonsense query");
	}

	private static void waitForFtsIndex(Cluster cluster, String indexName) {
		int maxRetries = 30;
		for (int i = 0; i < maxRetries; i++) {
			try {
				cluster.searchQuery(indexName, SearchQuery.queryString("*"));
				return;
			} catch (Exception ex) {
				if (i < maxRetries - 1 && (ex.getMessage().contains("no planPIndexes")
						|| ex.getMessage().contains("pindex_consistency")
						|| ex.getMessage().contains("pindex not available")
						|| ex.getMessage().contains("index not found"))) {
					sleepMs(2000);
					continue;
				}
				if (i >= maxRetries - 1) {
					throw new RuntimeException("FTS index " + indexName + " did not become ready in time", ex);
				}
			}
		}
	}

	private record InsertedDoc(String id, MutationState ms) {}
}
