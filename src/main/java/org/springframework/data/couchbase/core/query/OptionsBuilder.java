package org.springframework.data.couchbase.core.query;

import static org.springframework.data.couchbase.core.query.Meta.MetaKey.QUERY;
import static org.springframework.data.couchbase.core.query.Meta.MetaKey.RETRY_STRATEGY;
import static org.springframework.data.couchbase.core.query.Meta.MetaKey.TIMEOUT;

import java.time.Duration;
import java.util.Optional;

import com.couchbase.client.java.kv.ExistsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

public class OptionsBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(OptionsBuilder.class);

	static QueryOptions buildQueryOptions(Query query, QueryOptions options, QueryScanConsistency scanConsistency) {
		options = options != null ? options : QueryOptions.queryOptions();
		if (query.getParameters() != null) {
			if (query.getParameters() instanceof JsonArray) {
				options.parameters((JsonArray) query.getParameters());
			} else {
				options.parameters((JsonObject) query.getParameters());
			}
		}

		Meta meta = query.getMeta() != null ? query.getMeta() : new Meta();
		QueryOptions.Built optsBuilt = options.build();
		JsonObject optsJson = getQueryOpts(optsBuilt);
		QueryScanConsistency qsc = mostSpecific(QueryScanConsistency.NOT_BOUNDED, getScanConsistency(optsJson),
				scanConsistency, meta.get(QUERY));
		Duration timeout = mostSpecific(Duration.ofSeconds(0), getTimeout(optsBuilt), meta.get(TIMEOUT));
		RetryStrategy retryStrategy = mostSpecific(null, getRetryStrategy(optsBuilt), meta.get(RETRY_STRATEGY));
		// JsonObject mutationState = mostSpecific(null, getScanVectors(optsJson));

		if (qsc != null) {
			options.scanConsistency(qsc);
		}
		if (timeout != null) {
			options.timeout(timeout);
		}
		if (retryStrategy != null) {
			options.retryStrategy(retryStrategy);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("query options: {}", getQueryOpts(options.build()));
		}
		return options;
	}

	public static ExistsOptions buildExistsOptions(ExistsOptions options) {
		options = options != null ? options : ExistsOptions.existsOptions();
		return options;
	}

	public static InsertOptions buildInsertOptions(InsertOptions options, PersistTo persistTo, ReplicateTo replicateTo,
			DurabilityLevel durabilityLevel, Duration expiry, CouchbaseDocument doc) {
		options = options != null ? options : InsertOptions.insertOptions();
		if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
			options.durability(persistTo, replicateTo);
		} else if (durabilityLevel != DurabilityLevel.NONE) {
			options.durability(durabilityLevel);
		}
		if (expiry != null) {
			options.expiry(expiry);
		} else if (doc.getExpiration() != 0) {
			options.expiry(Duration.ofSeconds(doc.getExpiration()));
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("insert options: {}" + toString(options));
		}
		return options;
	}

	public static UpsertOptions buildUpsertOptions(UpsertOptions options, PersistTo persistTo, ReplicateTo replicateTo,
			DurabilityLevel durabilityLevel, Duration expiry, CouchbaseDocument doc) {
		options = options != null ? options : UpsertOptions.upsertOptions();
		if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
			options.durability(persistTo, replicateTo);
		} else if (durabilityLevel != DurabilityLevel.NONE) {
			options.durability(durabilityLevel);
		}
		if (expiry != null) {
			options.expiry(expiry);
		} else if (doc.getExpiration() != 0) {
			options.expiry(Duration.ofSeconds(doc.getExpiration()));
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("upsert options: {}" + toString(options));
		}
		return options;
	}

	public static ReplaceOptions buildReplaceOptions(ReplaceOptions options, PersistTo persistTo, ReplicateTo replicateTo,
			DurabilityLevel durabilityLevel, Duration expiry, Long cas, CouchbaseDocument doc) {
		options = options != null ? options : ReplaceOptions.replaceOptions();
		if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
			options.durability(persistTo, replicateTo);
		} else if (durabilityLevel != DurabilityLevel.NONE) {
			options.durability(durabilityLevel);
		}
		if (expiry != null) {
			options.expiry(expiry);
		} else if (doc.getExpiration() != 0) {
			options.expiry(Duration.ofSeconds(doc.getExpiration()));
		}
		if (cas != null) {
			options.cas(cas);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("replace options: {}" + toString(options));
		}
		return options;
	}

	public static RemoveOptions buildRemoveOptions(RemoveOptions options, PersistTo persistTo, ReplicateTo replicateTo,
			DurabilityLevel durabilityLevel, Long cas) {
		options = options != null ? options : RemoveOptions.removeOptions();
		if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
			options.durability(persistTo, replicateTo);
		} else if (durabilityLevel != DurabilityLevel.NONE) {
			options.durability(durabilityLevel);
		}
		RemoveOptions.Built optsBuilt = options.build();
		Duration timeout = mostSpecific(Duration.ofSeconds(0), optsBuilt.timeout());
		RetryStrategy retryStrategy = mostSpecific(null, optsBuilt.retryStrategy());

		if (timeout != null) {
			options.timeout(timeout);
		}
		if (retryStrategy != null) {
			options.retryStrategy(retryStrategy);
		}
		if (cas != null) {
			options.cas(cas);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("remove options: {}" + toString(options));
		}
		return options;
	}

	/**
	 * scope annotation could be a
	 * 
	 * @param domainType
	 * @return
	 */
	public static String getScopeFrom(Class<?> domainType) {
		if (domainType == null) {
			return null;
		}
		Scope ann = AnnotatedElementUtils.findMergedAnnotation(domainType, Scope.class);
		if (ann != null && !CollectionIdentifier.DEFAULT_COLLECTION.equals(ann.value())) {
			return ann.value();
		}
		return null;
	}

	public static String getCollectionFrom(Class<?> domainType) {
		if (domainType == null) {
			return null;
		}
		Collection ann = AnnotatedElementUtils.findMergedAnnotation(domainType, Collection.class);
		if (ann != null && !CollectionIdentifier.DEFAULT_COLLECTION.equals(ann.value())) {
			return ann.value();
		}
		return null;
	}

	static String toString(InsertOptions o) {
		StringBuilder s = new StringBuilder();
		InsertOptions.Built b = o.build();
		s.append("{");
		s.append("durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	static String toString(UpsertOptions o) {
		StringBuilder s = new StringBuilder();
		UpsertOptions.Built b = o.build();
		s.append("{");
		s.append("durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	static String toString(ReplaceOptions o) {
		StringBuilder s = new StringBuilder();
		ReplaceOptions.Built b = o.build();
		s.append("{");
		s.append("cas: " + b.cas());
		s.append(", durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	static String toString(RemoveOptions o) {
		StringBuilder s = new StringBuilder();
		RemoveOptions.Built b = o.build();
		s.append("{");
		s.append("cas: " + b.cas());
		s.append(", durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	private static JsonObject getQueryOpts(QueryOptions.Built optsBuilt) {
		JsonObject jo = JsonObject.create();
		optsBuilt.injectParams(jo);
		return jo;
	}

	/**
	 * Get the most-specific
	 *
	 * @param deflt the default value, which we treat as not set
	 * @param choice array of values or Optional&lt;values&gt;, ordered from most to least specific
	 * @param <T>
	 * @return the most specific choice
	 */
	public static <T> T mostSpecific(T deflt, Object... choice) {
		T chosen = choice[0] instanceof Optional ? ((Optional<T>) choice[0]).orElse(null) : (T) choice[0];
		for (int i = 1; i < choice.length; i++) {
			if (chosen == null || chosen.equals(deflt)) { // overwrite null or default...
				if (choice[i] != null) { // ... with non-null
					chosen = choice[i] instanceof Optional ? ((Optional<T>) choice[i]).orElse(null) : (T) choice[i];
				}
			}
		}
		return chosen;
	}

	private static QueryScanConsistency getScanConsistency(JsonObject opts) {
		String str = opts.getString("scan_consistency");
		if ("at_plus".equals(str)) {
			return null;
		}
		return str == null ? null : QueryScanConsistency.valueOf(str.toUpperCase());
	}

	private static JsonObject getScanVectors(JsonObject opts) {
		return opts.getObject("scan_vectors");
	}

	private static Duration getTimeout(QueryOptions.Built optsBuilt) {
		Optional<Duration> timeout = optsBuilt.timeout();
		return timeout.isPresent() ? timeout.get() : null;
	}

	private static RetryStrategy getRetryStrategy(QueryOptions.Built optsBuilt) {
		Optional<RetryStrategy> retryStrategy = optsBuilt.retryStrategy();
		return retryStrategy.isPresent() ? retryStrategy.get() : null;
	}

	public static Meta buildMeta(CouchbaseQueryMethod method, Class<?> typeToRead) {
		Meta meta = new Meta();
		// Scope and Collection annotations are handled in PseudArgs
		// this would include a ScanConsistency in a composed annotation as well.
		for (ScanConsistency ann : new ScanConsistency[] { method.getAnnotation(ScanConsistency.class),
				method.getClassAnnotation(ScanConsistency.class) }) {
			if (ann != null && ann.query() != null) {
				meta.set(QUERY, ann.query());
				break;
			}
		}

		// Expiry is never used in a derived (Query) method
		/*
		for (Expiry ann : new Expiry[] {  method.getEntityAnnotation(Expiry.class), method.getAnnotation(Expiry.class),
				method.getClassAnnotation(Expiry.class), }) {
			if (ann != null && ann.expiry() != 0) {
				int expiry = BasicCouchbasePersistentEntity.getExpiry(ann, null);
				meta.set(EXPIRY, Duration.ofSeconds(expiry));
				break;
			}
		}
		*/

		return meta;
	}

}
