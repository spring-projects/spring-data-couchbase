/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.couchbase.core.query;

import static org.springframework.data.couchbase.core.query.Meta.MetaKey.RETRY_STRATEGY;
import static org.springframework.data.couchbase.core.query.Meta.MetaKey.SCAN_CONSISTENCY;
import static org.springframework.data.couchbase.core.query.Meta.MetaKey.TIMEOUT;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import com.couchbase.client.java.transactions.TransactionQueryOptions;
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
import com.couchbase.client.java.kv.ExistsOptions;
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
		QueryScanConsistency metaQueryScanConsistency = meta.get(SCAN_CONSISTENCY) != null
				? ((ScanConsistency) meta.get(SCAN_CONSISTENCY)).query()
				: null;
		QueryScanConsistency qsc = fromFirst(QueryScanConsistency.NOT_BOUNDED, query.getScanConsistency(), getScanConsistency(optsJson),
				scanConsistency, metaQueryScanConsistency);
		Duration timeout = fromFirst(Duration.ofSeconds(0), getTimeout(optsBuilt), meta.get(TIMEOUT));
		RetryStrategy retryStrategy = fromFirst(null, getRetryStrategy(optsBuilt), meta.get(RETRY_STRATEGY));

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

	public static TransactionQueryOptions buildTransactionQueryOptions(QueryOptions options) {
		QueryOptions.Built built = options.build();
		TransactionQueryOptions txOptions = TransactionQueryOptions.queryOptions();

		JsonObject optsJson = getQueryOpts(built);

		if (optsJson.containsKey("use_fts")) {
			throw new IllegalArgumentException("QueryOptions.flexIndex is not supported in a transaction");
		}

		// todo gpx is this compatible with all forms of named and positional parameters? won't be compatible with JsonSerializer. maybe can put some support into SDK
		for (Map.Entry<String, Object> entry : optsJson.toMap().entrySet()) {
			txOptions.raw(entry.getKey(), entry.getValue());
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("query options: {}", optsJson);
		}
		return txOptions;
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
		Duration timeout = fromFirst(Duration.ofSeconds(0), optsBuilt.timeout());
		RetryStrategy retryStrategy = fromFirst(null, optsBuilt.retryStrategy());

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
	public static <T> T fromFirst(T deflt, Object... choice) {
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
		meta.set(SCAN_CONSISTENCY, method.getScanConsistencyAnnotation());
		return meta;
	}

	/**
	 * return the first merged annotation which does not have attribute with null/defaultValue from the listed elements.
	 * 
	 * @param <A>
	 * @param annotation
	 * @param attributeName
	 * @param defaultValue
	 * @param elements
	 * @return
	 */
	public static <A extends Annotation, V> A annotation(Class<A> annotation, String attributeName, V defaultValue,
			AnnotatedElement... elements) {
		int i = 1;
		for (AnnotatedElement el : elements) {
			A an = AnnotatedElementUtils.findMergedAnnotation(el, annotation);
			if (an != null) {
				if (defaultValue != null) {
					try {
						Method m = an.getClass().getMethod(attributeName);
						V value = (V) m.invoke(an);
						if (!defaultValue.equals(value)) {
							return an;
						}
					} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
						throw new RuntimeException(e);
					}
				} else {
					return an;
				}
			}
		}
		return null;
	}

	public static <A extends Annotation, V> A annotation(Class<A> annotation, V defaultValue,
			AnnotatedElement[] elements) {
		return annotation(annotation, "value", defaultValue, elements);
	}

	/**
	 * return the first merged annotation which is not null/defaultValue from the listed elements.
	 *
	 * @param <A>
	 * @param annotation
	 * @param defaultValue
	 * @param elements
	 * @return
	 */
	public static <A extends Annotation, V> V annotationAttribute(Class<A> annotation, String attributeName,
			V defaultValue, AnnotatedElement[] elements) {
		for (AnnotatedElement el : elements) {
			A an = AnnotatedElementUtils.findMergedAnnotation(el, annotation);
			if (an != null) {
				try {
					Method m = an.getClass().getMethod(attributeName);
					V result = (V) m.invoke(an);
					if (result != null && !result.equals(defaultValue)) {
						return result;
					}
				} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return null;
	}

	/**
	 * return the toString() of the first merged annotation which is not null/defaultValue from the listed elements.
	 * 
	 * @param annotation
	 * @param defaultValue
	 * @param elements
	 * @param <A>
	 * @return
	 */
	public static <A extends Annotation> String annotationString(Class<A> annotation, String attributeName,
			Object defaultValue, AnnotatedElement[] elements) {
		A result = annotation(annotation, defaultValue, elements);
		if (result == null) {
			return null;
		}
		try {
			Method m = result.getClass().getMethod(attributeName);
			Object value = m.invoke(result);
			return value.toString();
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	public static <A extends Annotation> String annotationString(Class<A> annotation, Object defaultValue,
			AnnotatedElement[] elements) {
		return annotationString(annotation, "value", defaultValue, elements);
	}

}
