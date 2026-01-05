/*
 * Copyright 2012-present the original author or authors
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

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;

/**
 * {@link ReactiveMutateInByIdOperation} implementations for Couchbase.
 *
 * @author Tigran Babloyan
 */
public class ReactiveMutateInByIdOperationSupport implements ReactiveMutateInByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveMutateInByIdOperationSupport.class);

	public ReactiveMutateInByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveMutateInById<T> mutateInById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveMutateInByIdSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, OptionsBuilder.getPersistTo(domainType),
				OptionsBuilder.getReplicateTo(domainType), OptionsBuilder.getDurabilityLevel(domainType, template.getConverter()),
				null, template.support(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
				Collections.emptyList(), false);
	}

	static class ReactiveMutateInByIdSupport<T> implements ReactiveMutateInById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final MutateInOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final ReactiveTemplateSupport support;
		private final boolean provideCas;
		private final List<String> removePaths = new ArrayList<>();
		private final List<String> upsertPaths = new ArrayList<>();
		private final List<String> insertPaths = new ArrayList<>();
		private final List<String> replacePaths = new ArrayList<>();
		

		ReactiveMutateInByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final MutateInOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, final Duration expiry, ReactiveTemplateSupport support, final List<String> removePaths, 
				final List<String> upsertPaths, final List<String> insertPaths, final List<String> replacePaths, final boolean provideCas) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.support = support;
			this.removePaths.addAll(removePaths);	
			this.upsertPaths.addAll(upsertPaths);
			this.insertPaths.addAll(insertPaths);
			this.replacePaths.addAll(replacePaths);
			this.provideCas = provideCas;
		}

		@Override
		public Mono<T> one(T object) {
			PseudoArgs<MutateInOptions> pArgs = new PseudoArgs(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("upsertById object={} {}", object, pArgs);
			}
			
			Mono<T> reactiveEntity = TransactionalSupport.verifyNotInTransaction("mutateInById")
					.then(support.encodeEntity(object)).flatMap(converted -> {
						return Mono
								.just(template.getCouchbaseClientFactory().withScope(pArgs.getScope())
										.getCollection(pArgs.getCollection()))
								.flatMap(collection -> collection.reactive()
										.mutateIn(converted.getId().toString(), getMutations(converted), buildMutateInOptions(pArgs.getOptions(), object, converted))
										.flatMap(
												result -> support.applyResult(object, converted, converted.getId(), result.cas(), null, null)));
					});

			return reactiveEntity.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
		}

		@Override
		public Flux<? extends T> all(Collection<? extends T> objects) {
			return Flux.fromIterable(objects).flatMap(this::one);
		}

		@Override
		public TerminatingMutateInById<T> withOptions(final MutateInOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveMutateInByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> inCollection(final String collection) {
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, persistTo, replicateTo, durabilityLevel, expiry,
					support, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdInCollection<T> inScope(final String scope) {
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, persistTo, replicateTo, durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths,
					replacePaths, provideCas);
		}

		@Override
		public MutateInByIdInScope<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdInScope<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withRemovePaths(final String... removePaths) {
			Assert.notNull(removePaths, "removePaths path must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, Arrays.asList(removePaths), upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withUpsertPaths(final String... upsertPaths) {
			Assert.notNull(upsertPaths, "upsertPaths path must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, Arrays.asList(upsertPaths), insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withInsertPaths(final String... insertPaths) {
			Assert.notNull(insertPaths, "insertPaths path must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, Arrays.asList(insertPaths), replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withReplacePaths(final String... replacePaths) {
			Assert.notNull(replacePaths, "replacePaths path must not be null.");
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths, Arrays.asList(replacePaths), provideCas);
		}

		@Override
		public MutateInByIdWithPaths<T> withCasProvided() {
			return new ReactiveMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support, removePaths, upsertPaths, insertPaths, replacePaths, true);
		}

		private MutateInOptions buildMutateInOptions(MutateInOptions options, T object, CouchbaseDocument doc) {
			return OptionsBuilder.buildMutateInOptions(options, persistTo, replicateTo, durabilityLevel, expiry, doc,
					provideCas ? support.getCas(object) : null);
		}

		private List<MutateInSpec> getMutations(CouchbaseDocument document) {
			List<MutateInSpec> mutations = new ArrayList<>();
			for (String path : removePaths) {
				mutations.add(MutateInSpec.remove(path));
			}
			for (String path : upsertPaths) {
				mutations.add(MutateInSpec.upsert(path, getCouchbaseContent(document, path)).createPath());
			}
			for (String path : insertPaths) {
				mutations.add(MutateInSpec.insert(path, getCouchbaseContent(document, path)).createPath());
			}
			for (String path : replacePaths) {
				mutations.add(MutateInSpec.replace(path, getCouchbaseContent(document, path)));
			}
			return mutations;
		}

		private Object getCouchbaseContent(CouchbaseDocument document, String path) {
			Object result = document.export();
			for(var node : path.split("\\.")) {
				if(result instanceof Map map) {
					result = map.get(node);
				} else {
					throw new IllegalArgumentException("Path " + path + " is not valid.");
				}
			}
			return result;
		}
	}

}
