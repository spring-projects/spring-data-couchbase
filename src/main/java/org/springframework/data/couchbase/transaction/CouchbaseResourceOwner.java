package org.springframework.data.couchbase.transaction;

import reactor.core.publisher.Mono;

import java.util.Optional;

import com.couchbase.client.core.annotation.Stability.Internal;

@Internal
public class CouchbaseResourceOwner {
	private static final ThreadLocal<CouchbaseResourceHolder> marker = new ThreadLocal();

	public CouchbaseResourceOwner() {}

	public static void set(CouchbaseResourceHolder toInject) {
		if (marker.get() != null) {
			throw new IllegalStateException(
					"Trying to set resource holder when already inside a transaction - likely an internal bug, please report it");
		} else {
			marker.set(toInject);
		}
	}

	public static void clear() {
		marker.remove();
	}

	public static Mono<Optional<CouchbaseResourceHolder>> get() {
		return Mono.deferContextual((ctx) -> {
			CouchbaseResourceHolder fromThreadLocal = marker.get();
			CouchbaseResourceHolder fromReactive = ctx.hasKey(CouchbaseResourceHolder.class)
					? ctx.get(CouchbaseResourceHolder.class)
					: null;
			if (fromThreadLocal != null) {
				return Mono.just(Optional.of(fromThreadLocal));
			} else {
				return fromReactive != null ? Mono.just(Optional.of(fromReactive)) : Mono.just(Optional.empty());
			}
		});
	}
}
