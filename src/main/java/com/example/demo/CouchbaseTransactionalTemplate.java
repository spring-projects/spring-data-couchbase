package com.example.demo;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

public class CouchbaseTransactionalTemplate {

	private final CouchbaseTemplate template;

	public CouchbaseTransactionalTemplate(CouchbaseTemplate template) {
		this.template = template;
	}

	public <T> SpringTransactionGetResult<T> findById(String id, Class<T> domainType) {
		try {
			CoreTransactionAttemptContext ctx = getContext();
			CoreTransactionGetResult getResult = ctx.get(  makeCollectionIdentifier(template.getCouchbaseClientFactory().getDefaultCollection().async()) , id).block();

			T t = template.support().decodeEntity(id, new String(getResult.contentAsBytes()), getResult.cas(), domainType,
					null, null, null);
			return new SpringTransactionGetResult<>(t, getResult);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	public <T> void replaceById(CoreTransactionGetResult getResult, T entity) {
		CoreTransactionAttemptContext ctx = getContext();
		Transcoder transCoder = template.getCouchbaseClientFactory().getCluster().environment().transcoder();
		Transcoder.EncodedValue encoded = transCoder.encode(template.support().encodeEntity(entity).export());
		ctx.replace(getResult, encoded.encoded());
	}

	private CoreTransactionAttemptContext getContext() {
		ReactiveCouchbaseResourceHolder resource = (ReactiveCouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		CoreTransactionAttemptContext atr;
		if (resource != null) {
			atr = resource.getCore();
		} else {
			ReactiveCouchbaseResourceHolder holder = (ReactiveCouchbaseResourceHolder) TransactionSynchronizationManager
					.getResource(template.getCouchbaseClientFactory().getCluster());
			atr = holder.getCore();
		}
		return atr;
	}


	public static ReactiveCouchbaseResourceHolder getSession(ReactiveCouchbaseTemplate template) {
		ReactiveCouchbaseResourceHolder resource = (ReactiveCouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		return resource;
	}

}
