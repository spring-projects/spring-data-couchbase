package com.example.demo;

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class CouchbaseTransactionalTemplate {

	private final CouchbaseTemplate template;

	public CouchbaseTransactionalTemplate(CouchbaseTemplate template) {
		this.template = template;
	}

	public <T> SpringTransactionGetResult<T> findById(String id, Class<T> domainType) {
		try {
			TransactionAttemptContext ctx = getContext();
			TransactionGetResult getResult = ctx.get(template.getCouchbaseClientFactory().getDefaultCollection(), id);

			// todo gp getResult.cas() is no longer exposed - required?
			T t = template.support().decodeEntity(id, getResult.contentAsObject().toString(), 0, domainType,
					null, null, null);
			return new SpringTransactionGetResult<>(t, getResult);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	public <T> void replaceById(TransactionGetResult getResult, T entity) {
		TransactionAttemptContext ctx = getContext();

		ctx.replace(getResult, template.support().encodeEntity(entity).getContent());
	}

	private TransactionAttemptContext getContext() {
		CouchbaseTransactionManager.CouchbaseResourceHolder resource = (CouchbaseTransactionManager.CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		TransactionAttemptContext atr;
		if (resource != null) {
			atr = resource.getAttemptContext();
		} else {
			CouchbaseResourceHolder holder = (CouchbaseResourceHolder) TransactionSynchronizationManager
					.getResource(template.getCouchbaseClientFactory().getCluster());
			atr = holder.getSession().getTransactionAttemptContext();
		}
		return atr;
	}

	public static ReactiveTransactionAttemptContext getContextReactive(ReactiveCouchbaseTemplate template) {
		CouchbaseTransactionManager.CouchbaseResourceHolder resource = (CouchbaseTransactionManager.CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		ReactiveTransactionAttemptContext atr = null;
		if (resource != null) {
			atr = resource.getAttemptContextReactive();
		} else {
			CouchbaseResourceHolder holder = (CouchbaseResourceHolder) TransactionSynchronizationManager
					.getResource(template.getCouchbaseClientFactory().getCluster());
			if (holder != null && holder.getSession() != null) {
				atr = holder.getSession().getReactiveTransactionAttemptContext();
			}
		}
		return atr;
	}

	public static ClientSession getSession(ReactiveCouchbaseTemplate template) {
		CouchbaseTransactionManager.CouchbaseResourceHolder resource = (CouchbaseTransactionManager.CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		return resource != null ? resource.getSession() : null;
	}

}
