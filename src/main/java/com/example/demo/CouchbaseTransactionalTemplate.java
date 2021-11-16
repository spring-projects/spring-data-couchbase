package com.example.demo;

import com.couchbase.transactions.AttemptContextReactive;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.TransactionGetResult;

public class CouchbaseTransactionalTemplate {

	private final CouchbaseTemplate template;

	public CouchbaseTransactionalTemplate(CouchbaseTemplate template) {
		this.template = template;
	}

	public <T> SpringTransactionGetResult<T> findById(String id, Class<T> domainType) {
		try {
			AttemptContext ctx = getContext();
			TransactionGetResult getResult = ctx.get(template.getCouchbaseClientFactory().getDefaultCollection(), id);

			T t = template.support().decodeEntity(id, getResult.contentAsObject().toString(), getResult.cas(), domainType,
					null, null, null);
			return new SpringTransactionGetResult<>(t, getResult);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	public <T> void replaceById(TransactionGetResult getResult, T entity) {
		AttemptContext ctx = getContext();

		ctx.replace(getResult, template.support().encodeEntity(entity).getContent());
	}

	private AttemptContext getContext() {
		CouchbaseTransactionManager.CouchbaseResourceHolder resource = (CouchbaseTransactionManager.CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		AttemptContext atr;
		if (resource != null) {
			atr = resource.getAttemptContext();
		} else {
			CouchbaseResourceHolder holder = (CouchbaseResourceHolder) TransactionSynchronizationManager
					.getResource(template.getCouchbaseClientFactory().getCluster());
			atr = holder.getSession().getAttemptContext();
		}
		return atr;
	}

	public static AttemptContextReactive getContextReactive(ReactiveCouchbaseTemplate template) {
		CouchbaseTransactionManager.CouchbaseResourceHolder resource = (CouchbaseTransactionManager.CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		AttemptContextReactive atr = null;
		if (resource != null) {
			atr = resource.getAttemptContextReactive();
		} else {
			CouchbaseResourceHolder holder = (CouchbaseResourceHolder) TransactionSynchronizationManager
					.getResource(template.getCouchbaseClientFactory().getCluster());
			if (holder != null && holder.getSession() != null) {
				atr = holder.getSession().getAttemptContextReactive();
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
