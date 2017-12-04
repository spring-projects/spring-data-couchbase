package org.springframework.data.couchbase.repository.support;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;



import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;

import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.ViewQuery;
import rx.Observable;
import rx.Observable.OnSubscribe;

/**
 * JUnit test for SimpleReactiveCouchbaseRepository
 *
 * @author Rafael Moreti Santana
 * @since 3.1.0
 */
public class SimpleReactiveCouchbaseRepositoryTest {

	private static final org.springframework.data.couchbase.core.query.Consistency CONSISTENCY = Consistency.STRONGLY_CONSISTENT;

	private RxJavaCouchbaseOperations rxJavaCouchbaseOperations;
	private SimpleReactiveCouchbaseRepository<String, String> repository;

	@Before
	public void initMocks() {

		OnSubscribe<AsyncViewResult> onSubscribeResult = subscriber -> {

			OnSubscribe<AsyncViewRow> onSubscribeRow = rowSubscriber -> {
				try {
					TimeUnit.SECONDS.sleep(5); // some delay..
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				AsyncViewRow mockCountRow11 = mock(AsyncViewRow.class);
				when(mockCountRow11.value()).thenReturn("100");
				when(mockCountRow11.id()).thenReturn("id1");
				rowSubscriber.onNext(mockCountRow11);
				rowSubscriber.onCompleted();
			};

			Observable obs = Observable.create(onSubscribeRow);

			AsyncViewResult asyncViewResult = mock(AsyncViewResult.class);
			when(asyncViewResult.rows()).thenReturn(obs);

			try {
				TimeUnit.SECONDS.sleep(5); // some delay..
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			subscriber.onNext(asyncViewResult);
			subscriber.onCompleted();
		};

		Observable obs = Observable.create(onSubscribeResult);

		rxJavaCouchbaseOperations = mock(RxJavaCouchbaseOperations.class);
		when(rxJavaCouchbaseOperations.getDefaultConsistency()).thenReturn(CONSISTENCY);
		when(rxJavaCouchbaseOperations.queryView(any(ViewQuery.class))).thenReturn(obs);

		CouchbaseEntityInformation metadata = mock(CouchbaseEntityInformation.class);
		when(metadata.getJavaType()).thenReturn(String.class);

		repository = new SimpleReactiveCouchbaseRepository<String, String>(metadata, rxJavaCouchbaseOperations);
		repository.setViewMetadataProvider(mock(ViewMetadataProvider.class));
	}

	@Test
	public void shouldDeleteAll() {
		repository.deleteAll();
	}

}
