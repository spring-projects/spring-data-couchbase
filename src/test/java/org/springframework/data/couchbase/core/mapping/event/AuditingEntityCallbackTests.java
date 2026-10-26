/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.couchbase.core.mapping.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.domain.AuditedRecord;

/**
 * @author Artur Kalimullin
 */
class AuditingEntityCallbackTests {

	@Test
	void returnsAuditedImmutableEntity() {
		Instant now = Instant.parse("2026-08-12T09:00:00Z");
		IsNewAwareAuditingHandler auditingHandler = IsNewAwareAuditingHandler.from(new CouchbaseMappingContext());
		auditingHandler.setDateTimeProvider(() -> Optional.of(now));

		AuditedRecord original = new AuditedRecord("id", 0, null, null, "value");
		AuditingEntityCallback callback = new AuditingEntityCallback(() -> auditingHandler);
		AuditedRecord audited = (AuditedRecord) callback.onBeforeConvert(original, "collection");

		assertNotSame(original, audited);
		assertEquals(now, audited.createdDate());
		assertEquals(now, audited.lastModifiedDate());
	}
}
