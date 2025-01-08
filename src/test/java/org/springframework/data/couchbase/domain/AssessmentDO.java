/*
 * Copyright 2012-2025 the original author or authors
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

package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;
import org.springframework.data.couchbase.core.mapping.id.IdAttribute;

/**
 * @author Michael Reiche
 */
@Document()
public class AssessmentDO {
	@Id @GeneratedValue(strategy = GenerationStrategy.USE_ATTRIBUTES) private String documentId;

	@Field @IdAttribute private long eventTimestamp;

	@Field("docType") private String documentType;

	@Field private String id;

    public String getId() {
        return id;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof AssessmentDO)) {
            return false;
        }
        AssessmentDO that = (AssessmentDO) other;
        return equals(this.id, that.id) && equals(this.documentId, that.documentId)
                && equals(this.eventTimestamp, that.eventTimestamp) && equals(this.documentType, that.documentType);
    }

    boolean equals(Object s0, Object s1) {
        if (s0 == null && s1 == null || s0 == s1) {
            return true;
        }
        Object sa = s0 != null ? s0 : s1;
        Object sb = s0 != null ? s1 : s0;
        return sa.equals(sb);
    }
}
