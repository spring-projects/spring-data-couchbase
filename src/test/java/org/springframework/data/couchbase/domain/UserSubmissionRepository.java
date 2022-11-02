/*
 * Copyright 2020-2022 the original author or authors
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

import java.util.List;

import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * UserSubmission Repository for tests
 * 
 * @author Michael Reiche
 */
@Repository
public interface UserSubmissionRepository extends CouchbaseRepository<UserSubmission, String> {

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<UserSubmission> findByUsername(String username);

	@Query("UPDATE #{#n1ql.bucket} set address=$2 where meta().id=$1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> setByIdAddress(String id, Address abc);

	@Query("UPDATE #{#n1ql.bucket} set courses=$2 where meta().id=$1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
  void setOrderedCourses(String id, Course[] courses);

	@Query("UPDATE #{#n1ql.bucket} set courses=$courses where meta().id=$id")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	void setNamedCourses(String id, Course[] courses);
}
