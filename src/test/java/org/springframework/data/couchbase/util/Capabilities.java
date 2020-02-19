/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.util;

/**
 * Certain capabilities used to figure out if a test can be run or not.
 */
public enum Capabilities {
  /**
   * This cluster is able to perform sync replications.
   */
  SYNC_REPLICATION,
  /**
   * This cluster is able to handle N1QL queries.
   */
  QUERY,
  /**
   * This cluster is able to handle Analytics queries.
   */
  ANALYTICS,
  /**
   * This cluster is able to handle Search queries.
   */
  SEARCH,
  /**
   * This cluster is able to give us a config without opening a bucket.
   */
  GLOBAL_CONFIG,
  /**
   * This cluster is able to assign users to groups.
   */
  USER_GROUPS,
  /**
   * The cluster has collections enabled.
   */
  COLLECTIONS,
  /**
   * The cluster has views enabled.
   */
  VIEWS
}
