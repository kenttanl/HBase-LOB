/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mob.compactions;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * The counter used in sweep job.
 */
@InterfaceAudience.Private
public enum SweepCounter {

  /**
   * How many files are read. 
   */
  INPUT_FILE_COUNT,

  /**
   * How many files need to be merged or cleaned.
   */
  FILE_TO_BE_MERGE_OR_CLEAN,

  /**
   * How many files are left after merging.
   */
  FILE_AFTER_MERGE_OR_CLEAN,

  /**
   * How many records are updated.
   */
  RECORDS_UPDATED,
}
