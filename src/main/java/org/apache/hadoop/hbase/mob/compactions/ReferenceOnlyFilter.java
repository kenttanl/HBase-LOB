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

import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.generated.ReferenceOnlyFilterProtos;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter that returns the cells which have mob reference tags.
 */
@InterfaceAudience.Public
public class ReferenceOnlyFilter extends FilterBase {

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    if (null != cell) {
      // If a cell with a mob reference tag, it's included.
      if (MobUtils.isMobReferenceCell(cell)) {
        return ReturnCode.INCLUDE;
      }
    }
    return ReturnCode.SKIP;
  }

  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 0, "Expected 0 but got: %s",
        filterArguments.size());
    return new ReferenceOnlyFilter();
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    ReferenceOnlyFilterProtos.ReferenceOnlyFilter.Builder builder =
        ReferenceOnlyFilterProtos.ReferenceOnlyFilter.newBuilder();
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes
   *          A pb serialized {@link ReferenceOnlyFilter} instance
   * @return An instance of {@link ReferenceOnlyFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static ReferenceOnlyFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    // There is nothing to deserialize. Why do this at all?
    try {
      ReferenceOnlyFilterProtos.ReferenceOnlyFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    // Just return a new instance.
    return new ReferenceOnlyFilter();
  }
}
