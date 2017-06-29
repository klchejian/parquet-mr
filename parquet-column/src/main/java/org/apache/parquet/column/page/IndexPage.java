/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.page;

import static org.apache.parquet.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.parquet.Ints;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;

/**
 * Data for a index page
 *
 *
 */
public class IndexPage extends Page {

  private final BytesInput bytes;
  private final int indexSize;
//  private final int valuesNum;
  private final Encoding encoding;

  /**
   * creates an uncompressed page
   * @param bytes the content of the page
   */
  public IndexPage(BytesInput bytes,int indexSize,Encoding encoding) {
    this(bytes, (int)bytes.size(),indexSize,encoding); // TODO: fix sizes long or int
  }

  /**
   * creates a index page
   * @param bytes the (possibly compressed) content of the page
   * @param uncompressedSize the size uncompressed
   */
  public IndexPage(BytesInput bytes, int uncompressedSize,int indexSize,Encoding encoding) {
    super(Ints.checkedCast(bytes.size()), uncompressedSize);
    this.bytes = checkNotNull(bytes, "bytes");
    this.indexSize = indexSize;
    this.encoding = checkNotNull(encoding, "encoding");
  }

  public BytesInput getBytes() {
    return bytes;
  }

  public int getIndexSize() {
    return indexSize;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public IndexPage copy() throws IOException {
    return new IndexPage(BytesInput.copy(bytes), getUncompressedSize(),indexSize,encoding);
  }


  @Override
  public String toString() {
    return "Page [bytes.size=" + bytes.size() + ", uncompressedSize=" + getUncompressedSize() + "]";
  }


}
