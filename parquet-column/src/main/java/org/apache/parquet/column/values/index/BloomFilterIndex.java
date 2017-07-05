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
package org.apache.parquet.column.values.index;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.LittleEndianDataInputStream;
import org.apache.parquet.column.Index;
import org.apache.parquet.column.IndexTypeName;
import org.apache.parquet.column.page.IndexPage;
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.parquet.column.Encoding.INDEX;
import static org.apache.parquet.column.Encoding.PLAIN;

/**
 * a simple implementation of index for plain encoded values
 *
 */
public class BloomFilterIndex extends Index {

  private long[] longIndexContent = null;

  /**
   * @param indexPage the PLAIN encoded content of the dictionary
   * @throws IOException
   */
  public BloomFilterIndex(IndexPage indexPage) throws IOException {
    super(indexPage.getEncoding(), IndexTypeName.BLOOM_FILTER);
    if (indexPage.getEncoding() != INDEX
        && indexPage.getEncoding() != PLAIN) {
      throw new ParquetDecodingException("Index data encoding type not supported: " + indexPage.getEncoding());
    }

    final ByteBuffer indexByteBuf = indexPage.getBytes().toByteBuffer();
    longIndexContent = new long[indexPage.getIndexSize()];
    LongPlainValuesReader longReader = new LongPlainValuesReader();
    longReader.initFromPage(indexPage.getIndexSize(), indexByteBuf, 0);
    for(int i = 0; i < longIndexContent.length; i++) {
      longIndexContent[i] = longReader.readLong();
    }
  }

  @Override
  public long decodeToLong(int id) {
    return longIndexContent[id];
  }

  @Override
  public String toString() {
//    StringBuilder sb = new StringBuilder("PlainIntegerIndex {\n");
//    for (int i = 0; i < intIndexContent.length; i++) {
//      sb.append(i).append(" => ").append(intIndexContent[i]).append("\n");
//    }
//    return sb.append("}").toString();
    return null;
  }

  @Override
  public int getMaxId() {
    return longIndexContent.length - 1;
  }
}
