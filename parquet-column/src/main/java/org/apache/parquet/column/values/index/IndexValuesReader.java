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

import org.apache.parquet.Log;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Index;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.parquet.Log.DEBUG;

/**
 * Reads values that have been dictionary encoded
 *
 */
public class IndexValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(IndexValuesReader.class);

  private ByteBufferInputStream in;

  private Index index;

  private RunLengthBitPackingHybridDecoder decoder;

  public IndexValuesReader(Index index) {
    this.index = index;
  }

  @Override
  public void initFromPage(int valueCount, ByteBuffer page, int offset)
      throws IOException {
    this.in = new ByteBufferInputStream(page, offset, page.limit() - offset);
    if (page.limit() - offset > 0) {
      if (DEBUG)
        LOG.debug("init from page at offset " + offset + " for length " + (page.limit() - offset));
      int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
      if (DEBUG) LOG.debug("bit width " + bitWidth);
      decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);
    } else {
      decoder = new RunLengthBitPackingHybridDecoder(1, in) {
        @Override
        public int readInt() throws IOException {
          throw new IOException("Attempt to read from empty page");
        }
      };
    }
  }

//  @Override
//  public int readValueDictionaryId() {
//    try {
//      return decoder.readInt();
//    } catch (IOException e) {
//      throw new ParquetDecodingException(e);
//    }
//  }

//  @Override
//  public Binary readBytes() {
//    try {
//      return index.decodeToBinary(decoder.readInt());
//    } catch (IOException e) {
//      throw new ParquetDecodingException(e);
//    }
//  }
//
//  @Override
//  public float readFloat() {
//    try {
//      return index.decodeToFloat(decoder.readInt());
//    } catch (IOException e) {
//      throw new ParquetDecodingException(e);
//    }
//  }
//
//  @Override
//  public double readDouble() {
//    try {
//      return index.decodeToDouble(decoder.readInt());
//    } catch (IOException e) {
//      throw new ParquetDecodingException(e);
//    }
//  }

  @Override
  public int readInteger() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

//  @Override
//  public long readLong() {
//    try {
//      return index.decodeToLong(decoder.readInt());
//    } catch (IOException e) {
//      throw new ParquetDecodingException(e);
//    }
//  }

  @Override
  public void skip() {
    try {
      decoder.readInt(); // Type does not matter as we are just skipping dictionary keys
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }
}
