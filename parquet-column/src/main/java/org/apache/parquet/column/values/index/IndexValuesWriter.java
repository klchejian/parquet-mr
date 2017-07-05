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
import org.apache.parquet.bytes.*;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.IndexPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.IntList;
import org.apache.parquet.column.values.dictionary.IntList.IntIterator;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.util.*;

import static org.apache.parquet.bytes.BytesInput.concat;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 *  if the dictionary gets too big
 *
 */
public abstract class IndexValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(IndexValuesWriter.class);

//  /* max entries allowed for the dictionary will fail over to plain encoding if reached */
//  private static final int MAX_DICTIONARY_ENTRIES = Integer.MAX_VALUE - 1;
//  private static final int MIN_INITIAL_SLAB_SIZE = 64;

  /* encoding to label the data page */
  private final Encoding encodingForDataPage;

  /* encoding to label the dictionary page */
  protected final Encoding encodingForIndexPage;

  protected final int maxIndexByteSize;

   /* current size in bytes the index will take once serialized */
  protected int indexByteSize;

  protected int lastUsedIndexByteSize;

  protected int lastUsedIndexSize;


  /* index encoded values */
  protected Set<Binary> values = new HashSet<>();
//  protected List<Binary> encodedValues = new ArrayList<Binary>();

  /** indicates if this is the first page being processed */
  protected boolean firstPage = true;

  protected ByteBufferAllocator allocator;


  private List<RunLengthBitPackingHybridEncoder> encoders = new ArrayList<RunLengthBitPackingHybridEncoder>();

  /**
   * @param maxIndexByteSize
   */
  protected IndexValuesWriter(int maxIndexByteSize, Encoding encodingForDataPage, Encoding encodingForIndexPage, ByteBufferAllocator allocator) {
    this.maxIndexByteSize = maxIndexByteSize;
    this.allocator = allocator;
    this.encodingForDataPage = encodingForDataPage;
    this.encodingForIndexPage = encodingForIndexPage;
  }

  protected IndexPage indexPage(ValuesWriter indexPageWriter, int indexSize) {
    IndexPage ret = new IndexPage(indexPageWriter.getBytes(),indexSize,encodingForIndexPage);
    indexPageWriter.close();
    return ret;
  }


//  abstract protected void fallBackDictionaryEncodedData(ValuesWriter writer);

//  @Override
//  public long getBufferedSize() {
//    return encodedValues.size() * 4;
//  }
//
//  @Override
//  public long getAllocatedSize() {
//    // size used in memory
//    return encodedValues.size() * 4 + indexByteSize;
//  }

//  @Override
//  public BytesInput getBytes() {
//    int maxDicId = getDictionarySize() - 1;
//    if (DEBUG) LOG.debug("max dic id " + maxDicId);
//    int bitWidth = BytesUtils.getWidthFromMaxInt(maxDicId);
//    int initialSlabSize =
//        CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_INITIAL_SLAB_SIZE, maxDictionaryByteSize, 10);
//
//    RunLengthBitPackingHybridEncoder encoder =
//        new RunLengthBitPackingHybridEncoder(bitWidth, initialSlabSize, maxDictionaryByteSize, this.allocator);
//    encoders.add(encoder);
//    IntIterator iterator = encodedValues.iterator();
//    try {
//      while (iterator.hasNext()) {
//        encoder.writeInt(iterator.next());
//      }
//      // encodes the bit width
//      byte[] bytesHeader = new byte[] { (byte) bitWidth };
//      BytesInput rleEncodedBytes = encoder.toBytes();
//      if (DEBUG) LOG.debug("rle encoded bytes " + rleEncodedBytes.size());
//      BytesInput bytes = concat(BytesInput.from(bytesHeader), rleEncodedBytes);
//      // remember size of dictionary when we last wrote a page
////      lastUsedDictionarySize = getDictionarySize();
////      lastUsedDictionaryByteSize = dictionaryByteSize;
//      return bytes;
//    } catch (IOException e) {
//      throw new ParquetEncodingException("could not encode the values", e);
//    }
//  }

  @Override
  public Encoding getEncoding() {
    return encodingForDataPage;
  }

  @Override
  public void reset() {
    close();
    values = new HashSet<>();
  }

  @Override
  public void close() {
    values = null;
    for (RunLengthBitPackingHybridEncoder encoder : encoders) {
      encoder.close();
    }
    encoders.clear();
  }

//
//  public static class TestIntegerIndexValuesWriter extends IndexValuesWriter {
//
//    private IntList intContent = new IntList();
//    private Set<Integer> indexSet = new HashSet<>();
//    StringBuffer strbuf = new StringBuffer();
//    private List<RunLengthBitPackingHybridEncoder> encoders = new ArrayList<RunLengthBitPackingHybridEncoder>();
//
//
//
//
//    public TestIntegerIndexValuesWriter(int maxIndexByteSize, Encoding encodingForDataPage, Encoding encodingForIndexpage, ByteBufferAllocator allocator){
//      super(maxIndexByteSize,encodingForDataPage,encodingForIndexpage,allocator);
//    }
//
//    @Override
//    public void writeInteger(int v) {
//      if(!indexSet.contains(v)){
//        indexSet.add(v);
//        indexByteSize += 4;
//      }
//      intContent.add(v);
//
//    }
//
//
//
//    @Override
//    public BytesInput getBytes() {
//      int indexSize = indexSet.size()-1;
//      int bitWidth = BytesUtils.getWidthFromMaxInt(indexSize);
//      int initlalSlabSize = CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_INITIAL_SLAB_SIZE,maxIndexByteSize,10);
//      RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth,initlalSlabSize,maxIndexByteSize,this.allocator);
//      encoders.add(encoder);
//
//      IntIterator intIterator = intContent.iterator();
//      try{
//        while(intIterator.hasNext()){
//          encoder.writeInt(intIterator.next());
//        }
//
//        byte[] bytesHeader = new byte[] { (byte) bitWidth };
//        BytesInput rleEncodeBytes = encoder.toBytes();
//        BytesInput bytes = concat(BytesInput.from(bytesHeader),rleEncodeBytes);
//
//        lastUsedIndexSize = indexSet.size();
//        lastUsedIndexByteSize = indexByteSize;
//        return bytes;
//      }catch(IOException e){
//        throw new ParquetEncodingException("could not encode the values",e);
//      }
//    }
//
//    @Override
//    public IndexPage toIndexPageAndClose() {
//      if(lastUsedIndexSize>0){
//        int pageSize=maxIndexByteSize;
//        PlainValuesWriter indexEncoder = new PlainValuesWriter(lastUsedIndexByteSize,maxIndexByteSize,allocator);
//        Iterator<Integer> intIterator = indexSet.iterator();
//
//        for(int i = 0 ; i < lastUsedIndexSize; i++){
//          indexEncoder.writeInteger(intIterator.next());
//        }
//        return indexPage(indexEncoder);
//      }
//      return null;
//    }
//
//    @Override
//    public long getBufferedSize() {
////      strbuf.
//      return indexByteSize;
//    }
//
//    @Override
//    public long getAllocatedSize() {
//      return indexByteSize;
//    }
//
//    @Override
//    public String memUsageString(String prefix) {
//      return String.format(
//        "%s DictionaryValuesWriter{\n"
//          + "%s\n"
//          + "%s\n"
//          + "%s}\n",
//        prefix,
//        prefix + " index:" + indexByteSize,
//        prefix + " values:",
//        prefix
//      );
//    }
//  }
//
//
}
