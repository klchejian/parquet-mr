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

import javafx.scene.effect.Bloom;
import org.apache.parquet.Log;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.IndexPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
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
public abstract class BloomFilterValuesWriter extends IndexValuesWriter {

  private static final int MIN_INITIAL_SLAB_SIZE = 64;
  /**
   *
   * @param maxIndexByteSize
   * @param encodingForDataPage
   * @param encodingForIndexPage
   * @param allocator
   */
  protected BloomFilterValuesWriter(int maxIndexByteSize, Encoding encodingForDataPage, Encoding encodingForIndexPage, ByteBufferAllocator allocator) {
    super(maxIndexByteSize, encodingForDataPage, encodingForIndexPage, allocator);
  }

  public static class TestIntegerIndexValuesWriter extends BloomFilterValuesWriter {

    private IntList intContent = new IntList();
    private Set<Integer> indexSet = new HashSet<>();
    StringBuffer strbuf = new StringBuffer();
    private List<RunLengthBitPackingHybridEncoder> encoders = new ArrayList<RunLengthBitPackingHybridEncoder>();




    public TestIntegerIndexValuesWriter(int maxIndexByteSize, Encoding encodingForDataPage, Encoding encodingForIndexpage, ByteBufferAllocator allocator){
      super(maxIndexByteSize,encodingForDataPage,encodingForIndexpage,allocator);
    }

    @Override
    public void writeInteger(int v) {
      if(!indexSet.contains(v)){
        indexSet.add(v);
        indexByteSize += 4;
      }
      intContent.add(v);

    }



    @Override
    public BytesInput getBytes() {
      int indexSize = indexSet.size()-1;
      int bitWidth = BytesUtils.getWidthFromMaxInt(indexSize);
      int initlalSlabSize = CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_INITIAL_SLAB_SIZE,maxIndexByteSize,10);
      RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth,initlalSlabSize,maxIndexByteSize,this.allocator);
      encoders.add(encoder);

      IntIterator intIterator = intContent.iterator();
      try{
        while(intIterator.hasNext()){
          encoder.writeInt(intIterator.next());
        }

        byte[] bytesHeader = new byte[] { (byte) bitWidth };
        BytesInput rleEncodeBytes = encoder.toBytes();
        BytesInput bytes = concat(BytesInput.from(bytesHeader),rleEncodeBytes);

        lastUsedIndexSize = indexSet.size();
        lastUsedIndexByteSize = indexByteSize;
        return bytes;
      }catch(IOException e){
        throw new ParquetEncodingException("could not encode the values",e);
      }
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      if(lastUsedIndexSize>0){
        int pageSize=maxIndexByteSize;
        PlainValuesWriter indexEncoder = new PlainValuesWriter(lastUsedIndexByteSize,maxIndexByteSize,allocator);
        Iterator<Integer> intIterator = indexSet.iterator();

        for(int i = 0 ; i < lastUsedIndexSize; i++){
          indexEncoder.writeInteger(intIterator.next());
        }
        return indexPage(indexEncoder, lastUsedIndexSize);
      }
      return null;
    }

    @Override
    public long getBufferedSize() {
//      strbuf.
      return indexByteSize;
    }

    @Override
    public long getAllocatedSize() {
      return indexByteSize;
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
        "%s DictionaryValuesWriter{\n"
          + "%s\n"
          + "%s\n"
          + "%s}\n",
        prefix,
        prefix + " index:" + indexByteSize,
        prefix + " values:",
        prefix
      );
    }
  }

  public static class BloomFilterBinaryValuesWriter extends BloomFilterValuesWriter {
    private ArrayList<Binary> binaryArray = new ArrayList<>();
    private ArrayList<Integer> binaryIndex = new ArrayList<>();
    private List<PlainValuesWriter> encoders = new ArrayList<>();

    public BloomFilterBinaryValuesWriter(int maxIndexBytesSize, Encoding encodingForDatapage, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize, encodingForDatapage, encodingForDatapage, allocator);
    }

    @Override
    public void writeBytes(Binary v) {
      int indexNum = binaryArray.indexOf(v);
      if(indexNum == -1){
        indexNum = binaryArray.size();
        binaryArray.add(v);
      }
      binaryIndex.add(indexNum);
    }

    @Override
    public BytesInput getBytes() {
      int indexSize = binaryArray.size() - 1;
      int bitWidth = BytesUtils.getWidthFromMaxInt(indexSize);
      int initlalSlabSize = CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_INITIAL_SLAB_SIZE,maxIndexByteSize,10);
      PlainValuesWriter encoder = new PlainValuesWriter(binaryArray.size(),maxIndexByteSize, this.allocator);
      encoders.add(encoder);

      Iterator<Integer> binaryIterator = binaryIndex.iterator();
      try {
        while(binaryIterator.hasNext()) {
          encoder.writeBytes(binaryArray.get(binaryIterator.next()));
        }
        byte[] bytesHeader = new byte[] { (byte) bitWidth };
        BytesInput rleEncodeBytes = encoder.getBytes();
        BytesInput bytes = concat(BytesInput.from(bytesHeader), rleEncodeBytes);

        lastUsedIndexSize = binaryArray.size();
        lastUsedIndexByteSize = indexByteSize;
        return bytes;
      } catch (Exception e) {
        throw new ParquetEncodingException("could not encode the values", e);
      }
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      if(lastUsedIndexSize > 0 ) {
        int numBits = 128;
        int numHashFunctions = 4;
        BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
        int bitsize = bloomFilter.getBitSet().length + 2;
        PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

        indexEncoder.writeLong(numBits);
        indexEncoder.writeLong(numHashFunctions);


        for(int i = 0; i<lastUsedIndexSize; i++){
          bloomFilter.addBinary(binaryArray.get(i));
        }


        long[] bitSet = bloomFilter.getBitSet();
        for(int i = 0; i < bitSet.length; i++) {
          indexEncoder.writeLong(bitSet[i]);
        }
        return indexPage(indexEncoder, bitsize);

      }
      return null;
    }

    @Override
    public long getBufferedSize() {
      return indexByteSize;
    }

    @Override
    public long getAllocatedSize() {
      return indexByteSize;
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
        "%s DictionaryValuesWriter{\n"
          + "%s\n"
          + "%s\n"
          + "%s}\n",
        prefix,
        prefix + " index:" + indexByteSize,
        prefix + " values:",
        prefix
      );
    }
  }

//  public static class BloomFilterLongValuesWriter extends BloomFilterValuesWriter
//  public static class BloomFilterDoubleValuesWriter extends BloomFilterValuesWriter
//  public static class BloomFilterIntegerValuesWriter extends BloomFilterValuesWriter
//  public static class BloomFilterFloatValuesWriter extends BloomFilterValuesWriter

}
