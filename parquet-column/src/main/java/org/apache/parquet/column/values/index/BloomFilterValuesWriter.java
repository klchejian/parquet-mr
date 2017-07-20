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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.parquet.bytes.BytesInput.concat;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 *  if the dictionary gets too big
 *
 */
public abstract class BloomFilterValuesWriter extends IndexValuesWriter {

  public Logger logger = LoggerFactory.getLogger(this.getClass());


  private static final int MIN_INITIAL_SLAB_SIZE = 64;
  private static final double PREDEFINED_FALSE_POSITIVE_PROBABILITY = 0.01;
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

  protected ArrayList<Integer> numIndex = new ArrayList<>();
  protected List<PlainValuesWriter> encoders = new ArrayList<>();


  @Override
  public void resetIndex() {
    lastUsedIndexByteSize = 0;
    lastUsedIndexSize = 0;
    indexByteSize = 0;
    dataByteSize = 0;
    clearIndexContent();

  }

  protected abstract void clearIndexContent();

  @Override
  public void reset() {
    close();
//    values = new HashSet<>();
    numIndex = new ArrayList<>();
    dataByteSize = 0;
  }

  @Override
  public void close() {
    numIndex = null;
    for (PlainValuesWriter encoder : encoders) {
      encoder.close();
    }
    encoders.clear();
  }

  protected int getNumBits(int n, double p) {
    p = PREDEFINED_FALSE_POSITIVE_PROBABILITY;
    return -(int)Math.ceil((n*Math.log(p))/((Math.log(2))*(Math.log(2))));
  }

  protected int getNumHashFun(double p) {
    p = PREDEFINED_FALSE_POSITIVE_PROBABILITY;
    return -(int)Math.ceil(Math.log(p)/Math.log(2));
  }

  @Override
  public long getBufferedSize() {
    return dataByteSize;
  }

  @Override
  public long getAllocatedSize() {
    return dataByteSize + indexByteSize;
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

  @Override
  public BytesInput getBytes() {
    return null;
  }

  public static class TestIndexValuesWriter extends BloomFilterValuesWriter {

    private Set<Binary> indexSet = new HashSet<>();

    public TestIndexValuesWriter() {
      super(0,null,null,null);
    }

    @Override
    public void writeBytes(Binary v) {
      indexSet.add(v);
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      int numBits = getNumBits(indexSet.size(), 0.05);
      int numHashFunctions = getNumHashFun(0.05);
      BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
      int bitsize = bloomFilter.getBitSet().length + 2;
      PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

      indexEncoder.writeLong(numBits);
      indexEncoder.writeLong(numHashFunctions);

      Iterator<Binary> indexIterator = indexSet.iterator();

      while (indexIterator.hasNext()) {
        bloomFilter.addBinary(indexIterator.next());
      }

      long[] bitSet = bloomFilter.getBitSet();
      for(int i = 0 ; i < bitSet.length; i++) {
        indexEncoder.writeLong(bitSet[i]);
      }
      return indexPage(indexEncoder, bitsize);
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

  }

  public static class TestBinaryValuesWriter extends BloomFilterValuesWriter {

    private Set<Binary> indexSet = new HashSet<>();

    public TestBinaryValuesWriter(int maxIndexBytesSize, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize,null,encodingForIndexpage,allocator);
    }

    @Override
    public void writeBytes(Binary v) {
      indexSet.add(v);
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      int numBits = getNumBits(indexSet.size(), 0.05);
      int numHashFunctions = getNumHashFun(0.05);
      BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
      int bitsize = bloomFilter.getBitSet().length + 2;
      PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

      indexEncoder.writeLong(numBits);
      indexEncoder.writeLong(numHashFunctions);

      Iterator<Binary> indexIterator = indexSet.iterator();

      while (indexIterator.hasNext()) {
        bloomFilter.addBinary(indexIterator.next());
      }

      long[] bitSet = bloomFilter.getBitSet();
      for(int i = 0 ; i < bitSet.length; i++) {
        indexEncoder.writeLong(bitSet[i]);
      }
      return indexPage(indexEncoder, bitsize);
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

  }

  public static class TestLongValuesWriter extends BloomFilterValuesWriter {

    private Set<Long> indexSet = new HashSet<>();

    public TestLongValuesWriter(int maxIndexBytesSize, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize,null,encodingForIndexpage,allocator);
    }

    @Override
    public void writeLong(long v) {
      indexSet.add(v);
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      int numBits = getNumBits(indexSet.size(), 0.05);
      int numHashFunctions = getNumHashFun(0.05);
      BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
      int bitsize = bloomFilter.getBitSet().length + 2;
      PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

      indexEncoder.writeLong(numBits);
      indexEncoder.writeLong(numHashFunctions);

      Iterator<Long> indexIterator = indexSet.iterator();

      while (indexIterator.hasNext()) {
        bloomFilter.addLong(indexIterator.next());
      }

      long[] bitSet = bloomFilter.getBitSet();
      for(int i = 0 ; i < bitSet.length; i++) {
        indexEncoder.writeLong(bitSet[i]);
      }
      return indexPage(indexEncoder, bitsize);
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

  }

  public static class TestDoubleValuesWriter extends BloomFilterValuesWriter {

    private Set<Double> indexSet = new HashSet<>();

    public TestDoubleValuesWriter(int maxIndexBytesSize, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize,null,encodingForIndexpage,allocator);
    }

    @Override
    public void writeDouble(double v) {
      indexSet.add(v);
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      int numBits = getNumBits(indexSet.size(), 0.05);
      int numHashFunctions = getNumHashFun(0.05);
      BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
      int bitsize = bloomFilter.getBitSet().length + 2;
      PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

      indexEncoder.writeLong(numBits);
      indexEncoder.writeLong(numHashFunctions);

      Iterator<Double> indexIterator = indexSet.iterator();

      while (indexIterator.hasNext()) {
        bloomFilter.addDouble(indexIterator.next());
      }

      long[] bitSet = bloomFilter.getBitSet();
      for(int i = 0 ; i < bitSet.length; i++) {
        indexEncoder.writeLong(bitSet[i]);
      }
      return indexPage(indexEncoder, bitsize);
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

  }

  public static class TestIntegerValuesWriter extends BloomFilterValuesWriter {

    private Set<Integer> indexSet = new HashSet<>();

    public TestIntegerValuesWriter(int maxIndexBytesSize, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize,null,encodingForIndexpage,allocator);
    }

    @Override
    public void writeInteger(int v) {
      indexSet.add(v);
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      int numBits = getNumBits(indexSet.size(), 0.05);
      int numHashFunctions = getNumHashFun(0.05);
      BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
      int bitsize = bloomFilter.getBitSet().length + 2;
      PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

      indexEncoder.writeLong(numBits);
      indexEncoder.writeLong(numHashFunctions);

      Iterator<Integer> indexIterator = indexSet.iterator();

      while (indexIterator.hasNext()) {
        bloomFilter.addInteger(indexIterator.next());
      }

      long[] bitSet = bloomFilter.getBitSet();
      for(int i = 0 ; i < bitSet.length; i++) {
        indexEncoder.writeLong(bitSet[i]);
      }
      return indexPage(indexEncoder, bitsize);
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

  }

  public static class TestFloatValuesWriter extends BloomFilterValuesWriter {

    private Set<Float> indexSet = new HashSet<>();

    public TestFloatValuesWriter(int maxIndexBytesSize, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize,null,encodingForIndexpage,allocator);
    }

    @Override
    public void writeFloat(float v) {
      indexSet.add(v);
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      int numBits = getNumBits(indexSet.size(), 0.05);
      int numHashFunctions = getNumHashFun(0.05);
      BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
      int bitsize = bloomFilter.getBitSet().length + 2;
      PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

      indexEncoder.writeLong(numBits);
      indexEncoder.writeLong(numHashFunctions);

      Iterator<Float> indexIterator = indexSet.iterator();

      while (indexIterator.hasNext()) {
        bloomFilter.addFloat(indexIterator.next());
      }

      long[] bitSet = bloomFilter.getBitSet();
      for(int i = 0 ; i < bitSet.length; i++) {
        indexEncoder.writeLong(bitSet[i]);
      }
      return indexPage(indexEncoder, bitsize);
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

  }

  public static class BloomFilterBinaryValuesWriter extends BloomFilterValuesWriter {
//    private ArrayList<Binary> binaryArray = new ArrayList<>();
    private Set<Binary> indexSet = new HashSet<>();
    private ArrayList<Binary> dataBinary = new ArrayList<>();
    private long startTime = 0;
    private  long endTime = 0;

    public BloomFilterBinaryValuesWriter(int maxIndexBytesSize, Encoding encodingForDatapage, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize, encodingForDatapage, encodingForIndexpage, allocator);
    }

    @Override
    public void writeBytes(Binary v) {
      if( indexSet.add(v) ){
        indexByteSize += v.length();
      }
      dataBinary.add(v);
      if(v.equals(Binary.fromString(""))){
        dataByteSize += 1;
      }else {
        dataByteSize += v.length();
      }
      if(dataBinary.size() == 1){
        startTime = System.currentTimeMillis();
      }
      if(dataBinary.size() == 40000){
        endTime = System.currentTimeMillis();
        float second = (endTime - startTime)/1000F;
        System.out.print("---------=====secondds: " + second);
      }
    }

//    @Override
//    public void writeBytes(Binary v) {
//      int indexNum = binaryArray.indexOf(v);
//      if(indexNum == -1){
//        indexNum = binaryArray.size();
//        binaryArray.add(v);
//        indexByteSize += v.length();
//      }
//
//      numIndex.add(indexNum);
//      if(v.equals(Binary.fromString(""))){
//        dataByteSize += 1;
//      }else {
//        dataByteSize += v.length();
//      }
//      if(binaryArray.size() == 1){
//        startTime = System.currentTimeMillis();
//      }
//      if(binaryArray.size() == 40000){
//        endTime = System.currentTimeMillis();
//        float second = (endTime - startTime)/1000F;
//        System.out.print("---------=====secondds: " + second);
//      }
//    }

    @Override
    public BytesInput getBytes() {
      if (dataBinary.size() > 0) {
//        logger.info("--------che-------------indexBytesize" + dataByteSize + "maxIndexBYteSize" + maxIndexByteSize);
        PlainValuesWriter encoder = null;
        if(dataByteSize >= maxIndexByteSize){
          encoder = new PlainValuesWriter(dataByteSize, dataByteSize, this.allocator);
        } else {
          encoder = new PlainValuesWriter(dataByteSize, maxIndexByteSize, this.allocator);
        }
        encoders.add(encoder);

        Iterator<Binary> dataIterator = dataBinary.iterator();

        try {
          while ( dataIterator.hasNext() ) {
            encoder.writeBytes(dataIterator.next());
          }
          BytesInput bytes = encoder.getBytes();

          lastUsedIndexSize = indexSet.size();
          lastUsedIndexByteSize = indexByteSize;
          return bytes;
        } catch (Exception e) {
          throw new ParquetEncodingException("could not encoder the values " ,e);
        }
//        Iterator<Integer> indexIterator = numIndex.iterator();
//        try {
//          while (indexIterator.hasNext()) {
//            encoder.writeBytes(binaryArray.get(indexIterator.next()));
//          }
//          BytesInput bytes = encoder.getBytes();
//
//          lastUsedIndexSize = binaryArray.size();
//          lastUsedIndexByteSize = indexByteSize;
//          return bytes;
//        } catch (Exception e) {
//          throw new ParquetEncodingException("could not encode the values", e);
//        }
      }
      return null;
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      if(lastUsedIndexSize > 0 ) {
        int numBits = getNumBits(lastUsedIndexSize ,0.05);
        int numHashFunctions = getNumHashFun(0.05);
        BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
        int bitsize = bloomFilter.getBitSet().length + 2;
        PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

        indexEncoder.writeLong(numBits);
        indexEncoder.writeLong(numHashFunctions);


        Iterator<Binary> indexIterator = indexSet.iterator();

        while ( indexIterator.hasNext() ) {
          bloomFilter.addBinary(indexIterator.next());
        }

//        for(int i = 0; i<lastUsedIndexSize; i++){
//          bloomFilter.addBinary(binaryArray.get(i));
//        }


        long[] bitSet = bloomFilter.getBitSet();
        for(int i = 0; i < bitSet.length; i++) {
          indexEncoder.writeLong(bitSet[i]);
        }
        return indexPage(indexEncoder, bitsize);

      }
      return null;
    }

    @Override
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
//      binaryArray = null;
//      binaryArray = new ArrayList<>();
    }

    @Override
    public void reset() {
      super.reset();
      dataBinary = null;
      dataBinary = new ArrayList<>();
    }
  }

  public static class BloomFilterLongValuesWriter extends BloomFilterValuesWriter {
//    private ArrayList<Long> longArray = new ArrayList<>();
      private Set<Long> indexSet = new HashSet<>();
      private ArrayList<Long> dataBinary = new ArrayList<>();

    public BloomFilterLongValuesWriter(int maxIndexBytesSize, Encoding encodingForDatapage, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize, encodingForDatapage, encodingForIndexpage, allocator);
    }

    @Override
    public void writeLong(long v) {

      if(indexSet.add(v)) {
        indexByteSize += 8;
      }
      dataBinary.add(v);
      dataByteSize += 8;
//      System.out.println("----------binaryArraySize" + longArray.size() + "binary len:8(long)"+ "----------");
    }

    @Override
    public BytesInput getBytes() {
      if (dataBinary.size() > 0) {
        logger.info("--------che-------------indexBytesize" + dataByteSize + "maxIndexBYteSize" + maxIndexByteSize);
        PlainValuesWriter encoder = null;
        if(dataByteSize >= maxIndexByteSize) {
          encoder = new PlainValuesWriter(dataByteSize, dataByteSize, this.allocator);
        } else {
          encoder = new PlainValuesWriter(dataByteSize, maxIndexByteSize, this.allocator);
        }
        encoders.add(encoder);

        Iterator<Long> dataIterator = dataBinary.iterator();

        try {
          while (dataIterator.hasNext()) {
            encoder.writeLong(dataIterator.next());
          }
          BytesInput bytes = encoder.getBytes();

          lastUsedIndexSize = indexSet.size();
          lastUsedIndexByteSize = indexByteSize;
          return bytes;
        } catch (Exception e) {
          throw new ParquetEncodingException("could not encode the values", e);
        }
      }
      return null;
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      if(lastUsedIndexSize > 0 ) {
        int numBits = getNumBits(lastUsedIndexSize ,0.05);
        int numHashFunctions = getNumHashFun(0.05);
        BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
        int bitsize = bloomFilter.getBitSet().length + 2;
        PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

        indexEncoder.writeLong(numBits);
        indexEncoder.writeLong(numHashFunctions);

        Iterator<Long> indexIterator = indexSet.iterator();

        while ( indexIterator.hasNext() ) {
          bloomFilter.addLong(indexIterator.next());
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
    protected void clearIndexContent() {
//      longArray = null;
//      longArray = new ArrayList<>();
      indexSet = null;
      indexSet = new HashSet<>();
    }

    @Override
    public void reset() {
      super.reset();
      dataBinary = null;
      dataBinary = new ArrayList<>();
    }
  }

  public static class BloomFilterDoubleValuesWriter extends BloomFilterValuesWriter {
//    private ArrayList<Double> doubleArray = new ArrayList<>();
    private Set<Double> indexSet = new HashSet<>();
    private ArrayList<Double> dataBinary = new ArrayList<>();
    public BloomFilterDoubleValuesWriter(int maxIndexBytesSize, Encoding encodingForDatapage, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
      super(maxIndexBytesSize, encodingForDatapage, encodingForIndexpage, allocator);
    }

    @Override
    public void writeDouble(double v) {
      if(indexSet.add(v)) {
        indexByteSize += 8;
      }
      dataBinary.add(v);
      dataByteSize += 8;
//      System.out.println("----------binaryArraySize" + doubleArray.size() + "binary len:8(double)"+ "----------");
    }

    @Override
    public BytesInput getBytes() {
      if (dataBinary.size() > 0) {
        logger.info("--------che-------------indexBytesize" + dataByteSize + "maxIndexBYteSize" + maxIndexByteSize);
        PlainValuesWriter encoder = new PlainValuesWriter(dataByteSize, maxIndexByteSize, this.allocator);
        encoders.add(encoder);

        Iterator<Double> dataIterator = dataBinary.iterator();

        try {
          while (dataIterator.hasNext()) {
            encoder.writeDouble(dataIterator.next());
          }
          BytesInput bytes = encoder.getBytes();

          lastUsedIndexSize = indexSet.size();
          lastUsedIndexByteSize = indexByteSize;
          return bytes;
        } catch (Exception e) {
          throw new ParquetEncodingException("could not encode the values", e);
        }
      }
      return null;
    }

    @Override
    public IndexPage toIndexPageAndClose() {
      if(lastUsedIndexSize > 0 ) {
        int numBits = getNumBits(lastUsedIndexSize ,0.05);
        int numHashFunctions = getNumHashFun(0.05);
        BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
        int bitsize = bloomFilter.getBitSet().length + 2;
        PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

        indexEncoder.writeLong(numBits);
        indexEncoder.writeLong(numHashFunctions);


        Iterator<Double> indexIterator = indexSet.iterator();

        while ( indexIterator.hasNext() ) {
          bloomFilter.addDouble(indexIterator.next());
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
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

    @Override
    public void reset() {
      super.reset();
      dataBinary = null;
      dataBinary = new ArrayList<>();
    }
  }

  public static class BloomFilterIntegerValuesWriter extends BloomFilterValuesWriter {
//      private ArrayList<Integer> intArray = new ArrayList<>();
    private Set<Integer> indexSet = new HashSet<>();
    private ArrayList<Integer> dataBinary = new ArrayList<>();
      public BloomFilterIntegerValuesWriter(int maxIndexBytesSize, Encoding encodingForDatapage, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
        super(maxIndexBytesSize, encodingForDatapage, encodingForIndexpage, allocator);
      }

    @Override
      public void writeInteger(int v) {
      if(indexSet.add(v)) {
          indexByteSize += 4;
        }
      dataBinary.add(v);
        dataByteSize += 4;
//      System.out.println("----------binaryArraySize" + intArray.size() + "binary len:4(int)"+ "----------");
      }

      @Override
      public BytesInput getBytes() {
        if (dataBinary.size() > 0) {
          logger.info("--------che-------------indexBytesize" + dataByteSize + "maxIndexBYteSize" + maxIndexByteSize);
          PlainValuesWriter encoder = new PlainValuesWriter(dataByteSize, maxIndexByteSize, this.allocator);
          encoders.add(encoder);

          Iterator<Integer> dataIterator = dataBinary.iterator();

          try {
            while (dataIterator.hasNext()) {
              encoder.writeInteger(dataIterator.next());
            }
            BytesInput bytes = encoder.getBytes();

            lastUsedIndexSize = indexSet.size();
            lastUsedIndexByteSize = indexByteSize;
            return bytes;
          } catch (Exception e) {
            throw new ParquetEncodingException("could not encode the values", e);
          }
        }
        return null;
      }

      @Override
      public IndexPage toIndexPageAndClose() {
        if(lastUsedIndexSize > 0 ) {
          int numBits = getNumBits(lastUsedIndexSize ,0.05);
          int numHashFunctions = getNumHashFun(0.05);
          BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
          int bitsize = bloomFilter.getBitSet().length + 2;
          PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

          indexEncoder.writeLong(numBits);
          indexEncoder.writeLong(numHashFunctions);


          Iterator<Integer> indexIterator = indexSet.iterator();

          while ( indexIterator.hasNext() ) {
            bloomFilter.addInteger(indexIterator.next());
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
    protected void clearIndexContent() {
      indexSet = null;
      indexSet = new HashSet<>();
    }

    @Override
    public void reset() {
      super.reset();
      dataBinary = null;
      dataBinary = new ArrayList<>();
    }
    }

  public static class BloomFilterFloatValuesWriter extends BloomFilterValuesWriter {
//      private ArrayList<Float> floatArray = new ArrayList<>();
    private Set<Float> indexSet = new HashSet<>();
    private ArrayList<Float> dataBinary = new ArrayList<>();

      public BloomFilterFloatValuesWriter(int maxIndexBytesSize, Encoding encodingForDatapage, Encoding encodingForIndexpage, ByteBufferAllocator allocator) {
        super(maxIndexBytesSize, encodingForDatapage, encodingForIndexpage, allocator);
      }

    @Override
    public void writeFloat(float v) {
      if(indexSet.add(v)) {
          indexByteSize += 4;
        }
      dataBinary.add(v);
      dataByteSize += 4;
//      System.out.println("----------binaryArraySize" + floatArray.size() + "binary len:4(float)"+ "----------");
      }

      @Override
      public BytesInput getBytes() {
        if (dataBinary.size() > 0) {
          logger.info("--------che-------------indexBytesize" + dataByteSize + "maxIndexBYteSize" + maxIndexByteSize);
          PlainValuesWriter encoder = new PlainValuesWriter(dataByteSize, maxIndexByteSize, this.allocator);
          encoders.add(encoder);

          Iterator<Float> dataIterator = dataBinary.iterator();

          try {
            while (dataIterator.hasNext()) {
              encoder.writeFloat(dataIterator.next());
            }
            BytesInput bytes = encoder.getBytes();

            lastUsedIndexSize = indexSet.size();
            lastUsedIndexByteSize = indexByteSize;
            return bytes;
          } catch (Exception e) {
            throw new ParquetEncodingException("could not encode the values", e);
          }
        }
        return null;
      }

      @Override
      public IndexPage toIndexPageAndClose() {
        if(lastUsedIndexSize > 0 ) {
          int numBits = getNumBits(lastUsedIndexSize ,0.05);
          int numHashFunctions = getNumHashFun(0.05);
          BloomFilter bloomFilter = new BloomFilter(numBits,numHashFunctions);
          int bitsize = bloomFilter.getBitSet().length + 2;
          PlainValuesWriter indexEncoder = new PlainValuesWriter(bitsize, bitsize*8, allocator);

          indexEncoder.writeLong(numBits);
          indexEncoder.writeLong(numHashFunctions);


          Iterator<Float> indexIterator = indexSet.iterator();

          while ( indexIterator.hasNext() ) {
            bloomFilter.addFloat(indexIterator.next());
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
    protected void clearIndexContent() {
//      floatArray = null;
//      floatArray = new ArrayList<>();
    indexSet = null;
    indexSet = new HashSet<>();
  }

  @Override
  public void reset() {
    super.reset();
    dataBinary = null;
    dataBinary = new ArrayList<>();
  }

  }

}
