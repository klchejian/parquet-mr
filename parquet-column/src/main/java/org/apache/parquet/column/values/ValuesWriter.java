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
package org.apache.parquet.column.values;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.IndexPage;
import org.apache.parquet.io.api.Binary;

/**
 * base class to implement an encoding for a given column
 *
 * @author Julien Le Dem
 *
 */
public abstract class ValuesWriter {

  /**
   * used to decide if we want to work to the next page
   * @return the size of the currently buffered data (in bytes)
   */
  public abstract long getBufferedSize();


  // TODO: maybe consolidate into a getPage
  /**
   * @return the bytes buffered so far to write to the current page
   */
  public abstract BytesInput getBytes();

  /**
   * called after getBytes() and before reset()
   * @return the encoding that was used to encode the bytes
   */
  public abstract Encoding getEncoding();

  /**
   * called after getBytes() to reset the current buffer and start writing the next page
   */
  public abstract void reset();

  /**
   * Called to close the values writer. Any output stream is closed and can no longer be used.
   * All resources are released.
   */
  public void close() {
  }

  /**
   * Returns the dictionary generated by this writer if one was created.
   * As part of this operation the dictionary is closed and will not have
   * any new values written into it.
   *
   * @return the dictionary page or null if not dictionary based
   */
  public DictionaryPage toDictPageAndClose() {
    return null;
  }

  public IndexPage toIndexPageAndClose(){
    return null;
  }

  /**
   * reset the dictionary when a new block starts
   */
  public void resetDictionary() {
  }

  /**
   * reset the index when a new block starts
   */
  public void resetIndex(){

  }

  /**
   * @param value the value to encode
   */
  public void writeByte(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * ( > {@link #getBufferedSize} )
   * @return the allocated size of the buffer
   */
  abstract public long getAllocatedSize();

  /**
   * @param v the value to encode
   */
  public void writeBoolean(boolean v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeBytes(Binary v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeInteger(int v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeLong(long v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeDouble(double v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeFloat(float v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  abstract public String memUsageString(String prefix);

}
