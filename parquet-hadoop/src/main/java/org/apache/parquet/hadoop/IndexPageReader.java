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
package org.apache.parquet.hadoop;

import org.apache.parquet.Strings;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.IndexPage;
import org.apache.parquet.column.page.IndexPageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.column.Encoding.INDEX;

/**
 * A {@link IndexPageReadStore} implementation that reads dictionaries from
 * an open {@link ParquetFileReader}.
 *
 * This implementation will delegate index reads to a
 * {@link ColumnChunkPageReadStore} to avoid extra reads after a row group has
 * been loaded into memory.
 */
class IndexPageReader implements IndexPageReadStore {

  private final ParquetFileReader reader;
  private final Map<String, ColumnChunkMetaData> columns;
  private final Map<String, IndexPage> cache = new HashMap<String, IndexPage>();
  private ColumnChunkPageReadStore rowGroup = null;

  IndexPageReader(ParquetFileReader reader, BlockMetaData block) {
    this.reader = reader;
    this.columns = new HashMap<String, ColumnChunkMetaData>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath().toDotString(), column);
    }
  }

  /**
   * Sets this reader's row group's page store. When a row group is set, this
   * reader will delegate to that row group to return ubdex pages. This
   * avoids seeking and re-reading index bytes after this reader's row
   * group is loaded into memory.
   *
   * @param rowGroup a ColumnChunkPageReadStore for this reader's row group
   */
  void setRowGroup(ColumnChunkPageReadStore rowGroup) {
    this.rowGroup = rowGroup;
  }

  @Override
  public IndexPage readIndexPage(ColumnDescriptor descriptor) {
    if (rowGroup != null) {
      // if the row group has already been read, use that index
      return rowGroup.readIndexPage(descriptor);
    }

    String dotPath = Strings.join(descriptor.getPath(), ".");
    ColumnChunkMetaData column = columns.get(dotPath);
    if (column == null) {
      throw new ParquetDecodingException(
          "Cannot load index, unknown column: " + dotPath);
    }

    if (cache.containsKey(dotPath)) {
      return cache.get(dotPath);
    }

    try {
      synchronized (cache) {
        // check the cache again in case this thread waited on another reading the same page
        if (!cache.containsKey(dotPath)) {
          IndexPage index = hasIndexPage(column) ? reader.readIndex(column) : null;
          // copy the index to ensure it can be reused if it is returned
          // more than once. this can happen when a IndexFilter has two or
          // more predicates for the same column.
          cache.put(dotPath, reusableCopy(index));
        }
      }

      return cache.get(dotPath);
    } catch (IOException e) {
      throw new ParquetDecodingException(
          "Failed to read index", e);
    }
  }

  private static IndexPage reusableCopy(IndexPage index) {
    if (index == null) {
      return null;
    }
    try {
      return new IndexPage(
          BytesInput.from(index.getBytes().toByteArray()),
        index.getIndexSize(), index.getEncoding());
    } catch (IOException e) {
      throw new ParquetDecodingException("Cannot read index", e);
    }
  }

  private boolean hasIndexPage(ColumnChunkMetaData column) {
    EncodingStats stats = column.getEncodingStats();
    if (stats != null) {
      // ensure there is a index page and that it is used to encode data pages
      return stats.hasIndexPages() && stats.hasIndexEncodedPages();
    }

    Set<Encoding> encodings = column.getEncodings();
    return encodings.contains(INDEX);
  }
}
