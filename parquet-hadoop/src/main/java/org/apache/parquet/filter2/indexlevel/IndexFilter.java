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
package org.apache.parquet.filter2.indexlevel;

import javafx.scene.effect.Bloom;
import org.apache.parquet.Log;
import org.apache.parquet.column.*;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.IndexPage;
import org.apache.parquet.column.page.IndexPageReadStore;
import org.apache.parquet.column.values.index.BloomFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.util.*;

import static org.apache.parquet.Preconditions.checkNotNull;


/**
 * Applies filters based on the contents of column index.
 */
public class IndexFilter implements FilterPredicate.Visitor<Boolean> {

  private static final Log LOG = Log.getLog(IndexFilter.class);
  private static final boolean BLOCK_MIGHT_MATCH = false;
  private static final boolean BLOCK_CANNOT_MATCH = true;

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns, IndexPageReadStore indices) {
    checkNotNull(pred, "pred");
    checkNotNull(columns, "columns");
    return pred.accept(new IndexFilter(columns, indices));
  }

  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();
  private final IndexPageReadStore indices;
  private IndexTypeName indexTypeName;

  private IndexFilter(List<ColumnChunkMetaData> columnsList, IndexPageReadStore indices) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }

    this.indices = indices;


  }

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    return columns.get(columnPath);
  }

  @SuppressWarnings("unchecked")
  private long[] expandIndex(ColumnChunkMetaData meta) throws IOException {
    ColumnDescriptor col = new ColumnDescriptor(meta.getPath().toArray(), meta.getType(), -1, -1);
    IndexPage page = indices.readIndexPage(col);

    // the chunk may not be index-encoded
    if (page == null) {
      return null;
    }

    Index index = page.getEncoding().initIndex(col, page);

    indexTypeName = index.getIndexTypeName();

    long[] indexByte = new long[index.getMaxId()+1];
//    Set indexSet = new HashSet<T>();

    for (int i=0; i<index.getMaxId()+1; i++) {
      indexByte[i] = index.decodeToLong(i);
//      switch(meta.getType()) {
//        case BINARY: indexSet.add(index.decodeToBinary(i));
//          break;
//        case INT32: indexSet.add(index.decodeToInt(i));
//          break;
//        case INT64: indexSet.add(index.decodeToLong(i));
//          break;
//        case FLOAT: indexSet.add(index.decodeToFloat(i));
//          break;
//        case DOUBLE: indexSet.add(index.decodeToDouble(i));
//          break;
//        default:
//          LOG.warn("Unknown index type" + meta.getType());
//      }
    }

    return indexByte;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
    T value = eq.getValue();

    if (value == null) {
      // the index contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column isn't in this file so all values are null, but the value
      // must be non-null because of the above check.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-index pages, don't bother decoding the
    // index because the row group can't be eliminated.
    if (hasNonIndexPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    try {
      long[] indexByte = expandIndex(meta);

      BloomFilter bloomFilter = new BloomFilter((int)indexByte[0], (int)indexByte[1]);
      bloomFilter.setBitSet(Arrays.copyOfRange(indexByte, 2,indexByte.length));
      switch(meta.getType()) {
        case BINARY: return !bloomFilter.testBinary((Binary) value);
        case INT32: return !bloomFilter.testInteger((Integer) value);
        case INT64: return !bloomFilter.testLong((Long) value);
        case FLOAT: return !bloomFilter.testFloat((Float) value);
        case DOUBLE: return !bloomFilter.testDouble((Double) value);
        default:
          LOG.warn("Unknown data type" + meta.getType());
      }
    } catch (IOException e) {
      LOG.warn("Failed to process index for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH; // cannot drop the row group based on this inex
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    T value = notEq.getValue();

    if (value == null && meta == null) {
      // the predicate value is null and all rows have a null value, so the
      // predicate is always false (null != null)
      return BLOCK_CANNOT_MATCH;
    }

    if (value == null) {
      // the index contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    if (meta == null) {
      // column is missing from this file and is always null and not equal to
      // the non-null test value, so the predicate is true for all rows
      return BLOCK_MIGHT_MATCH;
    }

    // if the chunk has non-index pages, don't bother decoding the
    // index because the row group can't be eliminated.
    if (hasNonIndexPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

//    try {
//      Set<T> dictSet = expandIndex(meta);
//      if (dictSet != null && dictSet.size() == 1 && dictSet.contains(value)) {
//        return BLOCK_CANNOT_MATCH;
//      }
//    } catch (IOException e) {
//      LOG.warn("Failed to process index for filter evaluation.", e);
//    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never less than a
      // value. for all x, null is never < x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-index pages, don't bother decoding the
    // index because the row group can't be eliminated.
    if (hasNonIndexPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = lt.getValue();

    try {
      long[] indexByte = expandIndex(meta);
      if (indexByte == null) {
        return BLOCK_MIGHT_MATCH;
      }

      BloomFilter bloomFilter = new BloomFilter((int)indexByte[0], (int)indexByte[1]);
      bloomFilter.setBitSet(Arrays.copyOfRange(indexByte, 2,indexByte.length-1));
      switch(meta.getType()) {
        case BINARY: return !bloomFilter.testBinary((Binary) value);
        case INT32: return !bloomFilter.testInteger((Integer) value);
        case INT64: return !bloomFilter.testLong((Long) value);
        case FLOAT: return !bloomFilter.testFloat((Float) value);
        case DOUBLE: return !bloomFilter.testDouble((Double) value);
        default:
          LOG.warn("Unknown data type" + meta.getType());
      }
//      for(T entry : indexSet) {
//        if(value.compareTo(entry) > 0) {
//          return BLOCK_MIGHT_MATCH;
//        }
//      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process index for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never less than or
      // equal to a value. for all x, null is never <= x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-index pages, don't bother decoding the
    // index because the row group can't be eliminated.
    if (hasNonIndexPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = ltEq.getValue();

    filterColumn.getColumnPath();

//    try {
//      Set<T> dictSet = expandIndex(meta);
//      if (dictSet == null) {
//        return BLOCK_MIGHT_MATCH;
//      }
//
//      for(T entry : dictSet) {
//        if(value.compareTo(entry) >= 0) {
//          return BLOCK_MIGHT_MATCH;
//        }
//      }
//
//      return BLOCK_CANNOT_MATCH;
//    } catch (IOException e) {
//      LOG.warn("Failed to process index for filter evaluation.", e);
//    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never greater than a
      // value. for all x, null is never > x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-index pages, don't bother decoding the
    // index because the row group can't be eliminated.
    if (hasNonIndexPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = gt.getValue();

//    try {
//      Set<T> dictSet = expandIndex(meta);
//      if (dictSet == null) {
//        return BLOCK_MIGHT_MATCH;
//      }
//
//      for(T entry : dictSet) {
//        if(value.compareTo(entry) < 0) {
//          return BLOCK_MIGHT_MATCH;
//        }
//      }
//
//      return BLOCK_CANNOT_MATCH;
//    } catch (IOException e) {
//      LOG.warn("Failed to process index for filter evaluation.", e);
//    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never greater than or
      // equal to a value. for all x, null is never >= x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-index pages, don't bother decoding the
    // index because the row group can't be eliminated.
    if (hasNonIndexPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = gtEq.getValue();

    filterColumn.getColumnPath();

//    try {
//      Set<T> dictSet = expandIndex(meta);
//      if (dictSet == null) {
//        return BLOCK_MIGHT_MATCH;
//      }
//
//      for(T entry : dictSet) {
//        if(value.compareTo(entry) <= 0) {
//          return BLOCK_MIGHT_MATCH;
//        }
//      }
//
//      return BLOCK_CANNOT_MATCH;
//    } catch (IOException e) {
//      LOG.warn("Failed to process index for filter evaluation.", e);
//    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public Boolean visit(And and) {
    return and.getLeft().accept(this) || and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Or or) {
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> udp) {
    throw new UnsupportedOperationException("UDP not supported with index evaluation.");
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> udp) {
    throw new UnsupportedOperationException("UDP not supported with index evaluation.");
  }

  @SuppressWarnings("deprecation")
  private static boolean hasNonIndexPages(ColumnChunkMetaData meta) {
    EncodingStats stats = meta.getEncodingStats();
    if (stats != null) {
      return stats.hasNonIndexEncodedPages();
    }

    // without EncodingStats, fall back to testing the encoding list
    Set<Encoding> encodings = new HashSet<Encoding>(meta.getEncodings());
    if (encodings.remove(Encoding.INDEX)) {
      // if remove returned true, PLAIN_DICTIONARY was present, which means at
      // least one page was dictionary encoded and 1.0 encodings are used

      // RLE and BIT_PACKED are only used for repetition or definition levels
      encodings.remove(Encoding.RLE);
      encodings.remove(Encoding.BIT_PACKED);

      if (encodings.isEmpty()) {
        return false; // no encodings other than dictionary or rep/def levels
      }

      return true;

    } else {
      // if PLAIN_DICTIONARY wasn't present, then either the column is not
      // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
      // for 2.0, this cannot determine whether a page fell back without
      // page encoding stats
      return true;
    }
  }
}
