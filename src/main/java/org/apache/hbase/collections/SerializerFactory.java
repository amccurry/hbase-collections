/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hbase.collections;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class SerializerFactory<K, V> {

  private static final byte[] SEP = Bytes.toBytes("|");

  public byte[] toBytes(Object o) {
    if (o == null) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    byte[] buf;
    if (o instanceof Integer) {
      byte[] bs = Bytes.toBytes((Integer) o);
      buf = new byte[bs.length + 1];
      buf[0] = 1;
      System.arraycopy(bs, 0, buf, 1, bs.length);
      return buf;
    }

    if (o instanceof Long) {
      byte[] bs = Bytes.toBytes((Long) o);
      buf = new byte[bs.length + 1];
      buf[0] = 2;
      System.arraycopy(bs, 0, buf, 1, bs.length);
      return buf;
    }

    if (o instanceof Float) {
      byte[] bs = Bytes.toBytes((Float) o);
      buf = new byte[bs.length + 1];
      buf[0] = 3;
      System.arraycopy(bs, 0, buf, 1, bs.length);
      return buf;
    }

    if (o instanceof Double) {
      byte[] bs = Bytes.toBytes((Double) o);
      buf = new byte[bs.length + 1];
      buf[0] = 4;
      System.arraycopy(bs, 0, buf, 1, bs.length);
      return buf;
    }

    if (o instanceof String) {
      byte[] bs = Bytes.toBytes((String) o);
      buf = new byte[bs.length + 1];
      buf[0] = 5;
      System.arraycopy(bs, 0, buf, 1, bs.length);
      return buf;
    }

    throw new RuntimeException("Type [" + o + "] not supported.");
  }

  @SuppressWarnings("unchecked")
  private V toObject(byte[] bs, int offset, int length) {
    byte b = bs[offset];
    int off = offset + 1;
    switch (b) {
    case 0:
      return null;
    case 1:
      return (V) ((Integer) Bytes.toInt(bs, off));
    case 2:
      return (V) ((Long) Bytes.toLong(bs, off));
    case 3:
      return (V) ((Float) Bytes.toFloat(bs, off));
    case 4:
      return (V) ((Double) Bytes.toDouble(bs, off));
    case 5:
      return (V) ((String) Bytes.toString(bs, off, bs.length - off));
    default:
      throw new RuntimeException("Type [" + b + "] not supported.");
    }
  }

  @SuppressWarnings("unchecked")
  public K getKey(Result result, byte[] mapName, byte[] family, byte[] qualifier) {
    byte[] row = result.getRow();
    byte[] rowPrefix = getRowPrefix(mapName);
    int offset = rowPrefix.length;
    return (K) toObject(row, offset, row.length - offset);
  }

  public V getValue(Result result, byte[] mapName, byte[] family, byte[] qualifier) {
    if (result.getRow() == null) {
      return null;
    }
    byte[] value = result.getValue(family, qualifier);
    return toObject(value, 0, value.length);
  }

  public Get getGet(K key, byte[] mapName, byte[] family, byte[] qualifier) {
    return new Get(getRow(key, mapName)).addColumn(family, qualifier);
  }

  public Put getPut(K key, V value, byte[] mapName, byte[] family, byte[] qualifier) {
    return new Put(getRow(key, mapName)).add(family, qualifier, toBytes(value));
  }

  public Delete getDelete(K key, byte[] mapName, byte[] family, byte[] qualifier) {
    return new Delete(getRow(key, mapName)).deleteColumn(family, qualifier);
  }

  private byte[] getRow(K key, byte[] mapName) {
    byte[] bs = toBytes(key);
    return Bytes.add(mapName, SEP, bs);
  }

  public byte[] getRowPrefix(byte[] mapName) {
    return Bytes.add(mapName, SEP);
  }

}
