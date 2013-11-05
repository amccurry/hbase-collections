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

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public abstract class HTableFactory {

  private static HTablePool _pool = new HTablePool();

  public HTableInterface getInstance(String table) {
    return getInstance(Bytes.toBytes(table));
  }

  public abstract HTableInterface getInstance(byte[] table);

  public static HTableFactory instance() {
    return new HTableFactory() {

      @Override
      public HTableInterface getInstance(byte[] table) {
        return _pool.getTable(table);
      }
    };
  }

}