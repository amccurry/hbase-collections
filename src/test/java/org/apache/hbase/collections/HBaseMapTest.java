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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HBaseMapTest {

  private static HBaseTestingUtility _utility;
  private static HTable _table;

  @BeforeClass
  public static void setupOnce() throws Exception {
    _utility = new HBaseTestingUtility();
    _utility.startMiniCluster();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    _table.close();
    _utility.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    _table = _utility.createTable(Bytes.toBytes("t1"), Bytes.toBytes("map"));
  }

  @After
  public void tearDown() throws IOException {
    _utility.deleteTable(Bytes.toBytes("t1"));
  }

  @Test
  public void testPutGet() throws IOException {
    Configuration configuration = _utility.getConfiguration();
    ConcurrentMap<String, String> map = new HBaseMap<String, String>(configuration, "t1", "test1");
    assertEquals(0, map.size());
    String s1 = map.put("key", "value");
    assertNull(s1);
    assertEquals(1, map.size());
    String s2 = map.get("key");
    assertEquals("value", s2);
  }

  @Test
  public void testEntrySet() throws IOException {
    Configuration configuration = _utility.getConfiguration();
    ConcurrentMap<Integer, Integer> map = new HBaseMap<Integer, Integer>(configuration, "t1", "test1");
    assertEquals(0, map.size());
    for (int i = 0; i < 100; i++) {
      map.put(i, i);
    }
    assertEquals(100, map.size());
    Set<Entry<Integer, Integer>> entrySet = map.entrySet();
    int i = 0;
    for (Entry<Integer, Integer> e : entrySet) {
      Integer key = e.getKey();
      Integer value = e.getValue();
      assertEquals((Integer) i, key);
      assertEquals((Integer) i, value);
      i++;
    }
  }

  @Test
  public void testRemove() throws IOException {
    Configuration configuration = _utility.getConfiguration();
    ConcurrentMap<Integer, Integer> map = new HBaseMap<Integer, Integer>(configuration, "t1", "test1");
    assertNull(map.remove(10));
    assertEquals(0, map.size());
    assertNull(map.put(10, 1000));
    assertEquals(1, map.size());
    assertEquals((Integer) 1000, map.remove(10));
    assertEquals(0, map.size());
  }

}
