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

import java.util.Map;
import java.util.Map.Entry;

public class UsingMap {

  public static void main(String[] args) {
    Map<String, String> map = new HBaseMap<String, String>("t1", "m1");
    map.put("k", "v");
    System.out.println(map.get("k"));

    Map<Integer, Integer> map2 = new HBaseMap<Integer, Integer>("t1", "m2");
    for (int i = 0; i < 10; i++) {
      map2.put(i, i);
    }

    for (int i = 0; i < 10; i++) {
      System.out.println(map2.get(i));
    }
    
    for (Entry<Integer,Integer> e : map2.entrySet()) {
      System.out.println(e);
    }
  }
}
