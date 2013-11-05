package org.apache.hbase.collections;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {

  private static final byte[] DEFAULT_FAMILY = Bytes.toBytes("map");
  private static final byte[] DEFAULT_SIZE = HConstants.EMPTY_BYTE_ARRAY;
  private static final byte[] DEFAULT_QUAL = HConstants.EMPTY_BYTE_ARRAY;

  private final HTableFactory _tableFactory;
  private SerializerFactory<K, V> _serializerFactory = new SerializerFactory<K, V>();
  private byte[] _table;
  private byte[] _family;
  private byte[] _defaultQualifier;
  private byte[] _mapName;
  private byte[] _sizeQualifier;

  static abstract class Command<T> {
    abstract T execute(HTableInterface table) throws IOException;
  }

  public HBaseMap(String table, String mapName) {
    this(Bytes.toBytes(table), Bytes.toBytes(mapName));
  }

  public HBaseMap(Configuration configuration, String table, String mapName) {
    this(configuration, Bytes.toBytes(table), Bytes.toBytes(mapName));
  }

  public HBaseMap(byte[] table, byte[] mapName) {
    this(table, DEFAULT_FAMILY, mapName, DEFAULT_QUAL, DEFAULT_SIZE);
  }

  public HBaseMap(Configuration configuration, byte[] table, byte[] mapName) {
    this(configuration, table, DEFAULT_FAMILY, mapName, DEFAULT_QUAL, DEFAULT_SIZE);
  }

  public HBaseMap(byte[] table, byte[] family, byte[] mapName, byte[] defaultQualifier, byte[] sizeQualifier) {
    this(HBaseConfiguration.create(), table, family, mapName, defaultQualifier, sizeQualifier);
  }

  public HBaseMap(Configuration configuration, byte[] table, byte[] family, byte[] mapName, byte[] defaultQualifier,
      byte[] sizeQualifier) {
    _table = table;
    _family = family;
    _mapName = mapName;
    _defaultQualifier = defaultQualifier;
    _sizeQualifier = sizeQualifier;
    _tableFactory = HTableFactory.instance(configuration);
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    return new AbstractSet<Map.Entry<K, V>>() {

      @Override
      public Iterator<java.util.Map.Entry<K, V>> iterator() {
        final HTableInterface htable = _tableFactory.getInstance(_table);
        final ResultScanner scanner;
        try {
          Scan scan = new Scan();
          scan.addFamily(_family);
          byte[] rowPrefix = _serializerFactory.getRowPrefix(_mapName);
          scan.setStartRow(rowPrefix);
          Filter filter = new PrefixFilter(rowPrefix);
          scan.setFilter(filter);
          scanner = htable.getScanner(scan);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        final Iterator<Result> iterator = scanner.iterator();
        return new Iterator<Map.Entry<K, V>>() {

          private K _key;
          private V _value;

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public java.util.Map.Entry<K, V> next() {
            Result result = iterator.next();
            _key = getKey(result);
            _value = getValue(result);
            return new Entry<K, V>() {

              @Override
              public K getKey() {
                return _key;
              }

              @Override
              public V getValue() {
                return _value;
              }

              @Override
              public V setValue(V value) {
                return HBaseMap.this.put(_key, value);
              }

              @Override
              public String toString() {
                return "[\"" + _key + "\",\"" + _value + "\"]";
              }

            };
          }

          @Override
          public void remove() {
            HBaseMap.this.remove(_key);
          }

          @Override
          protected void finalize() throws Throwable {
            scanner.close();
            htable.close();
          }

        };
      }

      @Override
      public int size() {
        return HBaseMap.this.size();
      }
    };
  }

  @Override
  public int size() {
    long sizeActual = sizeActual();
    if (sizeActual > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) sizeActual;
  }

  public long sizeActual() {
    return execute(new Command<Long>() {
      @Override
      Long execute(HTableInterface table) throws IOException {
        Get get = new Get(_mapName).addColumn(_family, _sizeQualifier);
        Result result = table.get(get);
        if (result.getRow() == null) {
          return 0L;
        }
        byte[] value = result.getValue(_family, _sizeQualifier);
        return Bytes.toLong(value);
      }
    });
  }

  @Override
  public boolean containsKey(Object key) {
    V v = get(key);
    if (v == null) {
      return false;
    } else {
      return true;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object k) {
    final K key = (K) k;
    return execute(new Command<V>() {
      @Override
      V execute(HTableInterface table) throws IOException {
        Get get = getGet(key);
        Delete delete = getDelete(key);
        while (true) {
          Result result = table.get(get);
          byte[] val;
          long sizeChange;
          if (result.getRow() == null) {
            return null;
          } else {
            val = result.getValue(_family, _defaultQualifier);
            sizeChange = -1;
          }
          boolean checkAndDelete = table.checkAndDelete(get.getRow(), _family, _defaultQualifier, val, delete);
          if (checkAndDelete) {
            updateSize(sizeChange);
            return getValue(result);
          }
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public V get(final Object k) {
    final K key = (K) k;
    return execute(new Command<V>() {
      @Override
      V execute(HTableInterface table) throws IOException {
        Get get = getGet(key);
        Result result = table.get(get);
        return getValue(result);
      }
    });
  }

  @Override
  public V put(final K key, final V value) {
    return execute(new Command<V>() {
      @Override
      V execute(HTableInterface table) throws IOException {
        Get get = getGet(key);
        Put put = getPut(key, value);
        while (true) {
          Result result = table.get(get);
          byte[] val;
          long sizeChange;
          if (result.getRow() == null) {
            val = null;
            sizeChange = 1;
          } else {
            val = result.getValue(_family, _defaultQualifier);
            sizeChange = 0;
          }
          boolean checkAndPut = table.checkAndPut(get.getRow(), _family, _defaultQualifier, val, put);
          if (checkAndPut) {
            updateSize(sizeChange);
            return getValue(result);
          }
        }
      }
    });
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    return execute(new Command<V>() {
      @Override
      V execute(HTableInterface table) throws IOException {
        Get get = getGet(key);
        Put put = getPut(key, value);
        byte[] val = null;
        boolean checkAndPut = table.checkAndPut(get.getRow(), _family, _defaultQualifier, val, put);
        if (checkAndPut) {
          updateSize(1l);
          return null;
        } else {
          return getValue(table.get(get));
        }
      }
    });
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    return execute(new Command<Boolean>() {
      @SuppressWarnings("unchecked")
      @Override
      Boolean execute(HTableInterface table) throws IOException {
        byte[] val = _serializerFactory.toBytes(value);
        Delete delete = getDelete((K) key);
        boolean checkAndDelete = table.checkAndDelete(delete.getRow(), _family, _defaultQualifier, val, delete);
        if (checkAndDelete) {
          updateSize(-1l);
        }
        return checkAndDelete;
      }
    });
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    return execute(new Command<Boolean>() {
      @Override
      Boolean execute(HTableInterface table) throws IOException {
        Put put = getPut(key, newValue);
        byte[] val = _serializerFactory.toBytes(oldValue);
        return table.checkAndPut(put.getRow(), _family, _defaultQualifier, val, put);
      }
    });
  }

  @Override
  public V replace(final K key, final V value) {
    return execute(new Command<V>() {
      @Override
      V execute(HTableInterface table) throws IOException {
        Get get = getGet(key);
        Put put = getPut(key, value);
        while (true) {
          Result result = table.get(get);
          byte[] val;
          if (result.getRow() == null) {
            return null;
          } else {
            val = result.getValue(_family, _defaultQualifier);
          }
          boolean checkAndPut = table.checkAndPut(get.getRow(), _family, _defaultQualifier, val, put);
          if (checkAndPut) {
            return getValue(result);
          }
        }
      }
    });
  }

  protected void updateSize(final long sizeChange) {
    execute(new Command<Void>() {
      @Override
      Void execute(HTableInterface table) throws IOException {
        table.incrementColumnValue(_mapName, _family, _sizeQualifier, sizeChange);
        return null;
      }
    });
  }

  protected K getKey(Result result) {
    return _serializerFactory.getKey(result, _mapName, _family, _defaultQualifier);
  }

  protected V getValue(Result result) {
    return _serializerFactory.getValue(result, _mapName, _family, _defaultQualifier);
  }

  protected Get getGet(K key) {
    return _serializerFactory.getGet(key, _mapName, _family, _defaultQualifier);
  }

  protected Put getPut(K key, V value) {
    return _serializerFactory.getPut(key, value, _mapName, _family, _defaultQualifier);
  }

  protected Delete getDelete(K key) {
    return _serializerFactory.getDelete(key, _mapName, _family, _defaultQualifier);
  }

  private <T> T execute(Command<T> command) {
    HTableInterface table = null;
    try {
      table = _tableFactory.getInstance(_table);
      return command.execute(table);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(table);
    }
  }

}
