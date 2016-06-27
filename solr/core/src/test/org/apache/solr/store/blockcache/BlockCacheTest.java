/*
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
package org.apache.solr.store.blockcache;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class BlockCacheTest extends LuceneTestCase {

  private static final String INDEX_PATH_CORE_NODE_1 = "hdfs://machine/solr/collection/core_node1/data/index";
  private static final String INDEX_PATH_CORE_NODE_2 = "hdfs://machine/solr/collection/core_node2/data/index";
  private static final byte[] BLOCK_1 = new byte[] { (byte)0xDE, (byte)0xAD };
  private static final byte[] BLOCK_2 = new byte[] { (byte)0xAD, (byte)0xDE };

  @Test
  public void testBlockCache() {
    int blocksInTest = 2000000;
    int blockSize = 1024;
    
    int slabSize = blockSize * 4096;
    long totalMemory = 2 * slabSize;
    
    BlockCache blockCache = new BlockCache(new Metrics(), true, totalMemory, slabSize, blockSize);
    byte[] buffer = new byte[1024];
    Random random = random();
    byte[] newData = new byte[blockSize];
    AtomicLong hitsInCache = new AtomicLong();
    AtomicLong missesInCache = new AtomicLong();
    long storeTime = 0;
    long fetchTime = 0;
    int passes = 10000;

    BlockCacheKey blockCacheKey = new BlockCacheKey();

    for (int j = 0; j < passes; j++) {
      long block = random.nextInt(blocksInTest);
      int file = 0;
      blockCacheKey.setBlock(block);
      blockCacheKey.setFile(file);
      blockCacheKey.setPath("/");

      if (blockCache.fetch(blockCacheKey, buffer)) {
        hitsInCache.incrementAndGet();
      } else {
        missesInCache.incrementAndGet();
      }

      byte[] testData = testData(random, blockSize, newData);
      long t1 = System.nanoTime();
      blockCache.store(blockCacheKey, 0, testData, 0, blockSize);
      storeTime += (System.nanoTime() - t1);

      long t3 = System.nanoTime();
      if (blockCache.fetch(blockCacheKey, buffer)) {
        fetchTime += (System.nanoTime() - t3);
        assertTrue(Arrays.equals(testData, buffer));
      }
    }
    System.out.println("Cache Hits    = " + hitsInCache.get());
    System.out.println("Cache Misses  = " + missesInCache.get());
    System.out.println("Store         = " + (storeTime / (double) passes) / 1000000.0);
    System.out.println("Fetch         = " + (fetchTime / (double) passes) / 1000000.0);
    System.out.println("# of Elements = " + blockCache.getSize());
  }

  /**
   * Verify checking of buffer size limits against the cached block size.
   */
  @Test
  public void testLongBuffer() {
    Random random = random();
    int blockSize = BlockCache._32K;
    int slabSize = blockSize * 1024;
    long totalMemory = 2 * slabSize;

    BlockCache blockCache = new BlockCache(new Metrics(), true, totalMemory, slabSize);
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(0);
    blockCacheKey.setFile(0);
    blockCacheKey.setPath("/");
    byte[] newData = new byte[blockSize*3];
    byte[] testData = testData(random, blockSize, newData);

    assertTrue(blockCache.store(blockCacheKey, 0, testData, 0, blockSize));
    assertTrue(blockCache.store(blockCacheKey, 0, testData, blockSize, blockSize));
    assertTrue(blockCache.store(blockCacheKey, 0, testData, blockSize*2, blockSize));

    assertTrue(blockCache.store(blockCacheKey, 1, testData, 0, blockSize - 1));
    assertTrue(blockCache.store(blockCacheKey, 1, testData, blockSize, blockSize - 1));
    assertTrue(blockCache.store(blockCacheKey, 1, testData, blockSize*2, blockSize - 1));
  }

  /**
   * Verify removing by path is a no-op on empty cache, and does not throw an exception
   */
  @Test
  public void testReleaseByPathWhenEmptyCache() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    assertTrue(blockCache.getSize() == 0);

    blockCache.releaseByPath(INDEX_PATH_CORE_NODE_1);

    assertTrue(blockCache.getSize() == 0);
  }

  /**
   * Verify removing by path is a no-op on cache without keys from that path
   */
  @Test
  public void testReleaseByPathWhenCacheDoesNotContainKeysWithPath() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    BlockCacheKey key = new BlockCacheKey();
    key.setPath(INDEX_PATH_CORE_NODE_1);
    key.setFile(1);
    key.setBlock(1L);

    blockCache.store(key, 0, BLOCK_1, 0, BLOCK_1.length);

    assertTrue(blockCache.getSize() == 1);

    blockCache.releaseByPath(INDEX_PATH_CORE_NODE_2);

    assertTrue(blockCache.getSize() == 1);
  }

  /**
   * Verify removing by path removes keys from that path
   */
  @Test
  public void testReleaseByPathWhenCacheDoesContainKeysWithPath() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    BlockCacheKey key = new BlockCacheKey();
    key.setPath(INDEX_PATH_CORE_NODE_1);
    key.setFile(1);
    key.setBlock(1L);
    blockCache.store(key, 0, BLOCK_1, 0, BLOCK_1.length);

    assertTrue(blockCache.getSize() == 1);

    blockCache.releaseByPath(INDEX_PATH_CORE_NODE_1);

    assertTrue(blockCache.getSize() == 0);
  }

  /**
   * Verify removing by path removes keys only from that path
   */
  @Test
  public void testReleaseByPathWhenCacheContainKeysWithMixedPaths() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    BlockCacheKey key1 = new BlockCacheKey();
    key1.setPath(INDEX_PATH_CORE_NODE_1);
    key1.setFile(1);
    key1.setBlock(1L);
    blockCache.store(key1, 0, BLOCK_1, 0, BLOCK_1.length);

    BlockCacheKey key2 = new BlockCacheKey();
    key2.setPath(INDEX_PATH_CORE_NODE_2);
    key2.setFile(1);
    key2.setBlock(1L);
    blockCache.store(key2, 0, BLOCK_2, 0, BLOCK_2.length);

    assertTrue(blockCache.getSize() == 2);

    blockCache.releaseByPath(INDEX_PATH_CORE_NODE_1);

    assertTrue(blockCache.getSize() == 1);
    assertTrue(blockCache.fetch(key2, new byte[BlockCache._32K]));
  }

  private static byte[] testData(Random random, int size, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }
}
