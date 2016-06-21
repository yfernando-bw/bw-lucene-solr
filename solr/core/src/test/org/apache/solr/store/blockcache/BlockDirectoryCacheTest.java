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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class BlockDirectoryCacheTest extends LuceneTestCase {

  private static final String INDEX_PATH_CORE_NODE_1 = "hdfs://machine/solr/collection/core_node1/data/index";
  private static final String INDEX_PATH_CORE_NODE_2 = "hdfs://machine/solr/collection/core_node2/data/index";
  private static final byte[] BLOCK_1 = new byte[] { (byte)0xDE, (byte)0xAD };
  private static final byte[] BLOCK_2 = new byte[] { (byte)0xAD, (byte)0xDE };

  @Test
  public void testReleaseWhenBlockCacheNotGlobal() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    BlockDirectoryCache blockDirectoryCache = new BlockDirectoryCache(blockCache, INDEX_PATH_CORE_NODE_1, new Metrics(), false);
    blockDirectoryCache.update("file1", 1L, 0, BLOCK_1, 0, BLOCK_1.length);

    assertTrue(blockCache.getSize() == 1);

    blockDirectoryCache.releaseResources();

    assertTrue(blockCache.getSize() == 1);
  }

  @Test
  public void testReleaseWhenOneBlockDirectoryCacheWithBlockCacheGlobal() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    BlockDirectoryCache blockDirectoryCache = new BlockDirectoryCache(blockCache, INDEX_PATH_CORE_NODE_1, new Metrics(), true);
    blockDirectoryCache.update("file1", 1L, 0, BLOCK_1, 0, BLOCK_1.length);

    assertTrue(blockCache.getSize() == 1);

    blockDirectoryCache.releaseResources();

    assertTrue(blockCache.getSize() == 0);
  }

  @Test
  public void testReleaseWhenTwoBlockDirectoryCachesWithBlockCacheGlobal() {
    BlockCache blockCache = new BlockCache(new Metrics(), true, 2 * BlockCache._128M);

    BlockDirectoryCache blockDirectoryCache1 = new BlockDirectoryCache(blockCache, INDEX_PATH_CORE_NODE_1, new Metrics(), true);
    blockDirectoryCache1.update("file1", 1L, 0, BLOCK_1, 0, BLOCK_1.length);
    BlockDirectoryCache blockDirectoryCache2 = new BlockDirectoryCache(blockCache, INDEX_PATH_CORE_NODE_2, new Metrics(), true);
    blockDirectoryCache2.update("file2", 1L, 0, BLOCK_2, 0, BLOCK_2.length);

    assertTrue(blockCache.getSize() == 2);

    blockDirectoryCache1.releaseResources();

    assertTrue(blockCache.getSize() == 1);
    assertTrue(blockDirectoryCache2.fetch("file2", 1L, 0, new byte[BLOCK_2.length], 0, BLOCK_2.length));
  }
}
