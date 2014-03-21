/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Defines the switching strategy. Depending upon implementation, it takes the input parameters
 * (such as last N sync time, last switched time, etc), and decides whether the WAL should
 * switch or not.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WALSwitchPolicy {

  final static long AGGRESSIVE_DEFAULT_WAL_SWITCH_THRESHOLD = 2000; // 2 sec

  void init(Configuration conf);

  /**
   * Based on last sync op duration, it decides whether WAL should do a switch to another writer.
   * @param lastSyncTimeTaken
   * @return true if WAL is eligible for switching.
   */
  boolean makeAWALSwitch(long lastSyncTimeTaken);

  /**
   * @return the walSwitch threshold time.
   */
  long getWalSwitchThreshold();
}
