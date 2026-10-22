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
package org.apache.iceberg.spark;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** A local {@link HadoopFileIO} that counts prefix listings and opened streams per location. */
public class CountingFileIO implements SupportsPrefixOperations {

  private final HadoopFileIO delegate = new HadoopFileIO(new Configuration());
  private final AtomicInteger prefixListings = new AtomicInteger();
  private final Map<String, AtomicInteger> streamsByLocation = Maps.newConcurrentMap();

  public int prefixListings() {
    return prefixListings.get();
  }

  public int streamsOpened(String location) {
    AtomicInteger count = streamsByLocation.get(location);
    return count == null ? 0 : count.get();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new CountingInputFile(delegate.newInputFile(path));
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new CountingInputFile(delegate.newInputFile(path, length));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    prefixListings.incrementAndGet();
    return delegate.listPrefix(prefix);
  }

  @Override
  public void deletePrefix(String prefix) {
    delegate.deletePrefix(prefix);
  }

  private class CountingInputFile implements InputFile {
    private final InputFile file;

    private CountingInputFile(InputFile file) {
      this.file = file;
    }

    @Override
    public long getLength() {
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      streamsByLocation
          .computeIfAbsent(file.location(), l -> new AtomicInteger())
          .incrementAndGet();
      return file.newStream();
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public boolean exists() {
      return file.exists();
    }
  }
}
