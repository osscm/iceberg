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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.iceberg.index.LeafFileEntry;
import org.apache.iceberg.index.LeafFileReader;
import org.apache.iceberg.index.LeafFileWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.spark.CountingFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFooterCachingInputFile {

  private static final Types.NestedField KEY = required(1, "id", Types.LongType.get());
  private static final int ENTRY_COUNT = 5_000;

  @TempDir private File dir;

  private final CountingFileIO io = new CountingFileIO();
  private final Cache<String, byte[]> tails = Caffeine.newBuilder().build();
  private String parquetLocation;
  private byte[] parquetBytes;

  @BeforeEach
  void writeLeafFile() throws IOException {
    File file = new File(dir, "leaf.parquet");
    this.parquetLocation = file.toURI().toString();
    try (LeafFileWriter writer =
        new LeafFileWriter(io.newOutputFile(parquetLocation), KEY, 16 * 1024)) {
      for (long id = 0; id < ENTRY_COUNT; id += 1) {
        writer.add(
            LeafFileEntry.builder()
                .keyValue(id)
                .transformValue(id)
                .filePath("data-" + (id % 7) + ".parquet")
                .position(id)
                .build());
      }
    }

    this.parquetBytes = Files.readAllBytes(file.toPath());
  }

  private InputFile cachingFile(String location) {
    return new FooterCachingInputFile(io.newInputFile(location), tails);
  }

  private int tailLength() {
    int footerLength =
        ByteBuffer.wrap(parquetBytes, parquetBytes.length - 8, 4)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt();
    return footerLength + 8;
  }

  @Test
  void sequentialReadsMatchRawFile() throws IOException {
    for (int bufferSize : new int[] {1, 7, 13, 1000, 4099, parquetBytes.length + 5}) {
      assertThat(readSequentially(cachingFile(parquetLocation), bufferSize))
          .as("buffer size %s", bufferSize)
          .isEqualTo(parquetBytes);
    }
  }

  @Test
  void seeksAndSingleByteReadsMatchRawFile() throws IOException {
    int tailStart = parquetBytes.length - tailLength();
    try (SeekableInputStream stream = cachingFile(parquetLocation).newStream()) {
      for (int pos : new int[] {0, tailStart - 1, tailStart, parquetBytes.length - 1, 3}) {
        stream.seek(pos);
        assertThat(stream.read()).as("byte at %s", pos).isEqualTo(parquetBytes[pos] & 0xFF);
        assertThat(stream.getPos()).isEqualTo(pos + 1);
      }

      stream.seek(parquetBytes.length);
      assertThat(stream.read()).isEqualTo(-1);
    }
  }

  @Test
  void positionedReadsStraddlingTheTailMatchRawFile() throws IOException {
    int tailStart = parquetBytes.length - tailLength();
    try (SeekableInputStream stream = cachingFile(parquetLocation).newStream()) {
      RangeReadable ranges = (RangeReadable) stream;
      for (int[] range :
          new int[][] {
            {tailStart - 10, 20}, {tailStart - 1, 2}, {0, parquetBytes.length}, {tailStart, 8}
          }) {
        byte[] buffer = new byte[range[1]];
        ranges.readFully(range[0], buffer);
        assertThat(buffer)
            .as("range at %s", range[0])
            .isEqualTo(Arrays.copyOfRange(parquetBytes, range[0], range[0] + range[1]));
      }

      byte[] tail = new byte[tailLength() + 3];
      assertThat(ranges.readTail(tail, 0, tail.length)).isEqualTo(tail.length);
      assertThat(tail)
          .isEqualTo(
              Arrays.copyOfRange(
                  parquetBytes, parquetBytes.length - tail.length, parquetBytes.length));
      assertThat(stream.getPos()).isZero();
    }
  }

  @Test
  void secondOpenServesTheTailWithoutOpeningTheFile() throws IOException {
    try (SeekableInputStream stream = cachingFile(parquetLocation).newStream()) {
      stream.seek(parquetBytes.length - 8);
      stream.read(new byte[8]);
    }

    assertThat(io.streamsOpened(parquetLocation)).isEqualTo(1);

    byte[] tail = new byte[tailLength()];
    try (SeekableInputStream stream = cachingFile(parquetLocation).newStream()) {
      stream.seek(parquetBytes.length - tail.length);
      assertThat(stream.read(tail)).isEqualTo(tail.length);
    }

    assertThat(io.streamsOpened(parquetLocation)).isEqualTo(1);
    assertThat(tail)
        .isEqualTo(
            Arrays.copyOfRange(
                parquetBytes, parquetBytes.length - tail.length, parquetBytes.length));
  }

  @Test
  void leafReadsThroughTheCacheMatchDirectReads() {
    List<LeafFileEntry> direct = LeafFileReader.readAll(io.newInputFile(parquetLocation), KEY);
    LeafFileReader.readAll(cachingFile(parquetLocation), KEY);
    List<LeafFileEntry> cached = LeafFileReader.readAll(cachingFile(parquetLocation), KEY);

    assertThat(cached).hasSize(ENTRY_COUNT);
    assertThat(cached)
        .extracting(LeafFileEntry::position)
        .isEqualTo(direct.stream().map(LeafFileEntry::position).collect(Collectors.toList()));
    assertThat(LeafFileReader.storedKeyType(cachingFile(parquetLocation), KEY))
        .isEqualTo(Types.LongType.get());
  }

  @Test
  void nonParquetFilePassesThrough() throws IOException {
    byte[] bytes = new byte[10_000];
    new Random(42).nextBytes(bytes);
    String location = write("data.bin", bytes);

    assertThat(readSequentially(cachingFile(location), 13)).isEqualTo(bytes);
    assertThat(readSequentially(cachingFile(location), 4096)).isEqualTo(bytes);
    assertThat(tails.getIfPresent(location)).isEmpty();
    // The first open probes the tail and hands its stream to the caller; later opens skip the
    // probe.
    assertThat(io.streamsOpened(location)).isEqualTo(2);
  }

  @Test
  void footerOverTheSizeLimitPassesThrough() throws IOException {
    int footerLength = FooterCachingInputFile.MAX_FOOTER_SIZE_BYTES + 1;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
    out.write(new byte[footerLength]);
    out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerLength).array());
    out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
    byte[] bytes = out.toByteArray();
    String location = write("large-footer.parquet", bytes);

    assertThat(readSequentially(cachingFile(location), 65_536)).isEqualTo(bytes);
    assertThat(tails.getIfPresent(location)).isEmpty();
  }

  private String write(String name, byte[] bytes) throws IOException {
    File file = new File(dir, name);
    Files.write(file.toPath(), bytes);
    return file.toURI().toString();
  }

  private static byte[] readSequentially(InputFile file, int bufferSize) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[bufferSize];
    try (SeekableInputStream stream = file.newStream()) {
      int bytesRead;
      while ((bytesRead = stream.read(buffer, 0, buffer.length)) >= 0) {
        out.write(buffer, 0, bytesRead);
      }
    }

    return out.toByteArray();
  }
}
