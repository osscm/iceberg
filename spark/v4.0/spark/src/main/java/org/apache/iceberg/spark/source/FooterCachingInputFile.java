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

import com.github.benmanes.caffeine.cache.Cache;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * An {@link InputFile} for an immutable Parquet file that reads the file's tail (the footer, its
 * 4-byte little-endian length, and the {@code PAR1} magic) once and serves later tail reads from a
 * shared cache. Reads before the tail go to the underlying file, which is only opened when needed.
 *
 * <p>Files that are not Parquet, or whose footer is larger than the size limit, are read through
 * unchanged.
 */
class FooterCachingInputFile implements InputFile {

  static final int MAX_FOOTER_SIZE_BYTES = 4 * 1024 * 1024;

  private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
  private static final int FOOTER_LENGTH_SIZE = 4;
  private static final int TRAILER_SIZE = FOOTER_LENGTH_SIZE + MAGIC.length;
  private static final byte[] NOT_CACHEABLE = new byte[0];

  private final InputFile delegate;
  private final Cache<String, byte[]> tails;

  FooterCachingInputFile(InputFile delegate, Cache<String, byte[]> tails) {
    Preconditions.checkArgument(delegate != null, "Invalid input file: null");
    Preconditions.checkArgument(tails != null, "Invalid tail cache: null");
    this.delegate = delegate;
    this.tails = tails;
  }

  @Override
  public long getLength() {
    return delegate.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    byte[] tail = tails.getIfPresent(location());
    if (tail == NOT_CACHEABLE) {
      return delegate.newStream();
    }

    if (tail != null) {
      return new TailCachingStream(delegate, getLength(), tail, null);
    }

    SeekableInputStream stream = delegate.newStream();
    try {
      tail = readTail(stream, getLength());
      if (tail == NOT_CACHEABLE) {
        // The probe moved the stream; it is handed to the caller as a plain stream.
        stream.seek(0L);
      }
    } catch (IOException e) {
      closeQuietly(stream, e);
      throw new UncheckedIOException("Failed to read the tail of " + location(), e);
    } catch (RuntimeException e) {
      closeQuietly(stream, e);
      throw e;
    }

    tails.put(location(), tail);
    return tail == NOT_CACHEABLE
        ? stream
        : new TailCachingStream(delegate, getLength(), tail, stream);
  }

  @Override
  public String location() {
    return delegate.location();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  /** Reads the footer, footer length, and magic, or returns {@link #NOT_CACHEABLE}. */
  private static byte[] readTail(SeekableInputStream stream, long length) throws IOException {
    if (length < TRAILER_SIZE + MAGIC.length) {
      return NOT_CACHEABLE;
    }

    byte[] trailer = new byte[TRAILER_SIZE];
    stream.seek(length - TRAILER_SIZE);
    IOUtil.readFully(stream, trailer, 0, TRAILER_SIZE);
    if (!Arrays.equals(MAGIC, Arrays.copyOfRange(trailer, FOOTER_LENGTH_SIZE, TRAILER_SIZE))) {
      return NOT_CACHEABLE;
    }

    int footerLength =
        ByteBuffer.wrap(trailer, 0, FOOTER_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN).getInt();
    if (footerLength < 0
        || footerLength > MAX_FOOTER_SIZE_BYTES
        || footerLength > length - TRAILER_SIZE - MAGIC.length) {
      return NOT_CACHEABLE;
    }

    byte[] tail = new byte[footerLength + TRAILER_SIZE];
    stream.seek(length - tail.length);
    IOUtil.readFully(stream, tail, 0, footerLength);
    System.arraycopy(trailer, 0, tail, footerLength, TRAILER_SIZE);
    return tail;
  }

  private static void closeQuietly(SeekableInputStream stream, Exception failure) {
    try {
      stream.close();
    } catch (IOException e) {
      failure.addSuppressed(e);
    }
  }

  /** Serves reads in the cached tail from memory and all other reads from the file. */
  private static class TailCachingStream extends SeekableInputStream implements RangeReadable {
    private final InputFile file;
    private final long length;
    private final byte[] tail;
    private final long tailStart;
    private SeekableInputStream fileStream;
    private long pos = 0L;
    private boolean closed = false;

    private TailCachingStream(
        InputFile file, long length, byte[] tail, SeekableInputStream fileStream) {
      this.file = file;
      this.length = length;
      this.tail = tail;
      this.tailStart = length - tail.length;
      this.fileStream = fileStream;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      checkOpen();
      if (newPos < 0) {
        throw new EOFException("Cannot seek to negative position: " + newPos);
      }

      this.pos = newPos;
    }

    @Override
    public int read() throws IOException {
      byte[] single = new byte[1];
      int bytesRead = read(single, 0, 1);
      return bytesRead < 0 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      checkOpen();
      Preconditions.checkPositionIndexes(offset, offset + len, buffer.length);
      if (len == 0) {
        return 0;
      }

      if (pos >= length) {
        return -1;
      }

      int bytesRead =
          pos >= tailStart ? readFromTail(buffer, offset, len) : readFromFile(buffer, offset, len);
      if (bytesRead > 0) {
        this.pos += bytesRead;
      }

      return bytesRead;
    }

    private int readFromTail(byte[] buffer, int offset, int len) {
      int tailOffset = (int) (pos - tailStart);
      int bytesToCopy = Math.min(len, tail.length - tailOffset);
      System.arraycopy(tail, tailOffset, buffer, offset, bytesToCopy);
      return bytesToCopy;
    }

    private int readFromFile(byte[] buffer, int offset, int len) throws IOException {
      // Stops at the tail boundary; the caller's next read is served from memory.
      int bytesToRead = (int) Math.min(len, tailStart - pos);
      SeekableInputStream stream = fileStream();
      if (stream.getPos() != pos) {
        stream.seek(pos);
      }

      return stream.read(buffer, offset, bytesToRead);
    }

    private SeekableInputStream fileStream() {
      if (fileStream == null) {
        this.fileStream = file.newStream();
      }

      return fileStream;
    }

    private void checkOpen() throws IOException {
      if (closed) {
        throw new IOException("Stream is closed: " + file.location());
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int len) throws IOException {
      checkOpen();
      Preconditions.checkPositionIndexes(offset, offset + len, buffer.length);
      if (position < 0 || position + len > length) {
        throw new EOFException(
            String.format(
                Locale.ROOT,
                "Cannot read %s bytes at position %s from %s of length %s",
                len,
                position,
                file.location(),
                length));
      }

      long savedPos = pos;
      try {
        this.pos = position;
        IOUtil.readFully(this, buffer, offset, len);
      } finally {
        this.pos = savedPos;
      }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int len) throws IOException {
      int bytesToRead = (int) Math.min(len, length);
      readFully(length - bytesToRead, buffer, offset, bytesToRead);
      return bytesToRead;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        this.closed = true;
        if (fileStream != null) {
          fileStream.close();
        }
      }
    }
  }
}
