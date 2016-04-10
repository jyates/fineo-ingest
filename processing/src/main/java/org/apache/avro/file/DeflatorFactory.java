package org.apache.avro.file;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.tukaani.xz.LZMA2Options;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.avro.file.DeflatorConstants.*;

/**
 * Factory for a deflator stream around the usual output stream
 */
public abstract class DeflatorFactory {
  public abstract OutputStream deflate(OutputStream baos) throws IOException;

  public abstract InputStream inflate(InputStream in) throws IOException;

  protected abstract String getName();

  public static final int DEFAULT_DEFLATE_LEVEL = Deflater.DEFAULT_COMPRESSION;
  public static final int DEFAULT_XZ_LEVEL = LZMA2Options.PRESET_DEFAULT;

  public static DeflatorFactory gzip() {
    return new DeflatorFactory() {
      @Override
      public OutputStream deflate(OutputStream baos) throws IOException {
        return new GZIPOutputStream(baos);
      }

      @Override
      public InputStream inflate(InputStream in) throws IOException {
        return new GZIPInputStream(in);
      }

      @Override
      protected String getName() {
        return DeflatorConstants.GZIP_DEFLATOR;
      }
    };
  }

  public static DeflatorFactory none() {
    return new DeflatorFactory() {
      @Override
      public OutputStream deflate(OutputStream baos) throws IOException {
        return baos;
      }

      @Override
      public InputStream inflate(InputStream in) throws IOException {
        return in;
      }

      @Override
      protected String getName() {
        return NULL_DEFLATOR;
      }
    };
  }

  public static DeflatorFactory bzip() {
    return new DeflatorFactory() {
      @Override
      public OutputStream deflate(OutputStream baos) throws IOException {
        return new BZip2CompressorOutputStream(baos);
      }

      @Override
      public InputStream inflate(InputStream in) throws IOException {
        return new BZip2CompressorInputStream(in);
      }

      @Override
      protected String getName() {
        return BZIP2_DEFLATOR;
      }
    };
  }

  public static DeflatorFactory xz(int compressionLevel) {
    return new DeflatorFactory() {
      @Override
      public OutputStream deflate(OutputStream baos) throws IOException {
        return new XZCompressorOutputStream(baos, compressionLevel);
      }

      @Override
      public InputStream inflate(InputStream in) throws IOException {
        return new XZCompressorInputStream(in);
      }

      @Override
      protected String getName() {
        return XZ_DEFLATOR;
      }
    };
  }

  public static DeflatorFactory deflate(int compressionLevel) {
    return new DeflatorFactory() {
      @Override
      public OutputStream deflate(OutputStream baos) throws IOException {
        // see org.apache.avro.file.DeflateCodec for why we use nowrap where
        return new DeflaterOutputStream(baos, new Deflater(compressionLevel, true));
      }

      @Override
      public InputStream inflate(InputStream in) throws IOException {
        return new DeflaterInputStream(in);
      }

      @Override
      protected String getName() {
        return DEFLATE_DEFLATOR;
      }
    };
  }

  public enum DeflatorFactoryEnum {
    NULL(() -> none()),
    BZIP(() -> bzip()),
    DEFLATE(() -> deflate(DEFAULT_DEFLATE_LEVEL)),
    GZIP(() -> gzip()),
    XZ(() -> xz(DEFAULT_XZ_LEVEL));

    private final Supplier<DeflatorFactory> supplier;

    DeflatorFactoryEnum(Supplier<DeflatorFactory> supplier) {
      this.supplier = supplier;
    }

    public DeflatorFactory getFactory() {
      return supplier.get();
    }

    public static int ordinalOf(DeflatorFactory deflator) {
      if (deflator == null) {
        return NULL.ordinal();
      }
      switch (deflator.getName()) {
        case NULL_DEFLATOR:
          return NULL.ordinal();
        case DeflatorConstants.GZIP_DEFLATOR:
          return GZIP.ordinal();
        case BZIP2_DEFLATOR:
          return BZIP.ordinal();
        case DEFLATE_DEFLATOR:
          return DEFLATE.ordinal();
        case XZ_DEFLATOR:
          return XZ.ordinal();
        default:
          throw new UnsupportedOperationException("Known deflator factory: " + deflator);
      }
    }
  }
}
