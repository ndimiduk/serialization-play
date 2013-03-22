package types;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

import util.HSerializer;

/**
 * Fixed-width Serializer for HBase LONG values.
 * <p>
 * The HBase LONG is an 8-byte, signed, two's complement integer stored in
 * Big-Endian byte order.
 * </p>
 * <p>
 * Serialization is performed by inverting the long sign bit and writing the
 * value to a byte array in big endian order. For DESCENDING sort order, the
 * encoded value is logically inverted.
 * </p>
 */
public class LONG extends HSerializer<Long> {

  static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  @Override
  public byte[] toBytes(Long val) {
    return toBytes(val, order);
  }

  @Override
  public void putBytes(ByteBuffer buff, Long val) {
    putBytes(buff, val, order);
  }

  @Override
  public Long fromBytes(byte[] bytes) {
    // TODO(perf): should we manage our own Long instance cache?
    return Long.valueOf(toLong(bytes, 0, SIZEOF_LONG, order));
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static long toLong(final byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG, DEFAULT_ORDER);
  }

  public static long toLong(final byte[] bytes, final Order order) {
    return toLong(bytes, 0, SIZEOF_LONG, order);
  }

  public static long toLong(final byte[] bytes, final int offset) {
    return toLong(bytes, offset, SIZEOF_LONG, DEFAULT_ORDER);
  }

  public static long toLong(final byte[] bytes, final int offset, final Order order) {
    return toLong(bytes, offset, SIZEOF_LONG, order);
  }

  public static long toLong(final byte[] bytes, final int offset, final int length, Order order) {
    long l = Bytes.toLong(bytes, offset, length);
    return l ^ Long.MIN_VALUE ^ order.mask;
  }

  public static byte[] toBytes(long val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(long val, Order order) {
    val = val ^ Long.MIN_VALUE ^ order.mask;
    return Bytes.toBytes(val);
  }

  public static void putBytes(final ByteBuffer buff, long val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, long val, Order order) {
    Bytes.putLong(buff.array(), buff.arrayOffset() + buff.position(),
      val ^ Long.MIN_VALUE ^ order.mask);
    buff.position(buff.position() + SIZEOF_LONG);
  }
}
