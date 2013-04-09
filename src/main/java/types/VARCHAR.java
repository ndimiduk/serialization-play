package types;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import util.HSerializer;

/**
 * Serializer for HBase VARCHAR values.
 * <p>
 * The HBase VARCHAR is an arbitrary number of unicode characters.
 * </p>
 * <p>
 * Serialization is performed by writing the encoded the String as UTF-8, in
 * big endian byte order, followed by a (null) termination byte. For
 * DESCENDING sort order, the encoded value is logically inverted.
 * </p>
 */
public class VARCHAR extends HSerializer<String> {

  static final Charset UTF8 = Charset.forName("UTF-8");

  static final byte NULL = (byte) 0x00;
  static final byte TERM = (byte) 0x01;

  public VARCHAR() { super(); }
  public VARCHAR(Order order) { super(order); }

  @Override
  public byte[] toBytes(String val) {
    return toBytes(val, order);
  }

  @Override
  public void putBytes(ByteBuffer buff, String val) {
    putBytes(buff, val, order);
  }

  @Override
  public String fromBytes(byte[] bytes) {
    return toString(bytes, 0, order);
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  // TODO: refactor {to,put}Bytes M-w, C-y, a la LONG raw helpers.

  public static byte[] toBytes(String val, Order order) {
    if (null == val) {
      return new byte[] { (byte) (NULL ^ order.mask()), TERM };
    }

    // TODO: reimplement with fewer allocations/copies
    byte[] encoded = val.getBytes(UTF8);
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = (byte) (encoded[i] + 2 ^ order.mask());
    }
    byte[] ret = new byte[encoded.length + 1];
    ret[encoded.length] = TERM;
    System.arraycopy(encoded, 0, ret, 0, encoded.length);
    return ret;
  }

  public static byte[] putBytes(byte[] dst, int dstOffset, String val, Order order) {
    if (null == val) {
      assert dst.length >= dstOffset + 2;
      dst[dstOffset] = (byte) (NULL ^ order.mask());
      dst[dstOffset + 1] = TERM;
      return dst;
    }

    // TODO: reimplement with fewer allocations/copies
    byte[] encoded = val.getBytes(UTF8);
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = (byte) (encoded[i] + 2 ^ order.mask());
    }
    assert dst.length >= dstOffset + encoded.length + 1;
    dst[dstOffset + encoded.length] = TERM;
    System.arraycopy(encoded, 0, dst, dstOffset, encoded.length);
    return dst;
  }

  public static ByteBuffer putBytes(ByteBuffer buff, String val, Order order) {
    if (null == val) {
      assert buff.limit() >= buff.position() + 2;
      buff.put((byte) (NULL ^ order.mask()));
      return buff.put(TERM);
    }

    // TODO: reimplement with fewer allocations/copies
    byte[] encoded = val.getBytes(UTF8);
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = (byte) (encoded[i] + 2 ^ order.mask());
    }
    assert buff.limit() >= buff.position() + encoded.length + 1;
    System.arraycopy(encoded, 0, buff.array(), buff.position(), encoded.length);
    buff.position(buff.position() + encoded.length);
    return buff.put(TERM);
  }

  public static String toString(byte[] bytes) {
    return toString(bytes, 0, DEFAULT_ORDER);
  }

  public static String toString(byte[] bytes, int offset) {
    return toString(bytes, offset, DEFAULT_ORDER);
  }

  public static String toString(byte[] bytes, int offset, Order order) {
    if (bytes[offset] == TERM) return "";
    if ((bytes[offset] ^ order.mask()) == NULL && bytes[offset + 1] == TERM)
      return null;

    ByteBuffer decoded = ByteBuffer.allocate(bytes.length - offset - 1);
    for (int i = offset; i < bytes.length; i++) {
      if (TERM == bytes[i])
        break;
      decoded.put((byte) ((bytes[i] ^ order.mask()) - 2));
    }
    byte[] ret = new byte[decoded.position()];
    System.arraycopy(decoded.array(), 0, ret, 0, ret.length);
    return new String(ret, UTF8);
  }
}
