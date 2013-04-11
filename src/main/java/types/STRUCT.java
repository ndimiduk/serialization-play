package types;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import util.HSerializer;

@SuppressWarnings("rawtypes")
public class STRUCT extends HSerializer<List<Object>> {

  private static final byte[] TRUE_ASC  = BOOLEAN.toBytes(true,  ASCENDING);
  private static final byte[] FALSE_ASC = BOOLEAN.toBytes(false, ASCENDING);
  private static final byte[] TRUE_DSC  = BOOLEAN.toBytes(true,  DESCENDING);
  private static final byte[] FALSE_DSC = BOOLEAN.toBytes(false, DESCENDING);

  protected List<HSerializer> schema;

  public STRUCT(List<HSerializer> schema) {
    super();
    this.schema = schema;
  }

  @Override
  public boolean supportsNull() { return true; }

  public STRUCT(List<HSerializer> schema, Order order) {
    // TODO: what does order mean to a struct?
    super(order);
    this.schema = schema;
  }

  public byte[] toBytes(List<Object> val) {
    List<byte[]> bytes = toBytes(order, schema, val);

    // populate a destination array
    int len = 0;
    for (byte[] b : bytes) { len += b.length; }
    ByteBuffer ret = ByteBuffer.allocate(len);
    for (byte[] b : bytes) { ret.put(b); }

    return ret.array();
  }

  public void write(ByteBuffer buff, List<Object> val) {
    for (byte[] b : toBytes(order, schema, val)) { buff.put(b); }
  }

  public List<Object> fromBytes(byte[] bytes) {
    return read(ByteBuffer.wrap(bytes));
  }

  public List<Object> read(ByteBuffer buff) {
    List<Object> ret = new ArrayList<Object>(schema.size());
    for (HSerializer s : schema) {
      if (s.supportsNull()) {
        ret.add(s.read(buff));
      } else {
        // read the isNull marker first, then decide whether to read further
        // into buff.
        if (new BOOLEAN(order).read(buff)) continue;
        else ret.add(s.read(buff));
      }
    }
    return ret;
  }

  /**
   * Use <code>schema</code> to serialize a the Objects in <code>val</code>.
   */
  @SuppressWarnings("unchecked")
  protected static List<byte[]> toBytes(Order order, List<HSerializer> schema, List<Object> val) {
    assert schema.size() == val.size() : "val length must match schema length";

    // serialize the values
    List<byte[]> bytes = new ArrayList<byte[]>(schema.size());
    Iterator<HSerializer> schemaIt = schema.iterator();
    Iterator<Object> valIt = val.iterator();
    while (schemaIt.hasNext() && valIt.hasNext()) {
      HSerializer s = schemaIt.next();
      Object v = valIt.next();
      if (s.supportsNull()) {
        bytes.add(s.toBytes(v));
      } else {
        // help out types that don't support null by prepending an isNull
        // marker. use isNull rather than isNotNull semantic because FALSE
        // sorts before TRUE, thus preserving the necessary semantic of null
        // sorting before values. this increases their serialized size by one
        // byte but allows for user flexibility.
        if (null == v) bytes.add(order == ASCENDING ? TRUE_ASC : TRUE_DSC);
        else {
          byte[] b = s.toBytes(v);
          byte[] escaped_b = new byte[b.length + 1];
          escaped_b[0] = (order == ASCENDING ? FALSE_ASC[0] : FALSE_DSC[0]);
          System.arraycopy(b, 0, escaped_b, 1, b.length);
          bytes.add(escaped_b);
        }
      }
    }
    assert !schemaIt.hasNext() && !valIt.hasNext();   
    return bytes;
  }
}
