package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import types.SHORT;
import util.HSerializer;

public class TestSHORT extends RandomTestHSerializable<Short> {

  protected Short create() {
    return Short.valueOf((short) r.nextInt());
  }

  protected HSerializer<Short> ascendingSerializer() { return new SHORT(ASCENDING); }
  protected HSerializer<Short> descendingSerializer() { return new SHORT(DESCENDING); }

}
