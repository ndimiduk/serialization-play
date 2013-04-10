package hbase;

import java.math.BigInteger;

import org.junit.Test;

import types.VARINT;
import util.HSerializer;

public class TestVARINT extends RandomTestHSerializable<BigInteger> {

  protected BigInteger create() {
    int maxNumBits = r.nextInt(10);
    return new BigInteger(maxNumBits, r);
  }

  protected HSerializer<BigInteger> ascendingSerializer() { return new VARINT(); }
  protected HSerializer<BigInteger> descendingSerializer() { return new VARINT(); }

  @Test
  @Override
  public void testHSerializable() {}

  @Test
  @Override
  public void testNullAwareCompareAsc() {}

  @Test
  @Override
  public void testNullAwareCompareDsc() {}
}
