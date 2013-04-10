package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.math.BigDecimal;

import org.junit.Test;

import types.DECIMAL;
import util.HSerializer;

public class TestDECIMAL extends RandomTestHSerializable<BigDecimal> {

  protected BigDecimal create() {
    return new BigDecimal(r.nextLong());
  }

  protected HSerializer<BigDecimal> ascendingSerializer() { return new DECIMAL(ASCENDING); }
  protected HSerializer<BigDecimal> descendingSerializer() { return new DECIMAL(DESCENDING); }

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
