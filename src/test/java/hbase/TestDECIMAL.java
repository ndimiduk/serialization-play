package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.math.BigDecimal;
import java.util.Comparator;

import org.junit.Test;

import types.DECIMAL;
import util.HSerializer;

public class TestDECIMAL extends RandomTestHSerializable<BigDecimal> {

  protected Comparator<BigDecimal> getComparator() {
    return new Comparator<BigDecimal>() {
      @Override
      public int compare(BigDecimal o1, BigDecimal o2) {
        return o1.compareTo(o2);
      }
    };
  }

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
