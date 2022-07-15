package ai.plantsense.cumulativeTotalizer;
// mutable accumulator of structured type for the aggregate function
public class CumulativeAccumulator {
//   public long sum = 0;
//   public int count = 0;
  public Double cumulativeDataValue = null;
  public Double prev_value = null;
  public Double currentValue = null;
  public Double lastCumulative = null;
  public Double lastKnownCumulativeVal = 0.0;
  public Double deltaDiffOfWindow = 0.0;
  public Double first_value=null;
}