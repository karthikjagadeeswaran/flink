package ai.plantsense.runminutehourly;

import org.apache.flink.table.functions.AggregateFunction;

// function that takes (value BIGINT, weight INT), stores intermediate results in a structured
// type of CumulativeAccumulator, and returns the weighted average as BIGINT
public class RunTimeHourly extends AggregateFunction<Double, RunTime> {
  
  @Override
  public RunTime createAccumulator() {
     System.out.println("entering createAccumulator method");
    return new RunTime();
  }

  @Override
  public Double getValue(RunTime acc) {
    System.out.println("entering GetValue method");
    if(acc.prev_value==1.0 && acc.prev_min!=59){
      System.out.println("final calculation phase");
      double diff = 60.0 - acc.prev_min;
      acc.total_run_min=acc.total_run_min + diff;
      return acc.total_run_min;
    }
    else{
      System.out.println("final phase");
      return acc.total_run_min;
    }      
  }

  public void accumulate(RunTime acc, Long run_min, Double iValue) {
    if(acc.prev_min==null && acc.prev_value==null){
      System.out.println("entering Accumulate method"+iValue+","+run_min);
      System.out.println("initial phase");
      acc.prev_min = run_min;
      acc.prev_value = iValue;
      System.out.println("after change in prev min and value initial phase:"+acc.prev_min+","+acc.prev_value);
    }
    else{
      System.out.println("entering Accumulate method"+iValue+","+run_min);
      acc.current_min = run_min;
      acc.current_value = iValue;
      if(acc.current_value!=1.0 && acc.prev_value==1.0){
        System.out.println("calculation phase");
        double diff = acc.current_min - acc.prev_min;
        acc.total_run_min=acc.total_run_min + diff;
        acc.prev_min = acc.current_min;
        acc.prev_value = acc.current_value;
        System.out.println("after change in prev min and value calc phase:"+acc.prev_min+","+acc.prev_value);
      }
      else if(acc.current_value==1.0 && acc.prev_value==1.0){
        System.out.println("calculation else phase");
        double diff = acc.current_min - acc.prev_min;
        acc.total_run_min=acc.total_run_min + diff;
        acc.prev_min = acc.current_min;
        acc.prev_value = acc.current_value;
      }
      else{
        System.out.println("pass phase");
        acc.prev_min = acc.current_min;
        acc.prev_value = acc.current_value;
        System.out.println("after change in prev min and value pass phase:"+acc.prev_min+","+acc.prev_value);
      }
    }
  }
  public void retract(RunTime acc,  Long run_min, Double iValue) {}
}
