package ai.plantsense.runminutehourly2;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import ai.plantsense.runminutehourly2.RunTimeOW;

import java.util.ArrayList;
import java.util.Arrays;
import java.time.Duration;
import java.time.LocalDateTime;

public class RunTimeOverWindow extends AggregateFunction<Double, RunTimeOW>{
   @Override
   public RunTimeOW createAccumulator() {
    System.out.println("entering createAccumulator method");
    return new RunTimeOW();
   }

   @Override
   public Double getValue(RunTimeOW acc) {
     System.out.println("entering GetValue method");
     if(acc.prev_value!=null && acc.current_value==null){
        return 0.0;
     }
     else{
         return acc.total_run_min;
     }
   }

  public void accumulate(RunTimeOW acc, LocalDateTime timestamp, Double iValue) {
    System.out.println("entering Accumulate method:"+iValue+","+timestamp);
    if(acc.prev_timestamp==null && acc.prev_value==null){
        System.out.println("initial phase");
        acc.prev_timestamp = timestamp;
        acc.prev_value = iValue;
        System.out.println("after change in prev min and value initial phase:"+acc.prev_timestamp+","+acc.prev_value);
    }
    else{
        acc.current_timestamp = timestamp;
        acc.current_value = iValue;
        if(acc.current_value!=1.0 && acc.prev_value==1.0){
            System.out.println("calculation phase");
            Duration duration = Duration.between(acc.prev_timestamp, acc.current_timestamp);
            acc.total_run_min= acc.total_run_min+Double.valueOf(duration.toMinutes());
            acc.prev_timestamp = acc.current_timestamp;
            acc.prev_value = acc.current_value;
            System.out.println("after change in prev min and value calc phase:"+acc.prev_timestamp+","+acc.prev_value);
        }
        else if(acc.current_value==1.0 && acc.prev_value==1.0){
            System.out.println("calculation else phase");
            Duration duration = Duration.between(acc.prev_timestamp, acc.current_timestamp);
            acc.total_run_min= acc.total_run_min+Double.valueOf(duration.toMinutes());
            acc.prev_timestamp = acc.current_timestamp;
            acc.prev_value = acc.current_value;
        }
        else{
            System.out.println("pass phase");
            acc.prev_timestamp =acc.current_timestamp;
            acc.prev_value = acc.current_value;
            System.out.println("after change in prev min and value pass phase:"+acc.prev_timestamp+","+acc.prev_value);
        }
    }
  }
  public void retract(RunTimeOW acc,  LocalDateTime timestamp, Double iValue) {}
}
