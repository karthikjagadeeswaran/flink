package ai.plantsense.cumulativeTotalizer;

import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.AggregateFunction;
import static org.apache.flink.table.api.Expressions.*;
import ai.plantsense.cumulativeTotalizer.*;
import org.apache.flink.table.functions.FunctionContext;

import java.util.*;

// function that takes (value BIGINT, weight INT), stores intermediate results in a structured
// type of CumulativeAccumulator, and returns the weighted average as BIGINT
public class CumulativeTotalFlow extends AggregateFunction<Double, CumulativeAccumulator> {
  
  @Override
  public CumulativeAccumulator createAccumulator() {
//     System.out.println("entering createAccumulator method");
    return new CumulativeAccumulator();
  }

  @Override
  public Double getValue(CumulativeAccumulator acc) {
        System.out.println("entering getValue method:"+acc.cumulativeDataValue);
        if (acc.cumulativeDataValue==null){
                return null;
        }
        else{
              return acc.cumulativeDataValue;  
        }
  }

  public void accumulate(CumulativeAccumulator acc, Double iValue) {
        // System.out.println("entering Accumulate method:"+iValue);
        System.out.println("prev Value and cumulative total:"+acc.prev_value+","+acc.cumulativeDataValue);
        acc.currentValue = iValue;
        if(acc.prev_value==null){
                //INFO:: Try to load from DB, Totalizer Table Table
                acc.prev_value= acc.currentValue;
                acc.first_value=acc.currentValue;
        }
        if(acc.lastCumulative==null){
                acc.lastCumulative = acc.currentValue;
        }

        Double deltaDiff = acc.currentValue - acc.prev_value;
        if(deltaDiff<0){
                acc.lastCumulative = acc.lastCumulative+acc.currentValue;
        }
        else{
                acc.lastCumulative = acc.lastCumulative+deltaDiff;
        }

        acc.prev_value = acc.currentValue;
        acc.cumulativeDataValue = acc.lastCumulative;
  }

    public void retract(CumulativeAccumulator acc, Double iValue) {
        // System.out.println("entering Retract method:"+iValue);
//        acc.currentValue = iValue;
//        if(acc.prev_value==null){
//            //INFO:: Try to load from DB, Totalizer Table Table
//            acc.prev_value= acc.currentValue;
//            acc.first_value=acc.currentValue;
//        }
//        if(acc.lastCumulative==null){
//            acc.lastCumulative = acc.currentValue;
//        }
//
//        Double deltaDiff = acc.currentValue - acc.prev_value;
//        if(deltaDiff<0){
//            acc.lastCumulative = acc.lastCumulative+acc.currentValue;
//        }
//        else{
//            acc.lastCumulative = acc.lastCumulative+deltaDiff;
//        }
//
//        acc.prev_value = acc.currentValue;
//        acc.cumulativeDataValue = acc.lastCumulative;
    }

   public void merge(CumulativeAccumulator acc, Iterable<CumulativeAccumulator> it) {
//       Iterator<CumulativeAccumulator> iterator = it.iterator();
//       System.out.println("entering merge method");
//       CumulativeAccumulator prev_ses_val = new CumulativeAccumulator();
//       while(iterator.hasNext()){
//           prev_ses_val = iterator.next();
//       }
//       if(prev_ses_val.prev_value>acc.first_value){
//           acc.lastCumulative = acc.lastCumulative+ prev_ses_val.lastCumulative;
//           acc.cumulativeDataValue = acc.lastCumulative;
//       }
//       else{
//           Double deltDiff = acc.lastCumulative - prev_ses_val.prev_value;
//           acc.lastCumulative = prev_ses_val.lastCumulative + deltDiff;
//           acc.cumulativeDataValue = acc.lastCumulative;
//       }
   }
}
