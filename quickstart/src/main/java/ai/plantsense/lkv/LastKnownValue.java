package ai.plantsense.lkv;

import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.AggregateFunction;
import static org.apache.flink.table.api.Expressions.*;
import ai.plantsense.lkv.*;
import org.apache.flink.table.functions.FunctionContext;

import java.util.*;

public class LastKnownValue extends AggregateFunction<Double, LastKnownValueAccumulator>{
  @Override
  public LastKnownValueAccumulator createAccumulator() {
    //     System.out.println("entering createAccumulator method");
    return new LastKnownValueAccumulator();
  }

  @Override
  public Double getValue(LastKnownValueAccumulator acc) {   
    return acc.lastknownvalue;  
  }

  public void accumulate(LastKnownValueAccumulator acc, Double iValue) {
    acc.lastknownvalue=iValue;
  }
}
