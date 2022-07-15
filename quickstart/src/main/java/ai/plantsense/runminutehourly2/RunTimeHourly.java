package ai.plantsense.runminutehourly2;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import ai.plantsense.runminutehourly2.RunTime;

import java.util.ArrayList;
import java.util.Arrays;

// function that takes (value BIGINT, weight INT), stores intermediate results in a structured
// type of CumulativeAccumulator, and returns the weighted average as BIGINT
public class RunTimeHourly extends AggregateFunction<Double, RunTime> {

    static int partition(Obj array[], int low, int high) {

        // choose the rightmost element as pivot
        Obj pivot = array[high];

        // pointer for greater element
        int i = (low - 1);

        // traverse through all elements
        // compare each element with pivot
        for (int j = low; j < high; j++) {
            if (array[j].value <= pivot.value) {

                // if element smaller than pivot is found
                // swap it with the greater element pointed by i
                i++;

                // swapping element at i with element at j
                Obj temp = array[i];
                array[i] = array[j];
                array[j] = temp;
            }

        }

        // swapt the pivot element with the greater element specified by i
        Obj temp = array[i + 1];
        array[i + 1] = array[high];
        array[high] = temp;

        // return the position from where partition is done
        return (i + 1);
    }

    static void quickSort(Obj array[], int low, int high) {
        if (low < high) {

            // find pivot element such that
            // elements smaller than pivot are on the left
            // elements greater than pivot are on the right
            int pi = partition(array, low, high);

            // recursive call on the left of pivot
            quickSort(array, low, pi - 1);

            // recursive call on the right of pivot
            quickSort(array, pi + 1, high);
        }
    }

  @Override
  public RunTime createAccumulator() {
     System.out.println("entering createAccumulator method");
    return new RunTime();
  }

   @Override
   public Double getValue(RunTime acc) {
     System.out.println("entering GetValue method");
     quickSort(acc.arr, 0, acc.arr.length - 1);
//     for (int i = 0; i < acc.arr.length; i++)
//     {
//      for (int j = i + 1; j < acc.arr.length; j++)
//      {
//        Obj tmp = new Obj();
//        if (acc.arr[i].run_min > acc.arr[j].run_min)
//        {
//          tmp = acc.arr[i];
//          acc.arr[i] = acc.arr[j];
//          acc.arr[j] = tmp;
//        }
//      }
//    }
     if(acc.arr!=null){
      System.out.println("final Array: "+acc.arr.length);
      Long prev_min = null;
      Double prev_value = null;
      Long current_min = null;
      Double current_value = null;
      Double total_run_min = 0.0;
      for (int i = 0; i < acc.arr.length; i++)
      {
        if(prev_min==null && prev_value==null){
          System.out.println("initial phase");
          prev_min = acc.arr[i].run_min;
          prev_value = acc.arr[i].value;
          System.out.println("after change in prev min and value initial phase:"+prev_min+","+prev_value);
        }
        else{
          current_min = acc.arr[i].run_min;
          current_value = acc.arr[i].value;
          if(current_value!=1.0 && prev_value==1.0){
            System.out.println("calculation phase");
            double diff = current_min - prev_min;
            total_run_min=total_run_min + diff;
            prev_min = current_min;
            prev_value = current_value;
            System.out.println("after change in prev min and value calc phase:"+prev_min+","+prev_value);
          }
          else if(current_value==1.0 && prev_value==1.0){
            System.out.println("calculation else phase");
            double diff = current_min - prev_min;
            total_run_min=total_run_min + diff;
            prev_min = current_min;
            prev_value = current_value;
          }
          else{
            System.out.println("pass phase");
            prev_min =current_min;
            prev_value = current_value;
            System.out.println("after change in prev min and value pass phase:"+prev_min+","+prev_value);
          }
        }
      }
      if(prev_value==1.0 && prev_min!=59){
        System.out.println("final calculation phase");
        double diff = 60.0 - prev_min;
        total_run_min=total_run_min + diff;
        return total_run_min;
      }
      else{
        System.out.println("final phase");
        return total_run_min;
      }
     }
     else{
       System.out.println("final phase");
       return 0.0;
     }
   }

  public void accumulate(RunTime acc, Long run_min, Double iValue) {
    System.out.println("entering Accumulate method:"+iValue+","+run_min);
    ArrayList<Obj> arrayList = new ArrayList<Obj>(Arrays.asList(acc.arr));
    arrayList.add(new Obj(run_min, iValue));
    acc.arr = arrayList.toArray(acc.arr);
    System.out.println("Array after adding element: "+acc.arr.length);  
  }
  public void retract(RunTime acc,  Long run_min, Double iValue) {}
}

















