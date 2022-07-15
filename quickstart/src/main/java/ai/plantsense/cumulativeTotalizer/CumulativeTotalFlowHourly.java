package ai.plantsense.cumulativeTotalizer;

import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.AggregateFunction;
import static org.apache.flink.table.api.Expressions.*;
import ai.plantsense.cumulativeTotalizer.*;
import org.apache.flink.table.functions.FunctionContext;

import java.util.*;

public class CumulativeTotalFlowHourly extends AggregateFunction<Double, CumulativeHrModel>  {
    
    // method to find the partition position
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
    public CumulativeHrModel createAccumulator() {
        System.out.println("entering createAccumulator method");
        return new CumulativeHrModel();
    }

    @Override
    public Double getValue(CumulativeHrModel acc) {
        System.out.println("entering GetValue method");
        quickSort(acc.arr, 0, acc.arr.length - 1);
        // for (int i = 0; i < acc.arr.length; i++)
        // {  
        //     for (int j = i + 1; j < acc.arr.length; j++)   
        //     {  
        //         Obj tmp = new Obj();
        //         if (acc.arr[i].run_min > acc.arr[j].run_min)   
        //         {  
        //         tmp = acc.arr[i];  
        //         acc.arr[i] = acc.arr[j];  
        //         acc.arr[j] = tmp;  
        //         }  
        //     }
        // }
        if(acc.arr!=null){
            System.out.println("final Array: "+acc.arr.length);
            Double cumulativeDataValue = null;
            Double prev_value = null;
            Double currentValue = null;
            Double lastCumulative = null;
            for (int i = 0; i < acc.arr.length; i++)
            {
                currentValue = acc.arr[i].value;
                if(prev_value==null){
                        //INFO:: Try to load from DB, Totalizer Table Table
                        prev_value= currentValue;
                }
                if(lastCumulative==null){
                        lastCumulative = 0.0;
                }

                Double deltaDiff = currentValue - prev_value;
                if(deltaDiff<0){
                        lastCumulative = lastCumulative+currentValue;
                }
                else{
                        lastCumulative = lastCumulative+deltaDiff;
                }

                prev_value = currentValue;
                cumulativeDataValue = lastCumulative;
            }
            return cumulativeDataValue;
        }
        else{
            return 0.0;
        }
    }

    public void accumulate(CumulativeHrModel acc, Long run_min, Double iValue) {
        System.out.println("entering Accumulate method:"+iValue+","+run_min);
        ArrayList<Obj> arrayList = new ArrayList<Obj>(Arrays.asList(acc.arr));
        arrayList.add(new Obj(run_min, iValue));
        acc.arr = arrayList.toArray(acc.arr);
        System.out.println("Array after adding element: "+acc.arr.length); 
    }
    public void retract(CumulativeHrModel acc,  Long run_min, Double iValue) {}

}
