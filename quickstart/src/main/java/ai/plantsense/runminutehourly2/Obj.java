package ai.plantsense.runminutehourly2;

public class Obj {
    public Long run_min;
    public Double value;

    public Obj(){
        run_min=null;
        value=null;
    }
    
    public Obj(Long rm, Double v){
        run_min = rm;
        value = v;
    }
}
