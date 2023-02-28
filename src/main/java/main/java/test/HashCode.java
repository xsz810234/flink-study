package main.java.test;

import org.apache.flink.table.functions.ScalarFunction;

public class HashCode extends ScalarFunction {
    private int factor =13;

    public HashCode(int factor){
        this.factor = factor;
    }

    public int eval(String s){
        return s.hashCode() * factor;
    }
}
