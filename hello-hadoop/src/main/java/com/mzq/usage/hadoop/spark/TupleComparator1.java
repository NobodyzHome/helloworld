package com.mzq.usage.hadoop.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparator1 implements Comparator<Tuple2<String, Integer>>, Serializable {

    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        return Integer.compare(o1._2(), o2._2());
    }
}
