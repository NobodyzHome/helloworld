package com.mzq.usage.hadoop.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple2<Character, Integer>>, Serializable {

    @Override
    public int compare(Tuple2<Character, Integer> o1, Tuple2<Character, Integer> o2) {
        return Integer.compare(o1._2(), o2._2());
    }
}
