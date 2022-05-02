package com.mzq.usage.haddop.mapreduce;

public class Test123 {

    public static void main(String[] args) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("hello world：" + args[0]);
    }
}
