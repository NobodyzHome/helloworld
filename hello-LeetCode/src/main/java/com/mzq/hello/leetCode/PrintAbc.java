package com.mzq.hello.leetCode;

public class PrintAbc {

    private int current = 0;
    private int test = 1;

    public void a() {
        synchronized (this) {
            while (true) {
                if (current % 3 == 0) {
                    System.out.println("a:" + (test++));
                    current++;
                    notify();
                } else {
                    try {
                        wait(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (test > 75) {
                    break;
                }
            }
        }
    }

    public void b() {
        synchronized (this) {
            while (true) {
                if (current % 3 == 1) {
                    System.out.println("b:" + (test++));
                    current++;
                    notify();
                } else {
                    try {
                        wait(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (test > 75) {
                    break;
                }
            }

        }
    }

    public void c() {
        synchronized (this) {
            while (true) {
                if (current % 3 == 2) {
                    System.out.println("c:" + (test++));
                    notify();
                    current++;
                } else {
                    try {
                        wait(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (test > 75) {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
        PrintAbc prinAbc = new PrintAbc();
        Thread thread1 = new Thread(prinAbc::a);
        Thread thread2 = new Thread(prinAbc::b);
        Thread thread3 = new Thread(prinAbc::c);

        thread3.start();
        thread2.start();
        thread1.start();
    }
}
