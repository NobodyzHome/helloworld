package com.mzq.hello.leetCode;

public class Reverse {

    public int solution(int x) {
        int result = 0;
        int temp;
        do {
            if (result < Integer.MIN_VALUE / 10 || result > Integer.MAX_VALUE / 10) {
                return 0;
            }
            // 先用x取余，获取最后一位的值
            temp = x % 10;
            // 每次都用上一次的结果*10，再加上当前获取出的最后一位，这样相当于把前面的位数都往前移了一位
            result = result * 10 + temp;
            // 计算完毕当前遍历的结果后，就让x变为x/10，这样就把计算过的末尾移除了，下一次遍历依旧余10就可以获得末位了
            x /= 10;
            // 任何数字，只要除10以后还不是0，那么它就还可以继续遍历，获取最后一位
        } while (x != 0);

        return result;
    }

    public static void main(String[] args) {
        int solution = new Reverse().solution(-2147483412);
        System.out.println(solution);
    }
}
