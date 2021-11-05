package com.mzq.hello.leetCode;

import java.util.Arrays;

public class PlusOne {

    public int[] solution(int[] digits) {
        int carry = 1;
        for (int index = digits.length - 1; index >= 0; index--) {
            // 将遍历的元素加上进位的值（第一次进位的值为题目指定的）
            int num = digits[index] + carry;
            // 如果加完的加过超过10，则给进位赋值1，当前元素赋值0
            if (num >= 10) {
                carry = 1;
                digits[index] = num % 10;
            } else {
                // 如果加完的值没有超过10，那么则不需要继续遍历了
                digits[index] = num;
                carry = 0;
                break;
            }
        }

        // 如果遍历完，还有进位，那么要生成一个新的数组，第一个元素是进位的值，后面的元素是原数组的值
        if (carry > 0) {
            int[] newNums = new int[digits.length + 1];
            newNums[0] = carry;
            for (int i = 0; i <= digits.length - 1; i++) {
                newNums[i + 1] = digits[i];
            }
            return newNums;
        } else {
            return digits;
        }
    }

    public static void main(String[] args) {
        PlusOne plusOne = new PlusOne();
        System.out.println(Arrays.toString(plusOne.solution(new int[]{2, 8, 9, 9})));
    }
}
