package com.mzq.hello.leetCode;

public class IsPalindrome {

    public boolean solution(int num) {
        // 如果num是负数，按照题目要求不符合回文数
        if (num < 0) {
            return false;
        } else if (num < 10) {
            // 如果num只有一位，那它必然也是回文数
            return true;
        }

        String numStr = String.valueOf(num);
        int length = numStr.length();
        int midIndex = length / 2;

        char[] first, end;
        // 如果数字长度是偶数，那么将数字平均拆分到两个数组。例如将123456拆分成[123]和[456]这两个数组
        if (length % 2 == 0) {
            first = numStr.substring(0, midIndex).toCharArray();
            end = numStr.substring(midIndex).toCharArray();
        } else {
            // 如果数字长度是奇数，那么不要中间那个数字，把数字拆成两个数组。例如将1234567拆分成[123]和[567]这两个数组
            first = numStr.substring(0, midIndex).toCharArray();
            end = numStr.substring(midIndex + 1).toCharArray();
        }

        boolean isPalindrome = true;
        int endIndex = end.length - 1;
        // 遍历这两个字符串，first从前往后遍历，last从后往前遍历，这样遍历完，如果都相同就是回文数，只要有一个不是，就不是回文数
        for (int firstIndex = 0; firstIndex <= first.length - 1; firstIndex++) {
            if (first[firstIndex] == end[endIndex--]) {
                continue;
            } else {
                isPalindrome = false;
                break;
            }
        }
        return isPalindrome;
    }

    public static void main(String[] args) {
        boolean solution = new IsPalindrome().solution(10);
        System.out.println(solution);
    }
}
