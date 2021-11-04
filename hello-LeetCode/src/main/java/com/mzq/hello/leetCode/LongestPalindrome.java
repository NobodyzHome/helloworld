package com.mzq.hello.leetCode;

public class LongestPalindrome {

    public String solution(String words) {
        int max = Integer.MIN_VALUE;
        String longest = null;

        for (int i = 0; i <= words.length() - 2; i++) {
            for (int j = i + 1; j <= words.length() - 1; j++) {
                String subStr = words.substring(i, j + 1);
                if (isPalindrome(subStr)) {
                    if (subStr.length() > max) {
                        max = subStr.length();
                        longest = subStr;
                    }
                }
            }
        }
        return longest;
    }


    public boolean isPalindrome(String numStr) {
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
        System.out.println(new LongestPalindrome().solution("ab"));
    }
}
