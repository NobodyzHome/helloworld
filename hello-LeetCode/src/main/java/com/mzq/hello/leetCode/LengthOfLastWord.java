package com.mzq.hello.leetCode;

public class LengthOfLastWord {

    public int solution(String s) {
        int length = s.length(), count = 0;
        boolean findValidChar = false;
        // 从后向前遍历
        for (int i = length - 1; i >= 0; i--) {
            char c = s.charAt(i);
            // 如果遍历字符是空，有两种情况：
            // 1.如果还没有遇到一个正确的字符，则继续遍历
            // 2.如果遇到正确字符了，则终止遍历，代表已经找到对应最后的字符串了
            if (c == ' ') {
                if (findValidChar) {
                    break;
                } else {
                    continue;
                }
            } else {
                // 如果不是空，则字符串长度+1
                findValidChar = true;
                count++;
            }
        }
        return count;
    }
}
