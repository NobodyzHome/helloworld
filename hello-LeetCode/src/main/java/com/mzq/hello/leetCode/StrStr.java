package com.mzq.hello.leetCode;

public class StrStr {

    public int strStr(String haystack, String needle) {
        if (needle.length() == 0) {
            return 0;
        } else if (haystack.length() == 0) {
            return -1;
        }

        int maxIndex = haystack.length() - 1;
        int searchIndex = -1;
        char firstExpectedChar = needle.charAt(0);
        // 遍历每一个字符
        for (int i = 0; i <= maxIndex; i++) {
            char currentChar = haystack.charAt(i);
            // 如果当前char和firstExpectedChar相同，则截取当前index到needle长度的子串，如果相等，则返回当前遍历的索引
            if (currentChar == firstExpectedChar) {
                // 如果要截取的end超过了最大index值，那么haystack肯定满足不了needle的要求了，直接返回-1
                int end = i + needle.length() - 1;
                if (end > maxIndex) {
                    break;
                } else {
                    // 如果end比maxIndex小，那么截取从i到end的子串，判断是否和needle相同
                    String subStr = haystack.substring(i, end + 1);
                    if (subStr.equals(needle)) {
                        searchIndex = i;
                        break;
                    }
                }
            }
        }

        return searchIndex;
    }

    public static void main(String[] args) {
        StrStr strStr = new StrStr();
        System.out.println(strStr.strStr("a", ""));
    }
}
