package com.mzq.hello.leetCode;

public class HalfSearch {

    public int solution(int[] nums, int value) {
        // start代表在start指针之前的元素（不包括start指针指向的元素）都是和value不匹配的元素，end代表在end指针之后的元素（不包括end指针指向的元素）都是和value不匹配的元素
        int start = 0, end = nums.length - 1;
        int valueIndex = -1;
        do {
            // 先找到中间索引
            int mid = (end + start) / 2;
            int midValue = nums[mid];
            // 判断中间索引的值是否和target相等，则中间索引就是我们要找的索引
            if (midValue == value) {
                valueIndex = mid;
                break;
            } else if (value > midValue) {
                // 如果target比中间索引值大，那么改变start为中间索引值+1
                start = mid + 1;
            } else {
                // 如果target比中间索引值小，那么改变end为中间索引值-1
                end = mid - 1;
            }
            // 如果start小于或等于end，则继续遍历。由于end代表它之后的元素都和value不匹配，因此如果start在end之后，就没必要继续查询了
        } while (start <= end);
        return valueIndex;
    }

    public static void main(String[] args) {
        int target = new HalfSearch().solution(new int[]{1, 3, 4, 6, 7, 8, 11, 12}, 13);
        System.out.println(target);
    }
}
