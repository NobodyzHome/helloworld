package com.mzq.hello.leetCode;

public class RemoveElement {

    public static int[] solution(int[] nums, int target) {
        int slow = -1, max = nums.length - 1;

        // 快指针遍历数组的每一个元素
        for (int fast = 0; fast <= max; fast++) {
            // 取当前遍历的数据
            int num = nums[fast];
            // 如果当前数据和target相等，则快指针继续向前遍历
            if (num == target) {
                continue;
            } else {
                // 如果当前数据和target不同，那么慢指针可以+1。代表slow索引之前的数据都和target不一样
                slow++;
                // 如果slow和fast不一致，说明fast遍历过和target相同的数据，那么把slow索引上的数据替换为fast索引上的数据
                if (slow != fast) {
                    nums[slow] = num;
                }
            }
        }

        // 在遍历完成后，如果slow+1比数组最大索引小，那么把从slow+1到最大索引的元素都赋值为0
        while (++slow <= max) {
            nums[slow] = 0;
        }
        return nums;
    }

    public static void main(String[] args) {
        int[] solution = solution(new int[]{2, 1, 1, 2, 3, 4, 2}, 2);
        System.out.println(solution);
    }
}
