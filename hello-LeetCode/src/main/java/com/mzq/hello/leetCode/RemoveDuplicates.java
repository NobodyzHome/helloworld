package com.mzq.hello.leetCode;

public class RemoveDuplicates {

    public int[] solution(int nums[]) {
        int slow = -1, max = nums.length - 1;
        // 快指针遍历数组中的每一个元素
        for (int fast = 0; fast <= max; fast++) {
            int num = nums[fast];
            // 获取上一个元素的索引
            int previousIndex = fast - 1;
            // 如果上一个元素的索引是-1（代表当前遍历的是第一条数据）或者上一个元素和当前元素不同（因为给定数组是有序的，所以不重复的数组一定是上一个元素和下一个元素不同），代表当前元素是有效的
            if (previousIndex < 0 || nums[previousIndex] != num) {
                // 当前元素有效的话，slow就可以+1
                slow++;
                // 如果slow和fast不同，说明fast遍历过无效的数据，那么把slow位置的元素替换为fast位置的元素
                if (slow != fast) {
                    nums[slow] = num;
                }
            } else {
                // 如果上一个元素和当前元素相同，则fast继续遍历
                continue;
            }
        }

        // 当完成重复数据的遍历后，如果slow+1比max小，那么从slow+1到max都设置为0
        while (++slow <= max) {
            nums[slow] = 0;
        }
        return nums;
    }

    public static void main(String[] args) {
        RemoveDuplicates removeDuplicates = new RemoveDuplicates();
        int[] solution = removeDuplicates.solution(new int[]{0, 0, 1, 2, 3, 3, 4, 5, 6, 6});
        System.out.println(solution);
    }
}
