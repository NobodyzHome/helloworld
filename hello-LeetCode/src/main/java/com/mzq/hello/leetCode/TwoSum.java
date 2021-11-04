package com.mzq.hello.leetCode;

public class TwoSum {

    public int[] solution(int[] nums, int target) {
        int max = nums.length - 1;
        int[] result = null;
        // slow指针，由于数组中必须为两个数，因此slow需要小于等于max-1。因为当slow=max时，就只可能有一个数了
        for (int slow = 0; slow <= max - 1; slow++) {
            // 获取slow指针对应的元素
            int slowNum = nums[slow];
            // 获取如果要相加得target时，期望的数字是什么。例如num=2,target=9,那么期望的元素则为7
            int remaining = target - slowNum;
            // 从slow指针的后一位开始遍历，判断数组中的元素是否和期望的元素相同
            for (int fast = slow + 1; fast <= max; fast++) {
                int fastNum = nums[fast];
                // 如果相同，那么slowNum和fastNum对应的索引就是题目需要的
                if (remaining == fastNum) {
                    result = new int[]{slow, fast};
                    break;
                }
            }
        }

        return result;
    }

    public static void main(String[] args) {
        int[] solution = new TwoSum().solution(new int[]{3, 3}, 6);
        System.out.println(solution);
    }
}
