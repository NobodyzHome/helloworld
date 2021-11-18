package com.mzq.hello.leetCode;

public class DeleteRepeatLinkedNode {
    public static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }

    public ListNode solution(ListNode head) {
        ListNode slow = head, fast = slow == null ? null : slow.next;
        if (slow == null) {
            return null;
        } else if (fast == null) {
            return head;
        }

        boolean hasRepeat = false;
        do {
            if (slow.val != fast.val) {
                if (hasRepeat) {
                    hasRepeat = false;
                    slow.next = fast;
                    slow = fast;
                    fast = fast.next;
                } else {
                    slow = fast;
                    fast = fast.next;
                }
            } else {
                fast = fast.next;
                if (fast == null) {
                    slow.next = null;
                } else {
                    hasRepeat = true;
                }
            }

        } while (fast != null);
        return head;
    }
}
