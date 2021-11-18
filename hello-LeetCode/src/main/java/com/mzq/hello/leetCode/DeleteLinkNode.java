package com.mzq.hello.leetCode;

import java.util.Objects;

public class DeleteLinkNode {
    public static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }

    public void solution(ListNode node, int target) {
        ListNode slow = null, fast = node;
        boolean findTarget = false;
        do {
            int num = fast.val;
            if (num != target) {
                if (findTarget) {
                    slow.next = fast;
                    slow = fast;
                    findTarget = false;
                } else {
                    slow = fast;
                }
                fast = fast.next;
            } else {
                fast = fast.next;
                findTarget = true;
            }
        } while (Objects.nonNull(fast));
    }

    public void solution(ListNode node) {
        ListNode slow = node, fast = slow.next;

        while (fast != null) {
            slow.val = fast.val;
            if (fast.next == null) {
                slow.next = null;
                break;
            } else {
                slow = fast;
                fast = fast.next;
            }
        }
    }

    public static void main(String[] args) {
        ListNode listNode1 = new ListNode(0);
        ListNode listNode2 = new ListNode(1);
        ListNode listNode3 = new ListNode(2);
        ListNode listNode4 = new ListNode(3);
        ListNode listNode5 = new ListNode(3);
        ListNode listNode6 = new ListNode(5);

        listNode1.next = listNode2;
        listNode2.next = listNode3;
        listNode3.next = listNode4;
        listNode4.next = listNode5;
        listNode5.next = listNode6;

        new DeleteLinkNode().solution(listNode2);
        System.out.println(listNode1);
    }
}
