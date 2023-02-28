package main.java.test.apitest.source.leetcode.twolinkadd;

public class Solution {
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode tmp = new ListNode();
        ListNode start = tmp;
        ListNode point = tmp;
        int next = 0;
        while (l1 != null || l2 != null){
            if(l1 == null){
                tmp.val = (l2.val+next)%10;
                next = (l2.val+next) /10;
                l2 = l2.next;
            } else if(l2 == null){
                tmp.val = (l1.val+next)%10;
                next = (l1.val+next) /10;
                l1 = l1.next;
            } else if(l1 != null && l2 !=null){
                tmp.val = (l2.val + l1.val + next)%10;
                next = (l2.val + l1.val + next)/10;
                l1 =l1.next;
                l2 = l2.next;
            }
            point = tmp;
            tmp = new ListNode();
            point.next =tmp;
        }
        if(next == 0){
            point.next = null;
        }else {
            point.next.val = next;
        }
        return start;
    }
}
