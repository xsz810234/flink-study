package main.java.test.apitest.source.leetcode.twolinkadd;

public class Main {
    public static void main(String[] args) {
//        ListNode l1 = new ListNode();
//        l1.val =2;
//        ListNode tmp1 = new ListNode(4);
//        l1.next = tmp1;
//        ListNode tmp2 = new ListNode(3);
//        tmp1.next = tmp2;
//        ListNode l2 = new ListNode();
//        l2.val =5;
//        ListNode tmp3 = new ListNode(6);
//        l2.next = tmp3;
//        ListNode tmp4 = new ListNode(4);
//        tmp3.next = tmp4;
        ListNode l1 = getListNode(new Integer[]{0,8,6,5,6,8,3,5,7});
        ListNode l2 = getListNode(new Integer[]{6,7,8,0,8,5,8,9,7});

        ListNode result = new Solution().addTwoNumbers(l1, l2);
        while (result != null){
            System.out.println(result.val);
            result = result.next;
        }
    }

    public static ListNode getListNode(Integer[] l1){
        ListNode tmp = new ListNode();
        ListNode start;
        start = tmp;

        for(int i = 0; i<l1.length; i++){
            tmp.val = l1[i];
            if(i < l1.length -1){
                tmp.next = new ListNode();
                tmp = tmp.next;
            }
        }
        return start;
    }
}
