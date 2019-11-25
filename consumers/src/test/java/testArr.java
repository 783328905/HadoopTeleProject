import org.junit.Test;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/22 10:16
 * 4
 */
public class testArr {
    @Test
    public void  test(){
        int [] a = new int [10];
        int [] b = new int []{1,2,3};
        int [][] c = new int[][]{{1,2},{3,90,8}};
        for( int[] i : c){
            for(int j : i)
                System.out.println(j);
            System.out.println("--------");
        }
    }

    public void test(int i){
        System.out.println(i);
    }

    @Test
    public void test2(){
        int i =0;
        test(++i);
        System.out.println(i);
    }

}
