import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestJava {
    public static void main(String[] args) {
        HashMap<String,Integer> map1 = new HashMap();
        map1.put("x",1);
        map1.put("y",2);
        HashMap<String,Integer> map2 = new HashMap();
        map2.putAll(map1);
        map2.put("z",3);

        Object map3 = map1.clone();

        HashMap<String,Integer> map4 = map1;
        HashMap<String,Integer> map5 = new HashMap<>(map1);

//
//        Iterator<Map.Entry<String, Integer>> x = map1.entrySet().iterator();
//        while(x.hasNext()){
//            Map.Entry<String, Integer> y = x.next();
//            map2.merge(y.getKey(), y.getValue(), (v1, v2) -> v1 + v2);
//        }
//
//        System.out.println(map2);



        System.out.println(map1);
        System.out.println(map2 == map1);
        System.out.println(map3 == map1);
        System.out.println(map4 == map1);
        System.out.println(map5);
        System.out.println(map5 == map1);



    }
}
