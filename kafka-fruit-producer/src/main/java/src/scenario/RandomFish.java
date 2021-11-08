package src.scenario;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RandomFish {
    private Map<Integer, String> basket = new HashMap<>();

    public RandomFish() {
        init();
    }

    private void init() {
        String[] fruits = new String[]{
                "오징어", "광어", "참치", "농어", "민어",
                "날치", "문어", "참돔", "청새치", "방어"
        };
        for (int i = 0; i < fruits.length; i++) {
            String fruit = fruits[i];
            basket.put(i, fruit);
        }
    }

    public String getRandomFish() {
        Random random = new Random();
        int i = random.nextInt(10);
        return basket.get(i);
    }
}
