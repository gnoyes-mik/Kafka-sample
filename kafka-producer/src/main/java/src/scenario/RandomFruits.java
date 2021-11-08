package src.scenario;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RandomFruits {
    private Map<Integer, String> basket = new HashMap<>();

    public RandomFruits() {
        init();
    }

    private void init() {
        String[] fruits = new String[]{
                "사과", "포도", "토마토", "바나나", "복분자",
                "복숭아", "블루베리", "산딸기", "살구", "앵두",
                "자두", "천도복숭아", "포도", "무화과", "배",
                "석류", "귤"};
        for (int i = 0; i < fruits.length; i++) {
            String fruit = fruits[i];
            basket.put(i, fruit);
        }
    }

    public String getRandomFruit() {
        Random random = new Random();
        int i = random.nextInt(17);
        return basket.get(i);
    }
}
