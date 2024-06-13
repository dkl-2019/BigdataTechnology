package cn.kaizi.azkaban;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class JavaJob {

    public static void main(String[] args) throws IOException {
        FileOutputStream fos = null;
        try {
            new FileOutputStream("/export/datasets/java.txt");
            fos.write("you are the best!".getBytes());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
