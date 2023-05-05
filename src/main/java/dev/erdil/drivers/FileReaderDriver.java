package dev.erdil.drivers;

import java.io.*;

public class FileReaderDriver {
    public static void main(String[] args) {
        try(BufferedReader reader = new BufferedReader(new FileReader("resources/wc.txt"))) {
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}