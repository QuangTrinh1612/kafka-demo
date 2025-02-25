package io.conduktor.demos.kafka.Request;

import java.io.IOException;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class RequestDemo {
    public static void main(String[] args) {
        String url = "https://fakestoreapi.com/products/1";

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                            .url(url)
                            .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                System.out.println(response.body().string());
            } else {
                System.out.println("Request failed: " + response.code());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}