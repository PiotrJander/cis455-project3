package edu.upenn.cis455.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpClient {

    public static HttpURLConnection get(URL url, Map<String, String> params) throws IOException {
        String query = "?" + FormUrlEncoded.fromMap(params);
        URL urlWithQuery = new URL(url, url.getPath() + query);
        HttpURLConnection conn = (HttpURLConnection) urlWithQuery.openConnection();
        conn.setRequestMethod("GET");
        return conn;
    }

    public static HttpURLConnection get(String urlString, Map<String, String> params) throws IOException {
        return get(new URL(urlString), params);
    }

    public static HttpURLConnection post(URL url, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Content-Length", String.valueOf(body.length()));
        OutputStream os = conn.getOutputStream();
        os.write(body.getBytes());
        return conn;
    }

    public static HttpURLConnection post(String urlString, String body) throws IOException {
        return post(new URL(urlString), body);
    }

    public static void main(String[] args) {

        // test GET
//        try {
//            HashMap<String, String> params = new HashMap<>();
//            params.put("q", "india");
//            HttpURLConnection conn = get("http://www.google.com/search", params);
////            conn.connect();
//            int responseCode = conn.getResponseCode();
//            conn.getErrorStream();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // test POST
        try {
            HttpURLConnection connection = post("http://httpbin.org/post", "{\"foo\": \"bar\"}");
            InputStreamReader inputStreamReader = new InputStreamReader(connection.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            System.out.println(connection.getResponseCode() + " " + bufferedReader.readLine());
            System.out.println(connection.getResponseCode() + " " + bufferedReader.readLine());
            System.out.println(connection.getResponseCode() + " " + bufferedReader.readLine());
            System.out.println(connection.getResponseCode() + " " + bufferedReader.readLine());
            System.out.println(bufferedReader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
