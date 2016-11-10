package edu.upenn.cis455.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpClient {

    public static HttpURLConnection get(URL url, String method, Map<String, String> params) throws IOException {
        String query = "?" + FormUrlEncoded.fromMap(params);
        URL urlWithQuery = new URL(url, "" + query);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        return conn;
    }

    public static HttpURLConnection get(String urlString, String method, Map<String, String> params) throws IOException {
        return get(new URL(urlString), method, params);
    }

//    public static void main(String[] args) {
//        try {
//            HashMap<String, String> params = new HashMap<>();
//            params.put("q", "india");
//            HttpURLConnection conn = get("http://www.google.com", "GET", params);
////            conn.connect();
//            int responseCode = conn.getResponseCode();
//            conn.getErrorStream();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}
