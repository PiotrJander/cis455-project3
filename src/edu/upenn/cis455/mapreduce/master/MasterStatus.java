package edu.upenn.cis455.mapreduce.master;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MasterStatus {

    private static final int ACTIVE_DURATION = 30;
    private final static String ID = "id";
    private final static String IP = "ip";
    private final static String LAST_ACTIVE = "lastActive";
    private final static String PORT = "port";
    private final static String STATUS = "getStatus";
    private final static String JOB = "job";
    private final static String KEYS_READ = "keysRead";
    private final static String KEYS_WRITTEN = "keysWritten";
    private final static String RESULTS = "results";
    private final static List<String> requestsParams = Arrays.asList(PORT, STATUS, JOB, KEYS_READ, KEYS_WRITTEN, RESULTS);
    private static Map<String, Map<String, Object>> workers = new HashMap<>();

    public static void getWorkerStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
        HashMap<String, Object> workerStatus = new HashMap<>();
        workerStatus.put(IP, request.getRemoteAddr());
        workerStatus.put(LAST_ACTIVE, Instant.now());

        requestsParams.forEach(param -> {
            workerStatus.put(param, request.getParameter(param));
        });

        boolean success = normalizeWorkerStatus(response, workerStatus);
        if (success) {
            String workerId = workerStatus.get(IP) + ":" + workerStatus.get(PORT);
            workerStatus.put(ID, workerId);
            workers.put(workerId, workerStatus);
        }
    }

    private static boolean normalizeWorkerStatus(
            HttpServletResponse response,
            HashMap<String, Object> workerStatus
    ) throws IOException {
        if (workerStatus.values().stream().anyMatch(value -> value == null)) {
            response.sendError(400, "Usage: parameters port, getStatus, job, keysRead, keysWritten, results required");
            return false;
        }

        try {
            Stream.of(PORT, KEYS_READ, KEYS_WRITTEN).forEach(s -> {
                int number = Integer.parseInt((String) workerStatus.get(s));
                workerStatus.put(s, number);
            });
        } catch (NumberFormatException e) {
            response.sendError(400, "`port`, `keysRead`, `keysWritten` parameters must be numbers");
            return false;
        }
        return true;
    }

    public static void getStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter writer = response.getWriter();
        writer.println("<html><head><title>Status</title></head><body>");

        printStatusTable(writer);
        printStatusForm(writer);

        writer.println("</body></html>");
    }

    private static void printStatusForm(PrintWriter writer) {
        writer.println("<form action=\"/launch\" method=\"post\">");
        writer.println("<p>Class name: <input type=\"text\" name=\"class_name\"></p>");
        writer.println("<p>Input directory: <input type=\"text\" name=\"input_dir\"></p>");
        writer.println("<p>Output directory: <input type=\"text\" name=\"output_dir\"></p>");
        writer.println("<p>Number of map threads: <input type=\"number\" name=\"map_threads\"></p>");
        writer.println("<p>Number of reduce threads: <input type=\"number\" name=\"reduce_threads\"></p>");
        writer.println("</form>");
    }

    private static void printStatusTable(PrintWriter writer) {
        writer.println("<table>");
        writer.println("<thead><tr><th>IP:port</th><th>status</th><th>job</th><th>keys read</th><th>keys written</th></tr></thead>");
        writer.println("<tbody>");

        workers.forEach((k, worker) -> {
            Instant lastActive = (Instant) worker.get(LAST_ACTIVE);
            if (lastActive.isAfter(Instant.now().minusSeconds(ACTIVE_DURATION))) {
                writer.format(
                        "<tr><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td></tr>\n",
                        worker.get(ID),
                        worker.get(STATUS),
                        worker.get(JOB),
                        worker.get(KEYS_READ),
                        worker.get(KEYS_WRITTEN)
                );
            }
        });

        writer.println("</tbody>");
        writer.println("</table>");
    }
}
