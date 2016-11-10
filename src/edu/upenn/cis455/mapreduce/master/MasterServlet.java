package edu.upenn.cis455.mapreduce.master;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MasterServlet extends HttpServlet {

    final static long serialVersionUID = 455555001;
    private final int ACTIVE_DURATION = 30;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private Map<String, Map<String, Object>> workers = new HashMap<>();

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
        this.request = request;
        this.response = response;

        switch (request.getPathInfo()) {
            case "/status":
                getStatus();
                break;
            case "/workerstatus":
                getWorkerStatus();
                break;
            default:
                response.sendError(404);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.request = req;
        this.response = resp;

        switch (request.getPathInfo()) {
            case "/launch":
                postLaunch();
                break;
            default:
                response.sendError(404);
        }
    }

    private void postLaunch() throws IOException {
        HashMap<String, Object> fields = new HashMap<>();

        LaunchFields.params.forEach(param -> {
            fields.put(param, request.getParameter(param));
        });

        boolean success = normalizeLaunch(fields);
        if (success) {
            // TODO launch job
        }
    }

    private boolean normalizeLaunch(HashMap<String, Object> fields) throws IOException {
        if (fields.values().stream().anyMatch(value -> value == null)) {
            response.sendError(400, "Usage: parameters class_name, input_dir, output_dir, map_threads, reduce_threads required");
            return false;
        }

        try {
            Stream.of(LaunchFields.MAP_THREADS, LaunchFields.REDUCE_THREADS).forEach(s -> {
                int number = Integer.parseInt((String) fields.get(s));
                fields.put(s, number);
            });
        } catch (NumberFormatException e) {
            response.sendError(400, "`map_threads`, `reduce_threads` parameters must be numbers");
            return false;
        }

        // TODO normalize dirs and class name

        return true;
    }

    private void getStatus() throws IOException {
        PrintWriter writer = response.getWriter();
        writer.println("<html><head><title>Status</title></head><body>");

        printStatusTable(writer);
        printStatusForm(writer);

        writer.println("</body></html>");
    }

    private void printStatusForm(PrintWriter writer) {
        writer.println("<form action=\"/launch\" method=\"post\">");
        writer.println("<p>Class name: <input type=\"text\" name=\"class_name\"></p>");
        writer.println("<p>Input directory: <input type=\"text\" name=\"input_dir\"></p>");
        writer.println("<p>Output directory: <input type=\"text\" name=\"output_dir\"></p>");
        writer.println("<p>Number of map threads: <input type=\"number\" name=\"map_threads\"></p>");
        writer.println("<p>Number of reduce threads: <input type=\"number\" name=\"reduce_threads\"></p>");
        writer.println("</form>");
    }

    private void printStatusTable(PrintWriter writer) {
        writer.println("<table>");
        writer.println("<thead><tr><th>IP:port</th><th>status</th><th>job</th><th>keys read</th><th>keys written</th></tr></thead>");
        writer.println("<tbody>");

        workers.forEach((k, worker) -> {
            Instant lastActive = (Instant) worker.get(WorkerFields.LAST_ACTIVE);
            if (lastActive.isAfter(Instant.now().minusSeconds(ACTIVE_DURATION))) {
                writer.format(
                        "<tr><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td></tr>\n",
                        worker.get(WorkerFields.ID),
                        worker.get(WorkerFields.STATUS),
                        worker.get(WorkerFields.JOB),
                        worker.get(WorkerFields.KEYS_READ),
                        worker.get(WorkerFields.KEYS_WRITTEN)
                );
            }
        });

        writer.println("</tbody>");
        writer.println("</table>");
    }

    private void getWorkerStatus() throws IOException {
        HashMap<String, Object> workerStatus = new HashMap<>();
        workerStatus.put(WorkerFields.IP, request.getRemoteAddr());
        workerStatus.put(WorkerFields.LAST_ACTIVE, Instant.now());

        WorkerFields.requestsParams.get().forEach(param -> {
            workerStatus.put(param, request.getParameter(param));
        });

        boolean success = normalizeWorkerStatus(workerStatus);
        if (success) {
            String workerId = workerStatus.get(WorkerFields.IP) + ":" + workerStatus.get(WorkerFields.PORT);
            workerStatus.put(WorkerFields.ID, workerId);
            workers.put(workerId, workerStatus);
        }
    }

    private boolean normalizeWorkerStatus(HashMap<String, Object> workerStatus) throws IOException {
        if (workerStatus.values().stream().anyMatch(value -> value == null)) {
            response.sendError(400, "Usage: parameters port, getStatus, job, keysRead, keysWritten, results required");
            return false;
        }

        try {
            Stream.of(WorkerFields.PORT, WorkerFields.KEYS_READ, WorkerFields.KEYS_WRITTEN).forEach(s -> {
                int number = Integer.parseInt((String) workerStatus.get(s));
                workerStatus.put(s, number);
            });
        } catch (NumberFormatException e) {
            response.sendError(400, "`port`, `keysRead`, `keysWritten` parameters must be numbers");
            return false;
        }
        return true;
    }

    private static class WorkerFields {
        final static String ID = "id";
        final static String IP = "ip";
        final static String LAST_ACTIVE = "lastActive";
        final static String PORT = "port";
        final static String STATUS = "getStatus";
        final static String JOB = "job";
        final static String KEYS_READ = "keysRead";
        final static String KEYS_WRITTEN = "keysWritten";
        final static String RESULTS = "results";

        final static Supplier<Stream<String>> requestsParams = () -> Stream.of(PORT, STATUS, JOB, KEYS_READ, KEYS_WRITTEN, RESULTS);
    }

    private static class LaunchFields {
        final static String CLASS_NAME = "class_name";
        final static String INPUT_DIR = "input_dir";
        final static String OUTPUT_DIR = "output_dir";
        final static String MAP_THREADS = "map_threads";
        final static String REDUCE_THREADS = "reduce_threads";

        final static List<String> params = Arrays.asList(CLASS_NAME, INPUT_DIR, OUTPUT_DIR, MAP_THREADS, REDUCE_THREADS);
    }
}
