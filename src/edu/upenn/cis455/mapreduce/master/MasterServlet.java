package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

    final static long serialVersionUID = 455555001;

    private HttpServletRequest request;
    private HttpServletResponse response;

    private Map<String, Map<String, Object>> workers = new HashMap<>();

    private static class WorkerFields {
        final static String ID = "id";
        final static String IP = "ip";
        final static String LAST_ACTIVE = "lastActive";
        final static String PORT = "port";
        final static String STATUS = "status";
        final static String JOB = "job";
        final static String KEYS_READ = "keysRead";
        final static String KEYS_WRITTEN = "keysWritten";
        final static String RESULTS = "results";

        final static Stream<String> requestsParams = Stream.of(PORT, STATUS, JOB, KEYS_READ, KEYS_WRITTEN, RESULTS);
    }

    private final int ACTIVE_DURATION = 30;

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
        this.request = request;
        this.response = response;

        switch (request.getPathInfo()) {
            case "/status":
                status();
            case "/workerstatus":
                workerStatus();
            default:
                response.sendError(404);
        }
    }

    private void status() throws IOException {
        PrintWriter writer = response.getWriter();
        writer.println("<html><head><title>Status</title></head><body>");
        writer.println("<table>");
        writer.println("<thead><tr><th>IP:port</th><th>status</th><th>job</th><th>keys read</th><th>keys written</th></tr></thead>");
        writer.println("<tbody>");

        workers.forEach((k, worker) -> {
            Instant lastActive = (Instant) worker.get(WorkerFields.LAST_ACTIVE);
            if (lastActive.isBefore(Instant.now().minusSeconds(ACTIVE_DURATION))) {
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
        writer.println("</body></html>");
    }

    private void workerStatus() throws IOException {
        HashMap<String, Object> workerStatus = new HashMap<>();
        workerStatus.put(WorkerFields.IP, request.getRemoteAddr());
        workerStatus.put(WorkerFields.LAST_ACTIVE, Instant.now());
        WorkerFields.requestsParams.forEach(param -> {
            workerStatus.put(param, request.getParameter(param));
        });

        boolean success = normalize(workerStatus);
        if (success) {
            String workerId = (String) workerStatus.get(WorkerFields.IP + ":" + WorkerFields.PORT);
            workerStatus.put(WorkerFields.ID, workerId);
            workers.put(workerId, workerStatus);
        }
    }

    private boolean normalize(HashMap<String, Object> workerStatus) throws IOException {
        if (workerStatus.values().stream().anyMatch(value -> value == null)) {
            response.sendError(400, "Usage: parameters port, status, job, keysRead, keysWritten, results required");
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
}
