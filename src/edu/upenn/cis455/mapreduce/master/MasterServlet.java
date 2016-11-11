package edu.upenn.cis455.mapreduce.master;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static edu.upenn.cis455.mapreduce.master.MasterStatus.getWorkerStatus;

public class MasterServlet extends HttpServlet {

    final static long serialVersionUID = 455555001;

    private WorkersMap workers = new WorkersMap();

//    static Logger log = Logger.getLogger(MasterServlet.class);

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
        switch (request.getPathInfo()) {
            case "/status":
                MasterStatus.getStatus(request, response, workers);
                break;
            case "/workerstatus":
                getWorkerStatus(request, response, workers);
                break;
            default:
                response.sendError(404);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        switch (request.getPathInfo()) {
            case "/launch":
                MasterLaunch.postLaunch(request, response, workers);
                break;
            default:
                response.sendError(404);
        }
    }
}
