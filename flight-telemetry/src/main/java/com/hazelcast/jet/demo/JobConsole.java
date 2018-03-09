package com.hazelcast.jet.demo;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import org.python.apache.commons.compress.utils.Charsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Supplier;

public class JobConsole implements Runnable {

    private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charsets.UTF_8));

    private final JetInstance jet;

    private final Supplier<Pipeline> pipelineSupplier;

    private Job job;

    public JobConsole(JetInstance jet, Supplier<Pipeline> pipelineSupplier) {
        this.jet = jet;
        this.pipelineSupplier = pipelineSupplier;
    }

    @Override
    public void run() {
        String command;
        while ((command = readCommand()) != null) {
            switch (command) {
                case "submit":
                    submitJob();
                    break;
                case "status":
                    queryJobStatus();
                    break;
                case "cancel":
                    cancelJob();
                    break;
                case "restart":
                    restartJob();
                    break;
                case "":
                    break;
                default:
                    System.err.println("INVALID COMMAND: " + command);
            }
        }
    }

    private String readCommand() {
        try {
            return reader.readLine().trim().toLowerCase();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void submitJob() {
        if (job == null) {
            Pipeline pipeline = pipelineSupplier.get();
            job = jet.newJob(pipeline);
        } else {
            System.err.println("JOB ALREADY SUBMITTED");
        }
    }

    private void queryJobStatus() {
        if (job != null) {
            System.out.println("JOB STATUS: " + job.getStatus());
        } else {
            System.err.println("JOB NOT SUBMITTED");
        }
    }

    private void cancelJob() {
        if (job != null) {
            if (job.cancel()) {
                System.out.println("JOB IS CANCELLED.");
            } else {
                System.err.println("JOB IS NOT CANCELLED.");
            }
        } else {
            System.err.println("JOB NOT SUBMITTED");
        }
    }

    private void restartJob() {
        if (job != null) {
            if (job.restart()) {
                System.out.println("JOB IS RESTARTING.");
            } else {
                System.err.println("JOB IS NOT RESTARTED.");
            }
        } else {
            System.err.println("JOB NOT SUBMITTED");
        }
    }
}
