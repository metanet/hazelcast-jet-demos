package com.hazelcast.jet.demo;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import org.python.apache.commons.compress.utils.Charsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JobConsole implements Runnable {

    private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charsets.UTF_8));

    private final JetInstance jet;

    private final JobConfig jobConfig;

    private final Pipeline pipeline;

    private Job job;

    JobConsole(JetInstance jet, Pipeline pipeline, JobConfig jobConfig) {
        this.jet = jet;
        this.pipeline = pipeline;
        this.jobConfig = jobConfig;
        if (this.jobConfig.getName() == null) {
            throw new IllegalArgumentException("The job has no name!");
        }
    }

    @Override
    public void run() {
        String command;
        while ((command = readCommand()) != null) {
            switch (command) {
                case "submit":
                    submitJob();
                    break;
                case "get":
                    getJob();
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
            job = jet.newJob(pipeline, jobConfig);
        } else {
            System.err.println("JOB ALREADY SUBMITTED");
        }
    }

    private void getJob() {
        if (job == null) {
            job = jet.getJob(jobConfig.getName());
            if (job != null) {
                System.out.println("JOB IS GOT");
            } else {
                System.err.println("JOB NOT FOUND");
            }
        } else {
            System.err.println("JOB ALREADY GOT");
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
