package com.hazelcast.jet.demo;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.log4j.Logger;
import org.python.apache.commons.compress.utils.Charsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JobConsole implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(JobConsole.class);


    private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charsets.UTF_8));

    private final JetInstance jet;

    private final JobConfig jobConfig;

    private final Pipeline pipeline;

    private volatile Job job;

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
        LOGGER.info("Job console has started.");
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
                case "delete":
                    deleteJob();
                    break;
                case "":
                    break;
                default:
                    LOGGER.error("INVALID COMMAND: " + command
                            + ". VALID COMMANDS: submit|get|status|cancel|restart|delete");
            }
        }
    }

    private String readCommand() {
        try {
            return reader.readLine().trim().toLowerCase();
        } catch (IOException e) {
            LOGGER.error("Error while reading command", e);
            return null;
        }
    }

    private void submitJob() {
        if (job == null) {
            job = jet.newJob(pipeline, jobConfig);
        } else {
            LOGGER.error("There is already a job reference.");
        }
    }

    private void getJob() {
        if (job == null) {
            job = jet.getJob(jobConfig.getName());
            if (job != null) {
                LOGGER.info("Job reference is fetched.");
            } else {
                LOGGER.error("Job not found.");
            }
        } else {
            LOGGER.error("There is already a job reference.");
        }
    }

    private void queryJobStatus() {
        if (job != null) {
            LOGGER.info("Jon status: " + job.getStatus());
        } else {
            LOGGER.error("There is no job reference.");
        }
    }

    private void cancelJob() {
        if (job != null) {
            if (job.cancel()) {
                LOGGER.info("Job is cancelled.");
            } else {
                LOGGER.error("Job is not cancelled.");
            }
        } else {
            LOGGER.error("There is no job reference.");
        }
    }

    private void restartJob() {
        if (job != null) {
            if (job.restart()) {
                LOGGER.info("Job is restarting.");
            } else {
                LOGGER.error("Job is not restarted.");
            }
        } else {
            LOGGER.error("There is no job reference.");
        }
    }

    private void deleteJob() {
        if (job != null) {
            job = null;
            LOGGER.info("Job reference is deleted.");
        } else {
            LOGGER.error("There is no job reference.");
        }
    }

}
