package com.hazelcast.jet.demo;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;

import java.io.IOException;

import static com.hazelcast.jet.demo.ReplayableFlightDataSource.buildJobConfig;
import static com.hazelcast.jet.demo.FlightTelemetry.buildPipeline;

public class Bootstrap {

    private static final GroupConfig GROUP_CONFIG = new GroupConfig("flight", "demo");

    static {
        System.setProperty("hazelcast.logging.type", "log4j");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String type = System.getProperty("type", "default").trim().toLowerCase();
        switch (type) {
            case "default":
                runFlightTelemetryJob();
                break;
            case "server":
                startJetServer();
                break;
            case "client":
                startJetClient();
                break;
            case "source":
                startSource();
                break;
            default:
                System.err.println("Invalid type: " + type + "available options: type=server|client|source");
        }
    }

    private static void runFlightTelemetryJob() {
        JetInstance jet = Jet.newJetInstance();
        Pipeline p = buildPipeline(jet, FlightDataSource.streamAircraft(10000));

        try {
            Job job = jet.newJob(p);
            job.join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void startJetServer() {
        JetConfig config = ReplayableFlightDataSource.buildJetConfig();
        config.getHazelcastConfig().setGroupConfig(GROUP_CONFIG);
        JetInstance jet = Jet.newJetInstance(config);

        try {
            JobConsole c = new JobConsole(jet, buildPipeline(jet, ReplayableFlightDataSource.buildSource()), buildJobConfig());
            c.run();
        } finally {
            jet.getHazelcastInstance().getLifecycleService().terminate();
        }
    }

    private static void startJetClient() {
        ClientConfig config = new ClientConfig();
        config.setGroupConfig(GROUP_CONFIG);
        JetInstance jet = Jet.newJetClient(config);

        try {
            JobConsole c = new JobConsole(jet, buildPipeline(jet, ReplayableFlightDataSource.buildSource()), buildJobConfig());
            c.run();
        } finally {
            jet.getHazelcastInstance().getLifecycleService().terminate();
        }
    }

    private static void startSource() throws IOException, InterruptedException {
        ClientConfig config = new ClientConfig();
        config.setGroupConfig(GROUP_CONFIG);
        JetInstance jet = Jet.newJetClient(config);

        FetchAircraft fetcher = new FetchAircraft(jet);
        fetcher.run();
    }

}
