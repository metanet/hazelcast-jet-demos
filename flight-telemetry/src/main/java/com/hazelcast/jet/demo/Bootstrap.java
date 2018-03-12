package com.hazelcast.jet.demo;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;

import java.io.IOException;

public class Bootstrap {

    private static final GroupConfig GROUP_CONFIG = new GroupConfig("flight", "demo");

    static {
        System.setProperty("hazelcast.logging.type", "log4j");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        switch (System.getProperty("type", "").trim().toLowerCase()) {
            case "server":
                startJetServer();
                break;
            case "client":
                startJetClient();
                break;
            case "fetcher":
                startFetcher();
                break;
            default:
                System.err.println("Missing system property: type=server|client|fetcher");
        }
    }

    private static void startJetServer() {
        JetConfig config = FlightTelemetry.buildJetConfig();
        config.getHazelcastConfig().setGroupConfig(GROUP_CONFIG);
        JetInstance jet = Jet.newJetInstance(config);

        try {
            JobConsole c = new JobConsole(jet, FlightTelemetry.buildPipeline(jet), FlightTelemetry.buildJobConfig());
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
            JobConsole c = new JobConsole(jet, FlightTelemetry.buildPipeline(jet), FlightTelemetry.buildJobConfig());
            c.run();
        } finally {
            jet.getHazelcastInstance().getLifecycleService().terminate();
        }
    }

    private static void startFetcher() throws IOException, InterruptedException {
        ClientConfig config = new ClientConfig();
        config.setGroupConfig(GROUP_CONFIG);
        JetInstance jet = Jet.newJetClient(config);

        FetchAircraft fetcher = new FetchAircraft(jet);
        fetcher.run();
    }

}
