package com.hazelcast.jet.demo;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

public class StartJetClient {


    static {
        System.setProperty("hazelcast.logging.type", "log4j");
    }

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.setGroupConfig(new GroupConfig("ensar-dev", "basri-dev"));
        JetInstance jet = Jet.newJetClient(config);

        try {
            JobConsole console = new JobConsole(jet, FlightTelemetry::buildPipeline);
            console.run();
        } finally {
            jet.getHazelcastInstance().getLifecycleService().terminate();
        }
    }

}
