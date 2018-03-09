package com.hazelcast.jet.demo;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;

public class StartJetInstance {

    static {
        System.setProperty("hazelcast.logging.type", "log4j");
    }

    public static void main(String[] args) {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setGroupConfig(new GroupConfig("ensar-dev", "basri-dev"));
        JetInstance jet = Jet.newJetInstance(config);

        try {
            JobConsole console = new JobConsole(jet, FlightTelemetry::buildPipeline);
            console.run();
        } finally {
            jet.getHazelcastInstance().getLifecycleService().terminate();
        }
    }

}
