package com.hazelcast.jet.demo;

import com.hazelcast.com.eclipsesource.json.Json;
import com.hazelcast.com.eclipsesource.json.JsonArray;
import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.com.eclipsesource.json.JsonValue;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import org.apache.log4j.Logger;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.util.stream.Collectors.toList;

/**
 * Polls the <a href="https://www.adsbexchange.com">ADS-B Exchange</a> HTTP API
 * for flight data. The API will be polled every {@code pollIntervalMillis} milliseconds.
 * <p>
 * Fetched aircraft are put into the {@link FlightTelemetry#SOURCE_MAP} IMap
 */
public class FetchAircraft {

    private static final Logger LOGGER = Logger.getLogger(FetchAircraft.class);
    private static final String SOURCE_URL = "https://public-api.adsbexchange.com/VirtualRadar/AircraftList.json";

    private final Map<Long, Long> idToTimestamp = new HashMap<>();
    private final URL url;
    private final IMap<Long, Aircraft> sink;

    FetchAircraft(JetInstance instance) throws MalformedURLException {
        this.url = new URL(SOURCE_URL);
        this.sink = instance.getMap(FlightTelemetry.SOURCE_MAP);
    }

    public void run() throws IOException, InterruptedException {
        while (true) {
            fetchAirCrafts();
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        }
    }

    private void fetchAirCrafts() throws IOException {
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.addRequestProperty("User-Agent", "Mozilla / 5.0 (Windows NT 6.1; WOW64) AppleWebKit / 537.36 (KHTML, like Gecko) Chrome / 40.0.2214.91 Safari / 537.36");
        con.getResponseCode();

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        con.disconnect();

        JsonValue value = Json.parse(response.toString());
        JsonObject object = value.asObject();
        JsonArray acList = object.get("acList").asArray();

        List<Aircraft> newEvents =
                acList.values().stream()
                        .map(this::parseAc)
                        .filter(a -> !isNullOrEmpty(a.getReg())) // there should be a reg number
                        .filter(a -> a.getPosTime() > 0) // there should be a timestamp
                        .filter(a -> {
                            // only emit updated newEvents
                            Long newTs = a.getPosTime();
                            if (newTs <= 0) {
                                return false;
                            }
                            Long oldTs = idToTimestamp.get(a.getId());
                            if (oldTs != null && newTs <= oldTs) {
                                return false;
                            }
                            idToTimestamp.put(a.getId(), newTs);
                            return true;
                        }).collect(toList());

        newEvents.forEach(a -> sink.put(a.getId(), a));

        LOGGER.info("Polled " + acList.size() + " aircraft, " + newEvents.size() + " new locations. map size: " + sink.size());
    }

    private Aircraft parseAc(JsonValue ac) {
        Aircraft aircraft = new Aircraft();
        aircraft.fromJson(ac.asObject());
        return aircraft;
    }

}
