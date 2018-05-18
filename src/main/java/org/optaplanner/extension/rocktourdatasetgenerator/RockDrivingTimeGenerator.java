/*
 * Copyright 2018 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.optaplanner.extension.rocktourdatasetgenerator;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;
import org.apache.commons.lang3.tuple.Pair;
import org.optaplanner.examples.common.app.LoggingMain;
import org.optaplanner.examples.vehiclerouting.domain.location.Location;
import org.optaplanner.extension.vrpdatasetgenerator.GenerationDistanceType;

public class RockDrivingTimeGenerator extends LoggingMain {

    public static void main(String[] args) {
        File inputFile = new File("data/rocktour/xlsxDrivingTimeSheetInput.txt");
        File outputFile = new File("data/rocktour/xlsxDrivingTimeSheetOutput.txt");
        String osmPath = "local/osm/north-america-latest.osm.pbf";
        new RockDrivingTimeGenerator(inputFile, outputFile, osmPath, "USA").generate();
    }

    private final File inputFile;
    private final File outputFile;

    private final GraphHopperOSM graphHopper;

    public RockDrivingTimeGenerator(File inputFile, File outputFile, String osmPath, String dataSourceName) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        if (!new File(osmPath).exists()) {
            throw new IllegalStateException("The osmPath (" + osmPath + ") does not exist.\n" +
                    "Download the osm file from http://download.geofabrik.de/ first.");
        }
        graphHopper = (GraphHopperOSM) new GraphHopperOSM().forServer();
        graphHopper.setOSMFile(osmPath);
        graphHopper.setGraphHopperLocation("local/graphHopper/" + dataSourceName);
        graphHopper.setEncodingManager(new EncodingManager("car"));
        logger.info("graphHopper loading...");
        graphHopper.importOrLoad();
        logger.info("graphHopper loaded.");
    }

    public void generate() {
        List<Pair<Double, Double>> latLongList = new ArrayList<>(100);
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            if (!reader.readLine().contains("Driving time")) {
                throw new IllegalArgumentException("The inputFile (" + inputFile + ")'s first line is incorrect.");
            }
            if (!reader.readLine().contains("Latitude")) {
                throw new IllegalArgumentException("The inputFile (" + inputFile + ")'s second line is incorrect.");
            }
            if (!reader.readLine().contains("Longitude")) {
                throw new IllegalArgumentException("The inputFile (" + inputFile + ")'s third line is incorrect.");
            }
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String[] tokens = line.split("\\s+");
                latLongList.add(Pair.of(Double.parseDouble(tokens[0]), Double.parseDouble(tokens[1])));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read inputFile (" + inputFile + ").", e);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.append("Driving time in seconds. Delete this sheet to generate it from air distances.\n");
            writer.append("Latitude\t");
            for (Pair<Double, Double> latLong : latLongList) {
                writer.append("\t").append(Double.toString(latLong.getLeft()));
            }
            writer.append("\n");
            writer.append("\tLongitude");
            for (Pair<Double, Double> latLong : latLongList) {
                writer.append("\t").append(Double.toString(latLong.getRight()));
            }
            writer.append("\n");
            for (Pair<Double, Double> fromLatLong : latLongList) {
                double fromLatitude = fromLatLong.getLeft();
                double fromLongitude = fromLatLong.getRight();
                writer.append(Double.toString(fromLatitude));
                writer.append("\t").append(Double.toString(fromLongitude));
                for (Pair<Double, Double> toLatLong : latLongList) {
                    double toLatitude = toLatLong.getLeft();
                    double toLongitude = toLatLong.getRight();
                    long drivingSeconds = fetchDrivingTime(fromLatitude, fromLongitude, toLatitude, toLongitude);
                    writer.append("\t").append(Long.toString(drivingSeconds));
                }
                writer.append("\n");
            }

        } catch (IOException e) {
            throw new IllegalArgumentException("Could not write outputFile (" + outputFile + ").", e);
        }
    }

    private long fetchDrivingTime(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude) {
        if (fromLatitude == toLatitude && fromLongitude == toLongitude) {
            return 0L;
        }
        GHRequest request = new GHRequest(fromLatitude, fromLongitude, toLatitude, toLongitude)
                .setWeighting("fastest")
                .setVehicle("car");
        GHResponse response = graphHopper.route(request);
        if (response.hasErrors()) {
            throw new IllegalStateException("GraphHopper gave " + response.getErrors().size()
                    + " errors to go from (" + fromLatitude + ", " + fromLongitude
                    + ") to (" + toLatitude + ", " + toLongitude + "). First error chained.",
                    response.getErrors().get(0)
            );
        }
        return response.getBest().getTime() / 1000L;
    }

}
