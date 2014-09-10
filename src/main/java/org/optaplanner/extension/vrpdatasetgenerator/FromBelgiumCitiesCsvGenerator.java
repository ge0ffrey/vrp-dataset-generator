/*
 * Copyright 2014 JBoss Inc
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

package org.optaplanner.extension.vrpdatasetgenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.EncodingManager;
import org.apache.commons.io.IOUtils;
import org.optaplanner.examples.common.app.LoggingMain;
import org.optaplanner.examples.vehiclerouting.domain.location.AirLocation;
import org.optaplanner.examples.vehiclerouting.domain.location.Location;
import org.optaplanner.examples.vehiclerouting.persistence.VehicleRoutingDao;

public class FromBelgiumCitiesCsvGenerator extends LoggingMain {

    public static void main(String[] args) {
        new FromBelgiumCitiesCsvGenerator().generate();
    }

    protected final VehicleRoutingDao vehicleRoutingDao;

    private final GraphHopper graphHopper;

    public FromBelgiumCitiesCsvGenerator() {
        vehicleRoutingDao = new VehicleRoutingDao();

        graphHopper = new GraphHopper().forServer();
        String osmPath = "local/osm/belgium-latest.osm.pbf";
        if (!new File(osmPath).exists()) {
            throw new IllegalStateException("The osmPath (" + osmPath + ") does not exist.\n" +
                    "Download the osm file from http://download.geofabrik.de/ first.");
        }
        graphHopper.setOSMFile(osmPath);
        graphHopper.setGraphHopperLocation("local/graphhopper");
        graphHopper.setEncodingManager(new EncodingManager(EncodingManager.CAR));
        graphHopper.importOrLoad();
        logger.info("GraphHopper loaded.");
    }

    public void generate() {
//        // Air
//        generateVrp(new File("data/raw/belgium-cities.csv"), 50, 10, 125, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 100, 10, 250, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 500, 20, 250, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 1000, 20, 500, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 2750, 55, 500, false, false);
//        // Road
//        generateVrp(new File("data/raw/belgium-cities.csv"), 50, 10, 125, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 100, 10, 250, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 500, 20, 250, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 1000, 20, 500, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), 2750, 55, 500, true, false);
        // Segmented road
        generateVrp(new File("data/raw/belgium-cities.csv"), 50, 10, 125, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), 100, 10, 250, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), 500, 20, 250, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), 1000, 20, 500, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), 2750, 55, 500, true, true);
    }

    public void generateVrp(File locationFile, int locationListSize, int vehicleListSize, int capacity, boolean road, boolean segmented) {
        // WARNING: this code is DIRTY.
        // It's JUST good enough to generate the Belgium datasets.
        String suffix = road ? (segmented ? "-segmentedRoad" : "-road") : "";
        String name = locationFile.getName().replaceAll("\\-cities.csv", "")
                + suffix + "-n" + locationListSize + "-k" + vehicleListSize;
        File vrpOutputFile = new File(vehicleRoutingDao.getDataDir(), "import"
                + (road ? "/roaddistance" : "")
                + "/capacitated/" + name + ".vrp");
        if (!vrpOutputFile.getParentFile().exists()) {
            throw new IllegalArgumentException("The vrpOutputFile parent directory (" + vrpOutputFile.getParentFile()
                    + ") does not exist.");
        }
        List<Location> locationList = selectLocationSubList(locationFile, locationListSize);
        BufferedWriter vrpWriter = null;
        try {
            vrpWriter = writeHeaders(vrpWriter, locationListSize, capacity, road, name, vrpOutputFile);
            writeNodeCoordSection(vrpWriter, locationList);
            writeEdgeWeightSection(vrpWriter, road, locationList);
            writeDemandSection(vrpWriter, locationListSize, vehicleListSize, capacity);
            writeDepotSection(vrpWriter);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the locationFile (" + locationFile.getName()
                    + ") or write the vrpOutputFile (" + vrpOutputFile.getName() + ").", e);
        } finally {
            IOUtils.closeQuietly(vrpWriter);
        }
        logger.info("Generated: {}", vrpOutputFile);
    }

    private BufferedWriter writeHeaders(BufferedWriter vrpWriter, int locationListSize, int capacity, boolean road, String name, File vrpOutputFile) throws IOException {
        vrpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vrpOutputFile), "UTF-8"));
        vrpWriter.write("NAME: " + name + "\n");
        vrpWriter.write("COMMENT: Generated for OptaPlanner. Road distance calculated with GraphHopper.\n");
        vrpWriter.write("TYPE: CVRP\n");
        vrpWriter.write("DIMENSION: " + locationListSize + "\n");
        if (road) {
            vrpWriter.write("EDGE_WEIGHT_TYPE: EXPLICIT\n");
            vrpWriter.write("EDGE_WEIGHT_FORMAT: FULL_MATRIX\n");
        } else {
            vrpWriter.write("EDGE_WEIGHT_TYPE: EUC_2D\n");
        }
        vrpWriter.write("CAPACITY: " + capacity + "\n");
        return vrpWriter;
    }

    private List<Location> selectLocationSubList(File locationFile, double locationListSize) {
        List<AirLocation> airLocationList = readAirLocationFile(locationFile);
        if (locationListSize > airLocationList.size()) {
            throw new IllegalArgumentException("The locationListSize (" + locationListSize
                    + ") is larger than the airLocationList size (" + airLocationList.size() + ").");
        }
        double selectionDecrement = locationListSize / (double) airLocationList.size();
        double selection = locationListSize;
        List<Location> newLocationList = new ArrayList<Location>(airLocationList.size());
        for (AirLocation location : airLocationList) {
            double newSelection = selection - selectionDecrement;
            if ((int) newSelection < (int) selection) {
                newLocationList.add(location);
            }
            selection = newSelection;
        }
        return newLocationList;
    }

    private void writeNodeCoordSection(BufferedWriter vrpWriter, List<Location> locationList) throws IOException {
        vrpWriter.write("NODE_COORD_SECTION\n");
        int id = 1;
        for (Location location : locationList) {
            vrpWriter.write(id + " " + location.getLatitude() + " " + location.getLongitude()
                    + (location.getName() != null ? " " + location.getName().replaceAll(" ", "_") : "") + "\n");
            id++;
        }
    }

    private void writeEdgeWeightSection(BufferedWriter vrpWriter, boolean road, List<Location> locationList) throws IOException {
        if (road) {
            vrpWriter.write("EDGE_WEIGHT_SECTION\n");
            DecimalFormat distanceFormat = new DecimalFormat("0.000");
            for (Location fromAirLocation : locationList) {
                for (Location toAirLocation : locationList) {
                    double distance;
                    if (fromAirLocation == toAirLocation) {
                        distance = 0.0;
                    } else {
                        GHRequest request = new GHRequest(fromAirLocation.getLatitude(), fromAirLocation.getLongitude(),
                                toAirLocation.getLatitude(), toAirLocation.getLongitude())
                                .setVehicle("car");
                        GHResponse response = graphHopper.route(request);
                        if (response.hasErrors()) {
                            throw new IllegalStateException("GraphHopper gave " + response.getErrors().size()
                                    + " errors. First error chained.",
                                    response.getErrors().get(0)
                            );
                        }
                        // Distance should be in km, not meter
                        distance = response.getDistance() / 1000.0;
                        if (distance == 0.0) {
                            throw new IllegalArgumentException("The fromAirLocation (" + fromAirLocation
                                    + ") and toAirLocation (" + toAirLocation + ") are the same.");
                        }
                    }
                    vrpWriter.write(distanceFormat.format(distance) + " ");
                }
                vrpWriter.write("\n");
                logger.info("All distances calculated for location ({}).", fromAirLocation);
            }
        } else {
            for (Location fromAirLocation : locationList) {
                for (Location toAirLocation : locationList) {
                    if (fromAirLocation != toAirLocation && fromAirLocation.getDistance(toAirLocation) == 0) {
                        throw new IllegalArgumentException("The fromAirLocation (" + fromAirLocation
                                + ") and toAirLocation (" + toAirLocation + ") are the same.");
                    }
                }
            }

        }
    }

    private void writeDemandSection(BufferedWriter vrpWriter, int locationListSize, int vehicleListSize, int capacity) throws IOException {
        vrpWriter.write("DEMAND_SECTION\n");
        // maximumDemand is 2 times the averageDemand. And the averageDemand is 2/3th of available capacity
        int maximumDemand = (4 * vehicleListSize * capacity) / (locationListSize * 3);
        Random random = new Random(37);
        vrpWriter.write("1 0\n");
        for (int i = 2; i <= locationListSize; i++) {
            vrpWriter.write(i + " " + (random.nextInt(maximumDemand) + 1) + "\n");
        }
    }

    private void writeDepotSection(BufferedWriter vrpWriter) throws IOException {
        vrpWriter.write("DEPOT_SECTION\n");
        vrpWriter.write("1\n");
        vrpWriter.write("-1\n");
        vrpWriter.write("EOF\n");
    }

    private List<AirLocation> readAirLocationFile(File locationFile) {
        List<AirLocation> locationList = new ArrayList<AirLocation>(3000);
        BufferedReader bufferedReader = null;
        long id = 0L;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(locationFile), "UTF-8"));
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                String[] tokens = line.split(";");
                if (tokens.length != 5) {
                    throw new IllegalArgumentException("The line (" + line + ") does not have 5 tokens ("
                            + tokens.length + ").");
                }
                AirLocation location = new AirLocation();
                location.setId(id);
                id++;
                location.setLatitude(Double.parseDouble(tokens[2]));
                location.setLongitude(Double.parseDouble(tokens[3]));
                location.setName(tokens[4]);
                locationList.add(location);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the locationFile (" + locationFile + ").", e);
        } finally {
            IOUtils.closeQuietly(bufferedReader);
        }
        logger.info("Read {} cities.", locationList.size());
        return locationList;
    }

}
