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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.util.PointList;
import org.apache.commons.io.IOUtils;
import org.optaplanner.examples.common.app.LoggingMain;
import org.optaplanner.examples.vehiclerouting.domain.location.AirLocation;
import org.optaplanner.examples.vehiclerouting.domain.location.Location;
import org.optaplanner.examples.vehiclerouting.domain.location.RoadLocation;
import org.optaplanner.examples.vehiclerouting.domain.location.segmented.HubSegmentLocation;
import org.optaplanner.examples.vehiclerouting.domain.location.segmented.RoadSegmentLocation;
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
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 50, 10, 125, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 100, 10, 250, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 500, 20, 250, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 1000, 20, 500, false, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 2750, 55, 500, false, false);
//        // Road
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 50, 10, 125, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 100, 10, 250, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 500, 20, 250, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 1000, 20, 500, true, false);
//        generateVrp(new File("data/raw/belgium-cities.csv"), null, 2750, 55, 500, true, false);
        // Segmented road
        generateVrp(new File("data/raw/belgium-cities.csv"), new File("data/raw/belgium-hubs.txt"), 50, 10, 125, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), new File("data/raw/belgium-hubs.txt"), 100, 10, 250, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), new File("data/raw/belgium-hubs.txt"), 500, 20, 250, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), new File("data/raw/belgium-hubs.txt"), 1000, 20, 500, true, true);
        generateVrp(new File("data/raw/belgium-cities.csv"), new File("data/raw/belgium-hubs.txt"), 2750, 55, 500, true, true);
    }

    public void generateVrp(File locationFile, File hubFile, int locationListSize, int vehicleListSize, int capacity, boolean road, boolean segmented) {
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
        List<HubSegmentLocation> hubList = readHubList(hubFile, segmented);
        List<Location> locationList = selectLocationSubList(locationFile, locationListSize, hubList.size(), road, segmented);
        BufferedWriter vrpWriter = null;
        try {
            vrpWriter = writeHeaders(vrpWriter, locationListSize, capacity, road, segmented, name, vrpOutputFile);
            writeHubCoordSection(vrpWriter, segmented, hubList);
            writeNodeCoordSection(vrpWriter, locationList);
            writeEdgeWeightSection(vrpWriter, road, segmented, hubList, locationList);
            writeDemandSection(vrpWriter, locationListSize, vehicleListSize, capacity, locationList);
            writeDepotSection(vrpWriter, locationList);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the locationFile (" + locationFile.getName()
                    + ") or write the vrpOutputFile (" + vrpOutputFile.getName() + ").", e);
        } finally {
            IOUtils.closeQuietly(vrpWriter);
        }
        logger.info("Generated: {}", vrpOutputFile);
    }

    private BufferedWriter writeHeaders(BufferedWriter vrpWriter, int locationListSize, int capacity, boolean road, boolean segmented, String name, File vrpOutputFile) throws IOException {
        vrpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vrpOutputFile), "UTF-8"));
        vrpWriter.write("NAME: " + name + "\n");
        vrpWriter.write("COMMENT: Generated for OptaPlanner. Road distance calculated with GraphHopper.\n");
        vrpWriter.write("TYPE: CVRP\n");
        vrpWriter.write("DIMENSION: " + locationListSize + "\n");
        if (road) {
            if (segmented) {
                vrpWriter.write("EDGE_WEIGHT_TYPE: SEGMENTED_EXPLICIT\n");
                vrpWriter.write("EDGE_WEIGHT_FORMAT: HUB_AND_NEARBY_MATRIX\n");
            } else {
                vrpWriter.write("EDGE_WEIGHT_TYPE: EXPLICIT\n");
                vrpWriter.write("EDGE_WEIGHT_FORMAT: FULL_MATRIX\n");
            }
        } else {
            vrpWriter.write("EDGE_WEIGHT_TYPE: EUC_2D\n");
        }
        vrpWriter.write("CAPACITY: " + capacity + "\n");
        return vrpWriter;
    }

    private void writeHubCoordSection(BufferedWriter vrpWriter, boolean segmented, List<HubSegmentLocation> hubList) throws IOException {
        if (!segmented) {
            return;
        }
        vrpWriter.write("HUBS: " + hubList.size() + "\n");
        vrpWriter.write("HUB_COORD_SECTION\n");
        for (HubSegmentLocation hub : hubList) {
            vrpWriter.write(hub.getId() + " " + hub.getLatitude() + " " + hub.getLongitude()
                    + (hub.getName() != null ? " " + hub.getName().replaceAll(" ", "_") : "") + "\n");
        }
    }

    private List<Location> selectLocationSubList(File locationFile, double locationListSize, long startId, boolean road, boolean segmented) {
        List<AirLocation> airLocationList = readAirLocationFile(locationFile, startId);
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
                Location newLocation = road ? (segmented ?
                        new RoadSegmentLocation(location.getId(), location.getLatitude(), location.getLongitude())
                        : new RoadLocation(location.getId(), location.getLatitude(), location.getLongitude()))
                        : new AirLocation(location.getId(), location.getLatitude(), location.getLongitude());
                newLocationList.add(newLocation);
            }
            selection = newSelection;
        }
        return newLocationList;
    }

    private void writeNodeCoordSection(BufferedWriter vrpWriter, List<Location> locationList) throws IOException {
        vrpWriter.write("NODE_COORD_SECTION\n");
        for (Location location : locationList) {
            vrpWriter.write(location.getId() + " " + location.getLatitude() + " " + location.getLongitude()
                    + (location.getName() != null ? " " + location.getName().replaceAll(" ", "_") : "") + "\n");
        }
    }

    private void writeEdgeWeightSection(BufferedWriter vrpWriter, boolean road, boolean segmented, List<HubSegmentLocation> hubList, List<Location> locationList) throws IOException {
        if (road) {
            DecimalFormat distanceFormat = new DecimalFormat("0.000");
            if (!segmented) {
                vrpWriter.write("EDGE_WEIGHT_SECTION\n");
                for (Location fromAirLocation : locationList) {
                    for (Location toAirLocation : locationList) {
                        double distance;
                        if (fromAirLocation == toAirLocation) {
                            distance = 0.0;
                        } else {
                            GHResponse response = fetchGhResponse(fromAirLocation, toAirLocation);
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
                for (HubSegmentLocation fromHubLocation : hubList) {
                    Map<HubSegmentLocation, Double> fromHubTravelDistanceMap = new LinkedHashMap<HubSegmentLocation, Double>(hubList.size());
                    fromHubLocation.setHubTravelDistanceMap(fromHubTravelDistanceMap);
                    for (HubSegmentLocation toHubLocation : hubList) {
                        if (fromHubLocation == toHubLocation) {
                            continue;
                        }
                        GHResponse response = fetchGhResponse(fromHubLocation, toHubLocation);
                        // Distance should be in km, not meter
                        double distance = response.getDistance() / 1000.0;
                        if (distance == 0.0) {
                            throw new IllegalArgumentException("The fromHubLocation (" + fromHubLocation
                                    + ") and toHubLocation (" + toHubLocation + ") are the same.");
                        }
                        fromHubTravelDistanceMap.put(toHubLocation, distance);
                    }
                    logger.info("All hub distances calculated for hub ({}).", fromHubLocation);
                }
                Map<Point, HubSegmentLocation> pointToHubMap = new HashMap<Point, HubSegmentLocation>(hubList.size());
                for (HubSegmentLocation hub : hubList) {
                    hub.setNearbyTravelDistanceMap(new LinkedHashMap<RoadSegmentLocation, Double>(100));
                    pointToHubMap.put(new Point(hub.getLatitude(), hub.getLongitude()), hub);
                }
                for (Location fromLocationG : locationList) {
                    RoadSegmentLocation fromLocation = (RoadSegmentLocation) fromLocationG;
                    Map<RoadSegmentLocation, Double> fromNearbyTravelDistanceMap = new LinkedHashMap<RoadSegmentLocation, Double>();
                    fromLocation.setNearbyTravelDistanceMap(fromNearbyTravelDistanceMap);
                    Map<HubSegmentLocation, Double> fromHubTravelDistanceMap = new LinkedHashMap<HubSegmentLocation, Double>();
                    fromLocation.setHubTravelDistanceMap(fromHubTravelDistanceMap);
                    for (Location toLocationG : locationList) {
                        RoadSegmentLocation toLocation = (RoadSegmentLocation) toLocationG;
                        if (fromLocation == toLocation) {
                            continue;
                        }
                        GHResponse response = fetchGhResponse(fromLocation, toLocation);
                        // Distance should be in km, not meter
                        double distance = response.getDistance() / 1000.0;
                        if (distance == 0.0) {
                            throw new IllegalArgumentException("The fromLocation (" + fromLocation
                                    + ") and toLocation (" + toLocation + ") are the same.");
                        }
                        PointList ghPointList = response.getPoints();
                        HubSegmentLocation firstHub = null;
                        for (int i = 0; i < ghPointList.size(); i++) {
                            double latitude = ghPointList.getLatitude(i);
                            double longitude = ghPointList.getLongitude(i);
                            HubSegmentLocation hub = pointToHubMap.get(new Point(latitude, longitude));
                            if (hub != null) {
                                firstHub = hub;
                                break;
                            }
                        }
                        HubSegmentLocation lastHub = null;
                        for (int i = ghPointList.size() - 1; i >= 0; i--) {
                            double latitude = ghPointList.getLatitude(i);
                            double longitude = ghPointList.getLongitude(i);
                            HubSegmentLocation hub = pointToHubMap.get(new Point(latitude, longitude));
                            if (hub != null) {
                                lastHub = hub;
                                break;
                            }
                        }
                        if (firstHub == null && lastHub == null) {
                            fromNearbyTravelDistanceMap.put(toLocation, distance);
                        } else {
                            GHResponse firstResponse = fetchGhResponse(fromLocation, firstHub);
                            // Distance should be in km, not meter
                            double firstHubDistance = firstResponse.getDistance() / 1000.0;
                            fromHubTravelDistanceMap.put(firstHub, firstHubDistance);
                            GHResponse lastResponse = fetchGhResponse(lastHub, toLocation);
                            // Distance should be in km, not meter
                            double lastHubDistance = lastResponse.getDistance() / 1000.0;
                            lastHub.getNearbyTravelDistanceMap().put(toLocation, lastHubDistance);
                            double segmentedDistance = fromLocation.getDistanceDouble(toLocation);
                            double distanceDiff = distance - segmentedDistance;
                            if (distanceDiff > 0.001) {
                                logger.warn("The distance ({}) is bigger than the segmentedDistance ({}). "
                                        + "It found a shortcut from {} to {}.",
                                        distance, segmentedDistance, fromLocation, toLocation);
                            } else if (distanceDiff < -0.001) {
                                throw new IllegalArgumentException("The distance (" + distance
                                        + ") is much smaller than the segmentedDistance (" + segmentedDistance + ").");
                            }
                        }
                    }
                    logger.info("All distances calculated for location ({}).", fromLocation);
                }
                vrpWriter.write("SEGMENTED_EDGE_WEIGHT_SECTION\n");
                for (HubSegmentLocation fromHub : hubList) {
                    vrpWriter.write(fromHub.getId() + " ");
                    for (Map.Entry<HubSegmentLocation, Double> entry : fromHub.getHubTravelDistanceMap().entrySet()) {
                        vrpWriter.write(entry.getKey().getId() + " " + distanceFormat.format(entry.getValue()) + " ");
                    }
                    for (Map.Entry<RoadSegmentLocation, Double> entry : fromHub.getNearbyTravelDistanceMap().entrySet()) {
                        vrpWriter.write(entry.getKey().getId() + " " + distanceFormat.format(entry.getValue()) + " ");
                    }
                    vrpWriter.write("\n");
                }
                for (Location fromLocationG : locationList) {
                    RoadSegmentLocation fromLocation = (RoadSegmentLocation) fromLocationG;
                    vrpWriter.write(fromLocation.getId() + " ");
                    for (Map.Entry<HubSegmentLocation, Double> entry : fromLocation.getHubTravelDistanceMap().entrySet()) {
                        vrpWriter.write(entry.getKey().getId() + " " + distanceFormat.format(entry.getValue()) + " ");
                    }
                    for (Map.Entry<RoadSegmentLocation, Double> entry : fromLocation.getNearbyTravelDistanceMap().entrySet()) {
                        vrpWriter.write(entry.getKey().getId() + " " + distanceFormat.format(entry.getValue()) + " ");
                    }
                    vrpWriter.write("\n");
                }
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

    private GHResponse fetchGhResponse(Location fromLocation, Location toLocation) {
        GHRequest request = new GHRequest(fromLocation.getLatitude(), fromLocation.getLongitude(),
                toLocation.getLatitude(), toLocation.getLongitude())
                .setVehicle("car");
        GHResponse response = graphHopper.route(request);
        if (response.hasErrors()) {
            throw new IllegalStateException("GraphHopper gave " + response.getErrors().size()
                    + " errors. First error chained.",
                    response.getErrors().get(0)
            );
        }
        return response;
    }

    private void writeDemandSection(BufferedWriter vrpWriter, int locationListSize, int vehicleListSize, int capacity,
            List<Location> locationList) throws IOException {
        vrpWriter.write("DEMAND_SECTION\n");
        // maximumDemand is 2 times the averageDemand. And the averageDemand is 2/3th of available capacity
        int maximumDemand = (4 * vehicleListSize * capacity) / (locationListSize * 3);
        Random random = new Random(37);
        boolean first = true;
        for (Location location : locationList) {
            if (first) {
                vrpWriter.write(location.getId() + " 0\n");
                first = false;
            } else {
                vrpWriter.write(location.getId() + " " + (random.nextInt(maximumDemand) + 1) + "\n");
            }
        }
    }

    private void writeDepotSection(BufferedWriter vrpWriter, List<Location> locationList) throws IOException {
        vrpWriter.write("DEPOT_SECTION\n");
        vrpWriter.write(locationList.get(0).getId() + "\n");
        vrpWriter.write("-1\n");
        vrpWriter.write("EOF\n");
    }

    private List<HubSegmentLocation> readHubList(File hubFile, boolean segmented) {
        if (!segmented) {
            return Collections.emptyList();
        }
        List<HubSegmentLocation> locationList = new ArrayList<HubSegmentLocation>(3000);
        BufferedReader bufferedReader = null;
        long id = 0L;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(hubFile), "UTF-8"));
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                String[] tokens = line.split(" ");
                if (tokens.length != 4) {
                    throw new IllegalArgumentException("The line (" + line + ") does not have 4 tokens ("
                            + tokens.length + ").");
                }
                HubSegmentLocation location = new HubSegmentLocation();
                location.setId(id);
                id++;
                location.setLatitude(Double.parseDouble(tokens[1]));
                location.setLongitude(Double.parseDouble(tokens[2]));
                location.setName(tokens[3]);
                locationList.add(location);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the hubFile (" + hubFile + ").", e);
        } finally {
            IOUtils.closeQuietly(bufferedReader);
        }
        logger.info("Read {} cities.", locationList.size());
        return locationList;
    }

    private List<AirLocation> readAirLocationFile(File locationFile, long startId) {
        List<AirLocation> locationList = new ArrayList<AirLocation>(3000);
        BufferedReader bufferedReader = null;
        long id = startId;
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

    private static class Point {

        public final double latitude;
        public final double longitude;

        public Point(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Point point = (Point) o;
            if (Double.compare(point.latitude, latitude) != 0) {
                return false;
            }
            if (Double.compare(point.longitude, longitude) != 0) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(latitude);
            result = (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(longitude);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "" + latitude + "," + longitude;
        }

    }

}
