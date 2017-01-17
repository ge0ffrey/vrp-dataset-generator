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

/**
 * This is very quick and VERY DIRTY code.
 */
public class FromCsvLocationsToVrpGenerator extends LoggingMain {

    public static void main(String[] args) {
        DataSource dataSource = args.length == 0 ? DataSource.BELGIUM : DataSource.valueOf(args[0]);
        new FromCsvLocationsToVrpGenerator(dataSource).generate();
    }

    protected final VehicleRoutingDao vehicleRoutingDao;

    private final DataSource dataSource;

    private final GraphHopper fastestGraphHopper;
    private final GraphHopper shortestGraphHopper;

    public FromCsvLocationsToVrpGenerator(DataSource dataSource) {
        vehicleRoutingDao = new VehicleRoutingDao();
        this.dataSource = dataSource;

        String osmPath = dataSource.getOsmPath();
        if (osmPath == null) {
            fastestGraphHopper = null;
            shortestGraphHopper = null;
            return;
        }
        if (!new File(osmPath).exists()) {
            throw new IllegalStateException("The osmPath (" + osmPath + ") does not exist.\n" +
                    "Download the osm file from http://download.geofabrik.de/ first.");
        }
        fastestGraphHopper = new GraphHopper().forServer();
        fastestGraphHopper.setOSMFile(osmPath);
        fastestGraphHopper.setGraphHopperLocation("local/graphHopper/" + dataSource.name() + "/fastest");
        fastestGraphHopper.setEncodingManager(new EncodingManager(EncodingManager.CAR));
        fastestGraphHopper.setCHShortcuts("fastest");
        fastestGraphHopper.importOrLoad();
        logger.info("fastestGraphHopper loaded.");
        shortestGraphHopper = new GraphHopper().forServer();
        shortestGraphHopper.setOSMFile(osmPath);
        shortestGraphHopper.setGraphHopperLocation("local/graphHopper/" + dataSource.name() + "/shortest");
        shortestGraphHopper.setEncodingManager(new EncodingManager(EncodingManager.CAR));
        shortestGraphHopper.setCHShortcuts("shortest");
        shortestGraphHopper.importOrLoad();
        logger.info("shortestGraphHopper loaded.");
    }

    public void generate() {
        switch (dataSource) {
            case BELGIUM:
                File belgiumLocationFile = new File("data/raw/belgium-2750.csv");
                File belgiumHubFile = new File("data/raw/belgium-hubs.txt");
                generateVrp(belgiumLocationFile, belgiumHubFile, 50, 1, 10, 125);
                generateVrp(belgiumLocationFile, belgiumHubFile, 50, 2, 10, 125);
                generateVrp(belgiumLocationFile, belgiumHubFile, 100, 1, 10, 250);
                generateVrp(belgiumLocationFile, belgiumHubFile, 100, 3, 10, 250);
                generateVrp(belgiumLocationFile, belgiumHubFile, 500, 1, 20, 250);
                generateVrp(belgiumLocationFile, belgiumHubFile, 500, 5, 20, 250);
                generateVrp(belgiumLocationFile, belgiumHubFile, 1000, 1, 20, 500);
                generateVrp(belgiumLocationFile, belgiumHubFile, 1000, 8, 20, 500);
                generateVrp(belgiumLocationFile, belgiumHubFile, 2750, 1, 55, 500);
                generateVrp(belgiumLocationFile, belgiumHubFile, 2750, 10, 55, 500);
                break;
            case USA:
                File usaLocationFile = new File("data/raw/usa-115475.csv");
                generateVrp(usaLocationFile, null, 1000, 1, 10, 1000);
                generateVrp(usaLocationFile, null, 5000, 1, 50, 1000);
                generateVrp(usaLocationFile, null, 10000, 1, 100, 1000);
                generateVrp(usaLocationFile, null, 50000, 1, 500, 1000);
                generateVrp(usaLocationFile, null, 100000, 1, 1000, 1000);
                break;
            case UK_TEAMS:
                generateVrp(new File("local/data/raw/uk-teams-41.csv"), null, 41, 1, 10, 125);
                generateVrp(new File("local/data/raw/uk-teams-92.csv"), null, 92, 1, 10, 250);
                generateVrp(new File("local/data/raw/uk-teams-160.csv"), null, 160, 1, 12, 250);
                generateVrp(new File("local/data/raw/uk-teams-201.csv"), null, 201, 1, 14, 250);
                break;
            default:
                throw new IllegalArgumentException("Unsupported dataSource (" + dataSource + ").");
        }
    }

    public void generateVrp(File locationFile, File hubFile, int locationListSize, int depotListSize, int vehicleListSize, int capacity) {
        generateVrp(locationFile, null, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.AIR_DISTANCE, VrpType.BASIC);
        generateVrp(locationFile, null, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.AIR_DISTANCE, VrpType.TIMEWINDOWED);
        if (dataSource != DataSource.USA) {
            generateVrp(locationFile, null, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.ROAD_DISTANCE_KM, VrpType.BASIC);
            // Road distance with timewindowed is pointless
            generateVrp(locationFile, null, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.ROAD_DISTANCE_TIME, VrpType.BASIC);
            generateVrp(locationFile, null, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.ROAD_DISTANCE_TIME, VrpType.TIMEWINDOWED);
        }
        if (hubFile != null && depotListSize == 1) {
            generateVrp(locationFile, hubFile, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.SEGMENTED_ROAD_DISTANCE_KM, VrpType.BASIC);
            generateVrp(locationFile, hubFile, locationListSize, depotListSize, vehicleListSize, capacity, GenerationDistanceType.SEGMENTED_ROAD_DISTANCE_TIME, VrpType.BASIC);
        }
    }

    public void generateVrp(File locationFile, File hubFile, int locationListSize, int depotListSize, int vehicleListSize, int capacity,
            GenerationDistanceType distanceType, VrpType vrpType) {
        // WARNING: this code is DIRTY.
        // It's JUST good enough to generate the Belgium an UK datasets.
        String name = locationFile.getName().replaceAll("\\-\\d+\\.csv", "")
                + distanceType.getFileSuffix() + vrpType.getFileSuffix()
                + (depotListSize != 1 ? "-d" + depotListSize : "")
                + "-n" + locationListSize + "-k" + vehicleListSize;
        File vrpOutputFile = createVrpOutputFile(name, distanceType, vrpType, depotListSize != 1);
        List<HubSegmentLocation> hubList = readHubList(hubFile, distanceType);
        List<Location> locationList = selectLocationSubList(locationFile, locationListSize, depotListSize, hubList.size(), distanceType);
        BufferedWriter vrpWriter = null;
        try {
            vrpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vrpOutputFile), "UTF-8"));
            vrpWriter = writeHeaders(vrpWriter, locationListSize, capacity, distanceType, vrpType, name);
            writeHubCoordSection(vrpWriter, distanceType, hubList);
            writeNodeCoordSection(vrpWriter, locationList);
            writeEdgeWeightSection(vrpWriter, distanceType, hubList, locationList);
            writeDemandSection(vrpWriter, locationListSize, depotListSize, vehicleListSize, capacity, locationList, vrpType);
            writeDepotSection(vrpWriter, locationList, depotListSize);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the locationFile (" + locationFile.getName()
                    + ") or write the vrpOutputFile (" + vrpOutputFile.getName() + ").", e);
        } finally {
            IOUtils.closeQuietly(vrpWriter);
        }
        logger.info("Generated: {}", vrpOutputFile);
    }

    private File createVrpOutputFile(String name, GenerationDistanceType distanceType, VrpType vrpType, boolean multidepot) {
        String dataSourceDir = dataSource.getDirName();
        File vrpOutputFile = new File(vehicleRoutingDao.getDataDir(), "import/" + dataSourceDir
                + "/" + vrpType.getDirName(multidepot)
                + "/" + distanceType.getDirName()
                + "/" + name + ".vrp");
        if (!vrpOutputFile.getParentFile().exists()) {
            throw new IllegalArgumentException("The vrpOutputFile parent directory (" + vrpOutputFile.getParentFile()
                    + ") does not exist.");
        }
        return vrpOutputFile;
    }

    private BufferedWriter writeHeaders(BufferedWriter vrpWriter, int locationListSize, int capacity,
            GenerationDistanceType distanceType, VrpType vrpType, String name) throws IOException {
        vrpWriter.write("NAME: " + name + "\n");
        if (dataSource == DataSource.UK_TEAMS) {
            vrpWriter.write("COMMENT: Generated"
                    + (distanceType == GenerationDistanceType.AIR_DISTANCE ? "" : " with GraphHopper")
                    + " by Graham Kendall, Geoffrey De Smet, Nasser Sabar and Angelina Yee.\n");
        } else {
            vrpWriter.write("COMMENT: Generated for OptaPlanner Examples"
                    + (distanceType == GenerationDistanceType.AIR_DISTANCE ? "" : " with GraphHopper")
                    + " by Geoffrey De Smet.\n");
        }
        vrpWriter.write("TYPE: " + vrpType.getHeaderType() +"\n");
        vrpWriter.write("DIMENSION: " + locationListSize + "\n");
        if (distanceType.isRoad()) {
            if (distanceType.isSegmented()) {
                vrpWriter.write("EDGE_WEIGHT_TYPE: SEGMENTED_EXPLICIT\n");
                vrpWriter.write("EDGE_WEIGHT_FORMAT: HUB_AND_NEARBY_MATRIX\n");
            } else {
                vrpWriter.write("EDGE_WEIGHT_TYPE: EXPLICIT\n");
                vrpWriter.write("EDGE_WEIGHT_FORMAT: FULL_MATRIX\n");
            }
            vrpWriter.write("EDGE_WEIGHT_UNIT_OF_MEASUREMENT: " + distanceType.getUnitOfMeasurement() + "\n");
        } else {
            vrpWriter.write("EDGE_WEIGHT_TYPE: EUC_2D\n");
        }
        vrpWriter.write("CAPACITY: " + capacity + "\n");
        return vrpWriter;
    }

    private void writeHubCoordSection(BufferedWriter vrpWriter, GenerationDistanceType distanceType, List<HubSegmentLocation> hubList) throws IOException {
        if (!distanceType.isSegmented()) {
            return;
        }
        vrpWriter.write("HUBS: " + hubList.size() + "\n");
        vrpWriter.write("HUB_COORD_SECTION\n");
        for (HubSegmentLocation hub : hubList) {
            vrpWriter.write(hub.getId() + " " + hub.getLatitude() + " " + hub.getLongitude()
                    + (hub.getName() != null ? " " + hub.getName().replaceAll(" ", "_") : "") + "\n");
        }
    }

    private List<Location> selectLocationSubList(File locationFile, int locationListSize, int depotListSize, long startId, GenerationDistanceType distanceType) {
        List<AirLocation> airLocationList = readAirLocationFile(locationFile, startId);
        if (locationListSize > airLocationList.size()) {
            throw new IllegalArgumentException("The locationListSize (" + locationListSize
                    + ") is larger than the airLocationList size (" + airLocationList.size() + ").");
        }
        List<Location> newLocationList = new ArrayList<Location>(locationListSize);
        // Extract the depot's to the beginning of the list first
        switch (depotListSize) {
            case 1:
                extractLocation(airLocationList, newLocationList, "BRUSSEL", distanceType);
                break;
            case 10:
                extractLocation(airLocationList, newLocationList, "WAVRE", distanceType);
            case 9:
                extractLocation(airLocationList, newLocationList, "LEUVEN", distanceType);
            case 8:
                extractLocation(airLocationList, newLocationList, "MONS", distanceType);
            case 7:
                extractLocation(airLocationList, newLocationList, "ANTWERPEN", distanceType);
            case 6:
                extractLocation(airLocationList, newLocationList, "LIEGE", distanceType);
            case 5:
                extractLocation(airLocationList, newLocationList, "BRUGGE", distanceType);
            case 4:
                extractLocation(airLocationList, newLocationList, "ARLON", distanceType);
            case 3:
                extractLocation(airLocationList, newLocationList, "HASSELT", distanceType);
            case 2:
                extractLocation(airLocationList, newLocationList, "GENT", distanceType);
                extractLocation(airLocationList, newLocationList, "NAMUR", distanceType);
                break;
            default:
                throw new IllegalArgumentException("The depotListSize (" + depotListSize + ") is not supported");
        }

        int customerListSize = locationListSize - depotListSize;
        double selection = customerListSize;
        double selectionDecrement = (double) customerListSize / airLocationList.size();
        if (depotListSize == 1) {
            // HACK to avoid changing to single depot datasets generated 3 years ago
            selectionDecrement = (double) locationListSize / (airLocationList.size() + depotListSize);
            selection = locationListSize - selectionDecrement;
        }
        for (AirLocation airLocation : airLocationList) {
            double newSelection = selection - selectionDecrement;
            // Only if the sum of the selectionDecrements flow over 1.0, select it
            if ((int) newSelection < (int) selection) {
                newLocationList.add(copyLocation(airLocation, distanceType));
            }
            selection = newSelection;
        }
        if (newLocationList.size() != locationListSize) {
            throw new IllegalStateException("The newLocationList size (" + newLocationList.size()
                    + ") is not locationListSize (" + locationListSize + ").");
        }
        return newLocationList;
    }

    private void extractLocation(List<AirLocation> airLocationList, List<Location> newLocationList, String name, GenerationDistanceType distanceType) {
        AirLocation airLocation = airLocationList.stream().filter(location -> location.getName().equals(name)).findFirst().get();
        airLocationList.remove(airLocation);
        newLocationList.add(copyLocation(airLocation, distanceType));
    }

    private Location copyLocation(AirLocation location, GenerationDistanceType distanceType) {
        Location newLocation = distanceType.isRoad() ? (distanceType.isSegmented() ?
                new RoadSegmentLocation(location.getId(), location.getLatitude(), location.getLongitude())
                : new RoadLocation(location.getId(), location.getLatitude(), location.getLongitude()))
                : new AirLocation(location.getId(), location.getLatitude(), location.getLongitude());
        newLocation.setName(location.getName());
        return newLocation;
    }

    private void writeNodeCoordSection(BufferedWriter vrpWriter, List<Location> locationList) throws IOException {
        vrpWriter.write("NODE_COORD_SECTION\n");
        for (Location location : locationList) {
            vrpWriter.write(location.getId() + " " + location.getLatitude() + " " + location.getLongitude()
                    + (location.getName() != null ? " " + location.getName().replaceAll(" ", "_") : "") + "\n");
        }
    }

    private void writeEdgeWeightSection(BufferedWriter vrpWriter, GenerationDistanceType distanceType, List<HubSegmentLocation> hubList, List<Location> locationList) throws IOException {
        if (distanceType.isRoad()) {
            DecimalFormat distanceFormat = new DecimalFormat("0.000");
            if (!distanceType.isSegmented()) {
                vrpWriter.write("EDGE_WEIGHT_SECTION\n");
                for (Location fromLocation : locationList) {
                    for (Location toLocation : locationList) {
                        double distance;
                        if (fromLocation == toLocation) {
                            distance = 0.0;
                        } else {
                            GHResponse response = fetchGhResponse(fromLocation, toLocation, distanceType);
                            distance = distanceType.extractDistance(response);
                            if (distance == 0.0) {
                                throw new IllegalArgumentException("The fromLocation (" + fromLocation
                                        + ") and toLocation (" + toLocation + ") have a zero distance.");
                            }
                        }
                        vrpWriter.write(distanceFormat.format(distance) + " ");
                    }
                    vrpWriter.write("\n");
                    logger.info("All distances calculated for location ({}).", fromLocation);
                }
            } else {
                for (HubSegmentLocation fromHubLocation : hubList) {
                    Map<HubSegmentLocation, Double> fromHubTravelDistanceMap = new LinkedHashMap<HubSegmentLocation, Double>(hubList.size());
                    fromHubLocation.setHubTravelDistanceMap(fromHubTravelDistanceMap);
                    for (HubSegmentLocation toHubLocation : hubList) {
                        if (fromHubLocation == toHubLocation) {
                            continue;
                        }
                        GHResponse response = fetchGhResponse(fromHubLocation, toHubLocation, distanceType);
                        double distance = distanceType.extractDistance(response);
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
                        GHResponse response = fetchGhResponse(fromLocation, toLocation, distanceType);
                        double distance = distanceType.extractDistance(response);
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
                            if (!fromHubTravelDistanceMap.containsKey(firstHub)) {
                                GHResponse firstResponse = fetchGhResponse(fromLocation, firstHub, distanceType);
                                double firstHubDistance = distanceType.extractDistance(firstResponse);
                                fromHubTravelDistanceMap.put(firstHub, firstHubDistance);
                            }
                            if (!lastHub.getNearbyTravelDistanceMap().containsKey(toLocation)) {
                                GHResponse lastResponse = fetchGhResponse(lastHub, toLocation, distanceType);
                                double lastHubDistance = distanceType.extractDistance(lastResponse);
                                lastHub.getNearbyTravelDistanceMap().put(toLocation, lastHubDistance);
                            }
                            double segmentedDistance = fromLocation.getDistanceDouble(toLocation);
                            double distanceDiff = distance - segmentedDistance;
                            if (distanceDiff > 0.01) {
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
                    if (fromAirLocation != toAirLocation && fromAirLocation.getDistanceTo(toAirLocation) == 0) {
//                        throw new IllegalArgumentException("The fromAirLocation (" + fromAirLocation
//                                + ") and toAirLocation (" + toAirLocation + ") are the same.");
                        logger.warn("The fromAirLocation (" + fromAirLocation
                                + ") and toAirLocation (" + toAirLocation + ") are the same.");
                    }
                }
            }

        }
    }

    private GHResponse fetchGhResponse(Location fromLocation, Location toLocation, GenerationDistanceType distanceType) {
        GHRequest request = new GHRequest(fromLocation.getLatitude(), fromLocation.getLongitude(),
                toLocation.getLatitude(), toLocation.getLongitude())
                .setVehicle("car");
        GraphHopper graphHopper = distanceType.isShortest() ? shortestGraphHopper : fastestGraphHopper;
        GHResponse response = graphHopper.route(request);
        if (response.hasErrors()) {
            throw new IllegalStateException("GraphHopper gave " + response.getErrors().size()
                    + " errors. First error chained.",
                    response.getErrors().get(0)
            );
        }
        return response;
    }

    private void writeDemandSection(BufferedWriter vrpWriter, int locationListSize, int depotListSize, int vehicleListSize, int capacity,
            List<Location> locationList, VrpType vrpType) throws IOException {
        vrpWriter.append("DEMAND_SECTION\n");
        // maximumDemand is 2 times the averageDemand. And the averageDemand is 2/3th of available capacity
        int maximumDemand = (4 * vehicleListSize * capacity) / (locationListSize * 3);
        int minReadyTime = 7 * 60 * 60; // 7:00
        int maxWindowTimeInHalfHours = 12 * 2; // 12 hours
        int maxDueTime = minReadyTime  + maxWindowTimeInHalfHours * 30 * 60; // 19:00
        int customerServiceDuration = 5 * 60; // 5 minutes
        int i = 0;
        Random random = new Random(37);
        for (Location location : locationList) {
            String line;
            if (i < depotListSize) {
                line = location.getId() + " 0";
                if (vrpType == VrpType.TIMEWINDOWED) {
                    // Depot open from 7:00 until 19:00
                    line += " " + minReadyTime + " " + maxDueTime + " 0";
                }
            } else {
                line = location.getId() + " " + (random.nextInt(maximumDemand) + 1);
                if (vrpType == VrpType.TIMEWINDOWED) {
                    int windowTimeInHalfHours = (4 * 2) + random.nextInt((4 * 2) + 1); // 4 to 8 hours
                    int readyTime = minReadyTime + random.nextInt(maxWindowTimeInHalfHours - windowTimeInHalfHours + 1) * 30 * 60;
                    int dueTime = readyTime + (windowTimeInHalfHours * 30 * 60);
                    line += " " + readyTime + " " + dueTime + " " + customerServiceDuration;
                }
            }
            vrpWriter.append(line).append("\n");
            i++;
        }
    }

    private void writeDepotSection(BufferedWriter vrpWriter, List<Location> locationList, int depotListSize) throws IOException {
        vrpWriter.append("DEPOT_SECTION\n");
        for (int i = 0; i < depotListSize; i++) {
            Location location = locationList.get(i);
            vrpWriter.append(Long.toString(location.getId())).append("\n");
        }
        vrpWriter.append("-1\n");
        vrpWriter.append("EOF\n");
    }

    private List<HubSegmentLocation> readHubList(File hubFile, GenerationDistanceType distanceType) {
        if (!distanceType.isSegmented()) {
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
                AirLocation location = new AirLocation();
                switch (dataSource) {
                    case BELGIUM:
                        if (tokens.length != 5) {
                            throw new IllegalArgumentException("The line (" + line + ") does not have 5 tokens ("
                                    + tokens.length + ").");
                        }
                        location.setId(id);
                        id++;
                        location.setLatitude(Double.parseDouble(tokens[2]));
                        location.setLongitude(Double.parseDouble(tokens[3]));
                        location.setName(tokens[4]);
                        break;
                    case USA:
                        if (tokens.length != 3) {
                            throw new IllegalArgumentException("The line (" + line + ") does not have 3 tokens ("
                                    + tokens.length + ").");
                        }
                        location.setId(id);
                        id++;
                        location.setLatitude(Double.parseDouble(tokens[1]));
                        location.setLongitude(Double.parseDouble(tokens[2]));
                        location.setName(tokens[0]);
                        break;
                    case UK_TEAMS:
                        if (tokens.length != 6) {
                            throw new IllegalArgumentException("The line (" + line + ") does not have 6 tokens ("
                                    + tokens.length + ").");
                        }
                        location.setId(Long.parseLong(tokens[5]));
                        location.setLatitude(Double.parseDouble(tokens[3]));
                        location.setLongitude(Double.parseDouble(tokens[4]));
                        location.setName(tokens[1] +  " (" + tokens[0] + ")");
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported dataSource (" + dataSource + ").");
                }
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
