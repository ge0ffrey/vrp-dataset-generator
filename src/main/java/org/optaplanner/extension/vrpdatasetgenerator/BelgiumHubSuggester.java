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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.util.PointList;
import org.apache.commons.io.IOUtils;
import org.optaplanner.examples.common.app.LoggingMain;
import org.optaplanner.examples.vehiclerouting.domain.location.AirLocation;

public class BelgiumHubSuggester extends LoggingMain {

    public static void main(String[] args) {
        new BelgiumHubSuggester().suggest();
    }

    private final GraphHopper graphHopper;

    public BelgiumHubSuggester() {
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

    public void suggest() {
//        suggest(new File("data/raw/belgium-cities.csv"), 50, new File("data/raw/belgium-hubs.txt"));
        suggest(new File("data/raw/belgium-cities.csv"), 100, new File("data/raw/belgium-hubs.txt"));
//        suggest(new File("data/raw/belgium-cities.csv"), 500, new File("data/raw/belgium-hubs.txt"));
//        suggest(new File("data/raw/belgium-cities.csv"), 1000, new File("data/raw/belgium-hubs.txt"));
//        suggest(new File("data/raw/belgium-cities.csv"), 2750, new File("data/raw/belgium-hubs.txt"));
    }

    public void suggest(File locationFile, int locationListSize, File outputFile) {
        List<AirLocation> locationList = readAirLocationFile(locationFile);
        if (locationListSize > locationList.size()) {
            throw new IllegalArgumentException("The locationListSize (" + locationListSize
                    + ") is larger than the locationList size (" + locationList.size() + ").");
        }
        double selectionDecrement = (double) locationListSize / (double) locationList.size();
        double selection = (double) locationListSize;
        int index = 1;
        List<AirLocation> newAirLocationList = new ArrayList<AirLocation>(locationList.size());
        for (AirLocation location : locationList) {
            double newSelection = selection - selectionDecrement;
            if ((int) newSelection < (int) selection) {
                newAirLocationList.add(location);
                index++;
            }
            selection = newSelection;
        }
        locationList = newAirLocationList;
        Map<Point, List<LocationPair>> pointToPairSetMap = new LinkedHashMap<Point, List<LocationPair>>(locationListSize * locationListSize);
        int rowIndex = 0;
        for (AirLocation fromAirLocation : locationList) {
            for (AirLocation toAirLocation : locationList) {
                double distance;
                if (fromAirLocation != toAirLocation) {
                    LocationPair locationPair = new LocationPair(fromAirLocation, toAirLocation);
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
                    PointList graphHopperPointList = response.getPoints();
                    for (int i = 0; i < graphHopperPointList.size(); i++) {
                        Point point = new Point(
                                graphHopperPointList.getLatitude(i), graphHopperPointList.getLongitude(i));
                        List<LocationPair> pairSet = pointToPairSetMap.get(point);
                        if (pairSet == null) {
                            pairSet = new ArrayList<LocationPair>(50);
                            pointToPairSetMap.put(point, pairSet);
                        } else if (pairSet.contains(locationPair)) {
                            continue;
                        }
                        pairSet.add(locationPair);
                    }
                }
            }
            logger.debug("  Finished routes for rowIndex {}/{}", rowIndex, locationList.size());
            rowIndex++;
        }
        logger.info("Filtering duplicate hubs and hubs below threshold...");
        int threshold = locationListSize;
        Map<List<LocationPair>, Point> pairSetToPointMap = new LinkedHashMap<List<LocationPair>, Point>(pointToPairSetMap.size());
        Map<LocationPair, List<Point>> pairToPointSetMap = new LinkedHashMap<LocationPair, List<Point>>(pointToPairSetMap.size());
        for (Iterator<Map.Entry<Point, List<LocationPair>>> it = pointToPairSetMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Point, List<LocationPair>> entry = it.next();
            Point point = entry.getKey();
            List<LocationPair> pairSet = entry.getValue();
            if (pairSet.size() >= threshold && !pairSetToPointMap.containsKey(pairSet)) {
                pairSetToPointMap.put(pairSet, point);
                for (LocationPair pair : pairSet) {
                    List<Point> pointSet = pairToPointSetMap.get(pair);
                    if (pointSet == null) {
                        pointSet = new ArrayList<Point>(50);
                        pairToPointSetMap.put(pair, pointSet);
                    }
                    pointSet.add(point);
                }
            } else {
                it.remove();
            }
        }
        pairSetToPointMap = null; // it's keys will only get broken anyway
        List<Point> sortingPointList = new ArrayList<Point>(pointToPairSetMap.size());
        for (Map.Entry<Point, List<LocationPair>> entry : pointToPairSetMap.entrySet()) {
            Point point = entry.getKey();
            point.pairSet = entry.getValue();
            sortingPointList.add(point);
        }
        pointToPairSetMap = null; // we don't need it any more

        logger.info("Writing hubs...");
        BufferedWriter vrpWriter = null;
        try {
            vrpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
            vrpWriter.write("HUB_COORD_SECTION\n");
            int id = 0;
            while (id < 20 && !sortingPointList.isEmpty()) {
                Collections.sort(sortingPointList); // Take the point with most pairs
                Point point = sortingPointList.remove(0);
                for (LocationPair pair : point.pairSet) {
                    for (Point otherPoint : pairToPointSetMap.get(pair)) {
                        if (otherPoint != point) {
                            otherPoint.pairSet.remove(pair);
                        }
                    }
                }
                vrpWriter.write("" + id + " " + point.latitude + " " + point.longitude + " " + id + "\n");
                id++;
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the locationFile (" + locationFile.getName()
                    + ") or write the vrpOutputFile (" + outputFile.getName() + ").", e);
        } finally {
            IOUtils.closeQuietly(vrpWriter);
        }
        // Throw in google docs spreadsheet and use add-on Mapping Sheets to visualize.
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

    private static class Point implements Comparable<Point> {

        public final double latitude;
        public final double longitude;

        public List<LocationPair> pairSet = null;

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
        public int compareTo(Point other) {
            int size = pairSet.size();
            int otherSize = other.pairSet.size();
            if (size < otherSize) {
                return 1; // Reverse order
            } else if (size > otherSize) {
                return -1;
            }
            if (latitude < other.latitude) {
                return -1;
            } else if (latitude > other.latitude) {
                return 1;
            }
            if (longitude < other.longitude) {
                return -1;
            } else if (longitude > other.longitude) {
                return 1;
            }
            return 0;
        }
    }

    private static class LocationPair {

        public final AirLocation fromLocation;
        public final AirLocation toLocation;

        private LocationPair(AirLocation fromLocation, AirLocation toLocation) {
            this.fromLocation = fromLocation;
            this.toLocation = toLocation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LocationPair that = (LocationPair) o;
            if (!fromLocation.equals(that.fromLocation)) {
                return false;
            }
            if (!toLocation.equals(that.toLocation)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = fromLocation.hashCode();
            result = 31 * result + toLocation.hashCode();
            return result;
        }

    }

}
