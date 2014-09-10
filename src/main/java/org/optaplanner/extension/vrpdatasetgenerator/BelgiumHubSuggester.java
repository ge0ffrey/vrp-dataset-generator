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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
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
        suggest(new File("data/raw/belgium-cities.csv"), 50, new File("data/raw/belgium-hubs.txt"));
//        suggest(new File("data/raw/belgium-cities.csv"), 100, new File("data/raw/belgium-hubs.txt"));
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
        Map<Point, Point> fromPointMap = new LinkedHashMap<Point, Point>(locationListSize * 10);
        Map<Point, Point> toPointMap = new LinkedHashMap<Point, Point>(locationListSize * 10);
        int rowIndex = 0;
        for (AirLocation fromAirLocation : locationList) {
            for (AirLocation toAirLocation : locationList) {
                double distance;
                if (fromAirLocation != toAirLocation) {
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
                    PointPart previousFromPointPart = null;
                    PointPart previousToPointPart = null;
                    for (int i = 0; i < graphHopperPointList.size(); i++) {
                        Point fromPoint = new Point(
                                graphHopperPointList.getLatitude(i), graphHopperPointList.getLongitude(i));
                        Point oldFromPoint = fromPointMap.get(fromPoint);
                        if (oldFromPoint == null) {
                            // Initialize fromPoint instance
                            fromPoint.pointPartMap = new LinkedHashMap<AirLocation, PointPart>();
                            fromPointMap.put(fromPoint, fromPoint);
                        } else {
                            // Reuse existing fromPoint instance
                            fromPoint = oldFromPoint;
                        }
                        PointPart fromPointPart = fromPoint.pointPartMap.get(fromAirLocation);
                        if (fromPointPart == null) {
                            fromPointPart = new PointPart(fromPoint, fromAirLocation);
                            fromPoint.pointPartMap.put(fromAirLocation, fromPointPart);
                            fromPointPart.previousPart = previousFromPointPart;
                        }
                        previousFromPointPart = fromPointPart;
                        Point toPoint = new Point(
                                graphHopperPointList.getLatitude(i), graphHopperPointList.getLongitude(i));
                        Point oldToPoint = toPointMap.get(toPoint);
                        if (oldToPoint == null) {
                            // Initialize toPoint instance
                            toPoint.pointPartMap = new LinkedHashMap<AirLocation, PointPart>();
                            toPointMap.put(toPoint, toPoint);
                        } else {
                            // Reuse existing toPoint instance
                            toPoint = oldToPoint;
                        }
                        // Basically do the same as fromPointPart, but while traversing in the other direction
                        PointPart toPointPart = toPoint.pointPartMap.get(toAirLocation);
                        boolean newToPointPart = false;
                        if (toPointPart == null) {
                            toPointPart = new PointPart(toPoint, toAirLocation);
                            toPoint.pointPartMap.put(toAirLocation, toPointPart);
                            newToPointPart = true;
                        }
                        if (previousToPointPart != null) {
                            previousToPointPart.previousPart = toPointPart;
                        }
                        if (newToPointPart) {
                            previousToPointPart = toPointPart;
                        } else {
                            previousToPointPart = null;
                        }
                    }
                }
            }
            logger.debug("  Finished routes for rowIndex {}/{}", rowIndex, locationList.size());
            rowIndex++;
        }
        logger.info("Filtering points below threshold...");
        List<Point> hubPointList = new ArrayList<Point>(20);
        List<Point> fromPointList = new ArrayList<Point>(fromPointMap.values());
        fromPointMap = null;
        int THRESHOLD = 10;
        while (!fromPointList.isEmpty()) {
            logger.info("  {} fromPoints left", fromPointList.size());
//            for (Iterator<Point> it = fromPointList.iterator(); it.hasNext(); ) {
//                Point point = it.next();
//                if (point.pointPartMap.values().size() < THRESHOLD) {
//                    it.remove();
//                    point.removed = true;
//                }
//            }
//            for (Point point : fromPointList) {
//                for (PointPart pointPart : point.pointPartMap.values()) {
//                    PointPart previousPart = pointPart.previousPart;
//                    while (previousPart != null && previousPart.point.removed) {
//                        previousPart = previousPart.previousPart;
//                    }
//                    pointPart.previousPart = previousPart;
//                }
//            }
            // Make the biggest merger of 2 big streams into 1 stream a hub.
            int maxRestCount = -1;
            Point maxRestPoint = null;
            for (Point point : fromPointList) {
                Multiset<Point> previousPoints = HashMultiset.create();
                for (PointPart pointPart : point.pointPartMap.values()) {
                    if (pointPart.previousPart != null) {
                        previousPoints.add(pointPart.previousPart.point);
                    }
                }
                if (!previousPoints.isEmpty()) {
                    Point majorPreviousPoint = Multisets.copyHighestCountFirst(previousPoints).elementSet().iterator().next();
                    int majorCount = previousPoints.count(majorPreviousPoint);
                    int restCount = point.pointPartMap.size() - majorCount;
                    if (restCount > maxRestCount) {
                        maxRestCount = restCount;
                        maxRestPoint = point;
                    }
                }
            }
            if (maxRestPoint == null) {
                throw new IllegalStateException("No maxRestPoint (" + maxRestPoint + ") found.");
            }
            maxRestPoint.hub = true;
            fromPointList.remove(maxRestPoint);
            hubPointList.add(maxRestPoint);
            // Remove trailing parts
            for (Iterator<Point> pointIt = fromPointList.iterator(); pointIt.hasNext(); ) {
                Point point = pointIt.next();
                for (Iterator<PointPart> partIt = point.pointPartMap.values().iterator(); partIt.hasNext(); ) {
                    PointPart pointPart = partIt.next();
                    if (pointPart.comesAfterHub()) {
                        partIt.remove();
                    }
                }
                if (point.pointPartMap.isEmpty()) {
                    point.removed = true;
                    pointIt.remove();
                }
            }
            if (hubPointList.size() > 20) {
                break;
            }
        }
        logger.info("Writing hubs...");
        BufferedWriter vrpWriter = null;
        try {
            vrpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
            vrpWriter.write("HUB_COORD_SECTION\n");
            int id = 0;
            for (Point point : hubPointList) {
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

    private static class Point {

        public final double latitude;
        public final double longitude;

        public Map<AirLocation, PointPart> pointPartMap;

        public boolean removed = false;
        public boolean hub = false;

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
            return "R-" + removed + "_H-" + hub;
        }

    }

    private static class PointPart {

        public final Point point;
        public final AirLocation anchor;
        public PointPart previousPart;

        public PointPart(Point point, AirLocation anchor) {
            this.point = point;
            this.anchor = anchor;
        }

        public boolean comesAfterHub() {
            PointPart ancestorPart = previousPart;
            while (true) {
                if (ancestorPart == null) {
                    return false;
                }
                if (ancestorPart.point.hub) {
                    return true;
                }
                ancestorPart = ancestorPart.previousPart;
            }
        }

        public boolean isPossiblyFirst() {
            if (previousPart == null) {
                return true;
            }
            PointPart ancestorPart = previousPart;
            // Skip the removed points.
            while (ancestorPart.point.removed) {
                if (ancestorPart.previousPart == null) {
                    return true;
                }
                ancestorPart = ancestorPart.previousPart;
            }
            if (ancestorPart.point.hub) {
                return false;
            }
            return true; // Maybe possibly first
        }

        public boolean isDefinitelyFirst() {
            if (previousPart == null) {
                return true;
            }
            PointPart ancestorPart = previousPart;
            // Skip the removed points.
            while (ancestorPart.point.removed) {
                if (ancestorPart.previousPart == null) {
                    return true;
                }
                ancestorPart = ancestorPart.previousPart;
            }
            if (ancestorPart.point.hub) {
                return false;
            }
            return false; // Not definitely first
        }

        @Override
        public String toString() {
            return point + "-" + anchor.getName();
        }
    }

}
