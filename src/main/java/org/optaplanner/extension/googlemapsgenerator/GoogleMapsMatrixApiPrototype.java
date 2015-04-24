/*
 * Copyright 2015 JBoss Inc
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

package org.optaplanner.extension.googlemapsgenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import com.google.maps.DistanceMatrixApi;
import com.google.maps.DistanceMatrixApiRequest;
import com.google.maps.GeoApiContext;
import com.google.maps.model.DistanceMatrix;
import com.google.maps.model.DistanceMatrixElement;
import com.google.maps.model.DistanceMatrixElementStatus;
import com.google.maps.model.DistanceMatrixRow;
import org.apache.commons.io.IOUtils;
import org.optaplanner.core.api.domain.solution.Solution;
import org.optaplanner.examples.tsp.domain.TravelingSalesmanTour;
import org.optaplanner.examples.tsp.domain.location.Location;
import org.optaplanner.examples.tsp.persistence.TspImporter;

public class GoogleMapsMatrixApiPrototype {

    public static void main(String[] args) {
        // TODO Use your own Google Maps API key credentials from https://console.developers.google.com
        // You'll need a Public API access server key
        GeoApiContext geoApiContext = new GeoApiContext().setApiKey("AIzaSyC8BwoiCD5yH0yzt7sgZ7UvWipSO8ddLpo");
        TravelingSalesmanTour tour = (TravelingSalesmanTour) new TspImporter(true)
                .readSolution(new File("../optaplanner-examples/data/tsp/import/usa/americanRoadTrip-n50.tsp"));
        BufferedWriter distanceWriter = null;
        BufferedWriter timeWriter = null;
        try {
            distanceWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("../optaplanner-examples/data/tsp/import/usa/americanRoadTrip-road-km-n50-new.tsp"), "UTF-8"));
            timeWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("../optaplanner-examples/data/tsp/import/usa/americanRoadTrip-road-time-n50-new.tsp"), "UTF-8"));
            List<Location> locationList = tour.getLocationList();
            int n = locationList.size();
            for (int i = 0; i < n; i++) {
                Location origin = locationList.get(i);
                String[] origins = new String[]{(origin.getLatitude() + "," + origin.getLongitude())};
                String[] destinations = new String[n - 1];
                for (int j = 0; j < n; j++) {
                    if (i != j) {
                        Location destination = locationList.get(j);
                        int k = (j < i) ? j : j - 1;
                        destinations[k] = destination.getLatitude() + "," + destination.getLongitude();
                    }
                }
                DistanceMatrixApiRequest request = DistanceMatrixApi.newRequest(geoApiContext);
                request.origins(origins);
                request.destinations(destinations);
                DistanceMatrix distanceMatrix;
                try {
                    distanceMatrix = request.await();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Google Maps DistanceMatrixApiRequest failed.", e);
                }
                if (distanceMatrix.rows.length != 1) {
                    throw new IllegalStateException("Rows length (" + distanceMatrix.rows.length + ") is not 1.");
                }
                DistanceMatrixRow row = distanceMatrix.rows[0];
                if (row.elements.length != (n - 1)) {
                    throw new IllegalStateException("Elements length (" + row.elements.length
                            + ") is not " + (n - 1) + ".");
                }
                for (int j = 0; j < n; j++) {
                    if (j != 0) {
                        distanceWriter.write(" ");
                        timeWriter.write(" ");
                    }
                    if (i == j) {
                        distanceWriter.write("0");
                        timeWriter.write("0");
                    } else {
                        int k = (j < i) ? j : j - 1;
                        DistanceMatrixElement element = row.elements[k];
                        if (element.status == DistanceMatrixElementStatus.OK) {
                            distanceWriter.write(Long.toString(element.distance.inMeters));
                            timeWriter.write(Long.toString(element.duration.inSeconds));
                        } else {
                            distanceWriter.write("?");
                            timeWriter.write("?");
                        }
                    }
                }
                distanceWriter.write("\n");
                timeWriter.write("\n");
                System.out.println("Written " + i);
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Sleep interrupted.");
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed writing file.", e);
        } finally {
            IOUtils.closeQuietly(distanceWriter);
            IOUtils.closeQuietly(timeWriter);
        }


    }

}
