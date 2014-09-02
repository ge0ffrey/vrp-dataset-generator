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
import org.optaplanner.examples.tsp.domain.City;
import org.optaplanner.examples.vehiclerouting.persistence.VehicleRoutingDao;

public class FromCitiesCsvGenerator extends LoggingMain {

    public static void main(String[] args) {
        new FromCitiesCsvGenerator().generate();
    }

    protected final VehicleRoutingDao vehicleRoutingDao;

    private final GraphHopper graphHopper;

    public FromCitiesCsvGenerator() {
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
        generateVrp(new File("data/raw/belgium-cities.csv"), 100, 10, 250);
    }

    public void generateVrp(File cityFile, int locationListSize, int vehicleListSize, int capacity) {
        List<City> cityList = readCityFile(cityFile);
        String name = cityFile.getName().replaceAll("\\-cities.csv", "")
                + "-road-n" + locationListSize + "-k" + vehicleListSize;
        File vrpOutputFile = new File(vehicleRoutingDao.getDataDir(), "import/roaddistance/capacitated/" + name + ".vrp");
        if (!vrpOutputFile.getParentFile().exists()) {
            throw new IllegalArgumentException("The vrpOutputFile parent directory (" + vrpOutputFile.getParentFile()
                    + ") does not exist.");
        }
        BufferedWriter vrpWriter = null;
        try {
            vrpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vrpOutputFile), "UTF-8"));
            vrpWriter.write("NAME: " + name + "\n");
            vrpWriter.write("COMMENT: Generated for OptaPlanner. Road distance calculated with GraphHopper.\n");
            vrpWriter.write("TYPE: CVRP\n");
            vrpWriter.write("DIMENSION: " + locationListSize + "\n");
            vrpWriter.write("EDGE_WEIGHT_TYPE: EXPLICIT\n");
            vrpWriter.write("EDGE_WEIGHT_FORMAT: FULL_MATRIX\n");
            vrpWriter.write("CAPACITY: " + capacity + "\n");
            vrpWriter.write("NODE_COORD_SECTION\n");
            double selectionDecrement = (double) locationListSize / (double) cityList.size();
            double selection = (double) locationListSize;
            int index = 1;
            for (City city : cityList) {
                double newSelection = selection - selectionDecrement;
                if ((int) newSelection < (int) selection) {
                    vrpWriter.write(index + " " + city.getLatitude() + " " + city.getLongitude() + "\n");
                    index++;
                }
                selection = newSelection;
            }
            vrpWriter.write("EDGE_WEIGHT_SECTION\n");
            DecimalFormat distanceFormat = new DecimalFormat("0.0000");
            for (City fromCity : cityList) {
                for (City toCity : cityList) {
                    double distance;
                    if (fromCity == toCity) {
                        distance = 0.0;
                    } else {
                        GHRequest request = new GHRequest(fromCity.getLatitude(), fromCity.getLongitude(),
                                toCity.getLatitude(), toCity.getLongitude())
                                .setVehicle("car");
                        GHResponse response = graphHopper.route(request);
                        if (response.hasErrors()) {
                            throw new IllegalStateException("GraphHopper gave " + response.getErrors().size()
                                    + " errors. First error chained.",
                                    response.getErrors().get(0));
                        }
                        distance = response.getDistance();
                    }
                    vrpWriter.write(distanceFormat.format(distance) + " ");
                }
                vrpWriter.write("\n");
                logger.info("All distances calculated for city ({}).", fromCity);
            }

            vrpWriter.write("DEMAND_SECTION\n");
            // maximumDemand is 2 times the averageDemand. And the averageDemand is 2/3th of available capacity
            int maximumDemand = (4 * vehicleListSize * capacity) / (locationListSize * 3);
            Random random = new Random(37);
            vrpWriter.write("1 0\n");
            for (int i = 2; i <= locationListSize; i++) {
                vrpWriter.write(i + " " + (random.nextInt(maximumDemand) + 1) + "\n");
            }
            vrpWriter.write("DEPOT_SECTION\n");
            vrpWriter.write("1\n");
            vrpWriter.write("-1\n");
            vrpWriter.write("EOF\n");
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the cityFile (" + cityFile.getName()
                    + ") or write the vrpOutputFile (" + vrpOutputFile.getName() + ").", e);
        } finally {
            IOUtils.closeQuietly(vrpWriter);
        }
        logger.info("Generated: {}", vrpOutputFile);
    }

    private List<City> readCityFile(File cityFile) {
        List<City> cityList = new ArrayList<City>(3000);
        BufferedReader bufferedReader = null;
        long id = 0L;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(cityFile), "UTF-8"));
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                String[] tokens = line.split(";");
                if (tokens.length != 5) {
                    throw new IllegalArgumentException("The line (" + line + ") does not have 5 tokens ("
                            + tokens.length + ").");
                }
                City city = new City();
                city.setId(id);
                id++;
                city.setLatitude(Double.parseDouble(tokens[2]));
                city.setLongitude(Double.parseDouble(tokens[3]));
                city.setName(tokens[4]);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the cityFile (" + cityFile + ").", e);
        } finally {
            IOUtils.closeQuietly(bufferedReader);
        }
        logger.info("Cities read.");
        return cityList;
    }

}
