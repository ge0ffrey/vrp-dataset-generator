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

package org.optaplanner.extension.tspdatasetgenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.optaplanner.examples.common.app.LoggingMain;

/**
 * This is very quick and VERY DIRTY code.
 */
public class FromVrpToTspConverter extends LoggingMain {

    public static void main(String[] args) {
        new FromVrpToTspConverter().convertAll();
    }

    public void convertAll() {
        convertAllInDir(new File("data/vehiclerouting/import/capacitated/"), new File("data/tsp/import/capacitated/"));
        convertAllInDir(new File("data/vehiclerouting/import/roaddistance/capacitated/"), new File("data/tsp/import/roaddistance/capacitated/"));
    }

    private void convertAllInDir(File inputDir, File outputDir) {
        File[] inputFiles = inputDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                String name = file.getName();
                return name.endsWith(".vrp") && !name.contains("-segmentedRoad");
            }
        });
        Arrays.sort(inputFiles);
        for (File inputFile : inputFiles) {
            convert(inputFile, new File(outputDir, inputFile.getName().replaceAll("\\-k\\d+\\.vrp", ".tsp")));
        }
    }

    private void convert(File inputFile, File outputFile) {
        BufferedReader vrpReader = null;
        BufferedWriter tspWriter = null;
        try {
            vrpReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
            tspWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
            try {
                convert(vrpReader, tspWriter, outputFile.getName().replaceAll("\\.tsp", ""));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Exception in inputFile (" + inputFile + ")", e);
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Exception in inputFile (" + inputFile + ")", e);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read the inputFile (" + inputFile
                    + ") or write the outputFile (" + outputFile + ").", e);
        } finally {
            IOUtils.closeQuietly(vrpReader);
            IOUtils.closeQuietly(tspWriter);
        }
        logger.info("Converted to: {}", outputFile.getName());
    }

    private void convert(BufferedReader vrpReader, BufferedWriter tspWriter, String tspName) throws IOException {
        convertLine(vrpReader, tspWriter, "NAME:.*", "NAME: " + tspName);
        copyLine(vrpReader, tspWriter, "COMMENT:.*");
        convertLine(vrpReader, tspWriter, "TYPE:.*", "TYPE: TSP");
        int dimension = readDimension(vrpReader, tspWriter);
        boolean explicit = readEdgeWeightType(vrpReader, tspWriter);
        if (explicit) {
            copyLine(vrpReader, tspWriter, "EDGE_WEIGHT_FORMAT:.*");
            copyLine(vrpReader, tspWriter, "EDGE_WEIGHT_UNIT_OF_MEASUREMENT:.*");
        }
        ignoreLine(vrpReader, tspWriter, "CAPACITY:.*");
        copyLine(vrpReader, tspWriter, "NODE_COORD_SECTION");
        for (int i = 0; i < dimension; i++) {
            copyLine(vrpReader, tspWriter, null);
        }
        if (explicit) {
            copyLine(vrpReader, tspWriter, "EDGE_WEIGHT_SECTION");
            for (int i = 0; i < dimension; i++) {
                copyLine(vrpReader, tspWriter, null);
            }
        }
        ignoreLine(vrpReader, tspWriter, "DEMAND_SECTION");
        for (int i = 0; i < dimension; i++) {
            ignoreLine(vrpReader, tspWriter, null);
        }
        ignoreLine(vrpReader, tspWriter, "DEPOT_SECTION");
        ignoreLine(vrpReader, tspWriter, null);
        ignoreLine(vrpReader, tspWriter, null);
        copyLine(vrpReader, tspWriter, "EOF");
    }

    private int readDimension(BufferedReader vrpReader, BufferedWriter tspWriter) throws IOException {
        String line = vrpReader.readLine();
        String inputPattern = "DIMENSION:.*";
        if (!line.matches(inputPattern)) {
            throw new IllegalStateException("The line (" + line
                    + ") does not match inputPattern (" + inputPattern + ").");
        }
        tspWriter.write(line + "\n");
        return Integer.parseInt(line.replaceAll("DIMENSION: *", ""));
    }

    private boolean readEdgeWeightType(BufferedReader vrpReader, BufferedWriter tspWriter) throws IOException {
        String line = vrpReader.readLine();
        String inputPattern = "EDGE_WEIGHT_TYPE:.*";
        if (!line.matches(inputPattern)) {
            throw new IllegalStateException("The line (" + line
                    + ") does not match inputPattern (" + inputPattern + ").");
        }
        tspWriter.write(line + "\n");
        return line.matches(".*EXPLICIT");
    }

    private void convertLine(BufferedReader vrpReader, BufferedWriter tspWriter, String inputPattern, String outputLine) throws IOException {
        String line = vrpReader.readLine();
        if (inputPattern != null && !line.matches(inputPattern)) {
            throw new IllegalStateException("The line (" + line + ") does not match inputPattern (" + inputPattern + ").");
        }
        tspWriter.write(outputLine + "\n");
    }

    private void copyLine(BufferedReader vrpReader, BufferedWriter tspWriter, String inputPattern) throws IOException {
        String line = vrpReader.readLine();
        if (inputPattern != null && !line.matches(inputPattern)) {
            throw new IllegalStateException("The line (" + line + ") does not match inputPattern (" + inputPattern + ").");
        }
        tspWriter.write(line + "\n");
    }

    private void ignoreLine(BufferedReader vrpReader, BufferedWriter tspWriter, String inputPattern) throws IOException {
        String line = vrpReader.readLine();
        if (inputPattern != null && !line.matches(inputPattern)) {
            throw new IllegalStateException("The line (" + line + ") does not match inputPattern (" + inputPattern + ").");
        }
    }

}
