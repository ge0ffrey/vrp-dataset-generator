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

package org.optaplanner.extension.vrpdatasetgenerator;

import com.graphhopper.GHResponse;

public enum GenerationDistanceType {
    AIR_DISTANCE,
    ROAD_DISTANCE_KM,
    ROAD_DISTANCE_TIME,
    SEGMENTED_ROAD_DISTANCE_KM,
    SEGMENTED_ROAD_DISTANCE_TIME;

    public String getFileSuffix() {
        switch (this) {
            case AIR_DISTANCE:
                return "";
            case ROAD_DISTANCE_KM:
                return "-road-km";
            case ROAD_DISTANCE_TIME:
                return "-road-time";
            case SEGMENTED_ROAD_DISTANCE_KM:
                return "-segmentedRoad-km";
            case SEGMENTED_ROAD_DISTANCE_TIME:
                return "-segmentedRoad-time";
            default:
                throw new IllegalStateException("The generationDistanceType (" + this
                        + ") is not implemented.");

        }
    }

    public boolean isRoad() {
        return this != AIR_DISTANCE;
    }

    public boolean isSegmented() {
        return this == SEGMENTED_ROAD_DISTANCE_KM || this == SEGMENTED_ROAD_DISTANCE_TIME;
    }

    public boolean isShortest() {
        return this == AIR_DISTANCE || this == ROAD_DISTANCE_KM || this == SEGMENTED_ROAD_DISTANCE_KM;
    }

    public double extractDistance(GHResponse response) {
        switch (this) {
            case AIR_DISTANCE:
                throw new IllegalStateException("The generationDistanceType (" + this
                        + ") does not support GHResponse.");
            case ROAD_DISTANCE_KM:
            case SEGMENTED_ROAD_DISTANCE_KM:
                // Distance should be in km, not meter
                return response.getDistance() / 1000.0;
            case ROAD_DISTANCE_TIME:
            case SEGMENTED_ROAD_DISTANCE_TIME:
                return response.getMillis() / 1000.0;
            default:
                throw new IllegalStateException("The generationDistanceType (" + this
                        + ") is not implemented.");

        }
    }

    public String getUnitOfMeasurement() {
        switch (this) {
            case AIR_DISTANCE:
                throw new IllegalStateException("The generationDistanceType (" + this
                        + ") does unit of measurement.");
            case ROAD_DISTANCE_KM:
            case SEGMENTED_ROAD_DISTANCE_KM:
                return "km";
            case ROAD_DISTANCE_TIME:
            case SEGMENTED_ROAD_DISTANCE_TIME:
                return "sec";
            default:
                throw new IllegalStateException("The generationDistanceType (" + this
                        + ") is not implemented.");

        }
    }

    public String getDirName() {
        switch (this) {
            case AIR_DISTANCE:
                return "air";
            case ROAD_DISTANCE_KM:
            case SEGMENTED_ROAD_DISTANCE_KM:
                return "road-km";
            case ROAD_DISTANCE_TIME:
            case SEGMENTED_ROAD_DISTANCE_TIME:
                return "road-time";
            default:
                throw new IllegalStateException("The generationDistanceType (" + this
                        + ") is not implemented.");
        }
    }

}
