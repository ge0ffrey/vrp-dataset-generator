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

public enum VrpType {
    BASIC,
    TIMEWINDOWED;

    public String getFileSuffix() {
        switch (this) {
            case BASIC:
                return "";
            case TIMEWINDOWED:
                return "-tw";
            default:
                throw new IllegalArgumentException("Unsupported vrpType (" + this + ").");
        }
    }

    public String getDirName() {
        switch (this) {
            case BASIC:
                return "basic";
            case TIMEWINDOWED:
                return "timewindowed";
            default:
                throw new IllegalArgumentException("Unsupported vrpType (" + this + ").");
        }
    }

    public String getHeaderType() {
        switch (this) {
            case BASIC:
                return "CVRP";
            case TIMEWINDOWED:
                return "CVRPTW";
            default:
                throw new IllegalArgumentException("Unsupported vrpType (" + this + ").");
        }
    }

}
