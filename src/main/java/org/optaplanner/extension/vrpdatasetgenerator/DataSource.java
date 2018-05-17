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

enum DataSource {
    BELGIUM,
    USA,
    UK_TEAMS;

    public String getOsmPath() {
        switch (this) {
            case BELGIUM:
                return "local/osm/belgium-latest.osm.pbf";
            case USA:
                return "local/osm/north-america-latest.osm.pbf";
            case UK_TEAMS:
                return"local/osm/great-britain-latest.osm.pbf";
            default:
                throw new IllegalArgumentException("Unsupported dataSource (" + this + ").");
        }
    }

    public String getDirName() {
        switch (this) {
            case BELGIUM:
                return "belgium";
            case USA:
                return "usa";
            case UK_TEAMS:
                return "uk-teams";
            default:
                throw new IllegalArgumentException("Unsupported dataSource (" + this + ").");
        }
    }

}
