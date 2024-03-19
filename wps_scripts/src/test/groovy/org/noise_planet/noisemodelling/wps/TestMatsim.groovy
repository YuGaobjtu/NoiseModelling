/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.noisemodelling.wps

import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.junit.Test
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.Add_Laeq_Leq_columns
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.Create_Isosurface
import org.noise_planet.noisemodelling.wps.Database_Manager.Display_Database
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Import_Activities
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Noise_From_Attenuation_Matrix
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Receivers_From_Activities_Random
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Traffic_From_Events
import org.noise_planet.noisemodelling.wps.Geometric_Tools.ZerodB_Source_From_Roads
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source
import org.noise_planet.noisemodelling.wps.NoiseModelling.Road_Emission_from_Traffic
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_traffic
import org.noise_planet.noisemodelling.wps.Receivers.Building_Grid
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.SQLException

/**
 * Test parsing of zip file using H2GIS database
 */
class TestMatsim extends JdbcTestCase {
    Logger LOGGER = LoggerFactory.getLogger(TestMatsim.class)

    @Test
    void testMatsimTutorial() throws SQLException, IOException {

      new Import_OSM().exec(connection, [
                "pathFile"      : TestMatsim.getResource("MatsimTutorial/nantes_ile.osm.pbf").getPath(),
                "targetSRID"    : 2154
        ]);

         new Traffic_From_Events().exec(connection, [
                "folder"        :  TestMatsim.getResource("MatsimTutorial").getPath(),
                "timeBinSize"   : 900,
                "populationFactor" : 0.01,
                "link2GeometryFile" : TestMatsim.getResource("MatsimTutorial/network.csv").getPath(),
                "SRID"          : 2154,
                "skipUnused"    : true
        ]);

        new Import_Activities().exec(connection,[
                "facilitiesPath"    : TestMatsim.getResource("MatsimTutorial/output_facilities.xml.gz").getPath(),
                "SRID"              : 2154,
                "outTableName"      : "ACTIVITIES"
        ]);

        new Building_Grid().exec(connection,[
                "tableBuilding"     : "buildings",
                "distance"          : 5.0,
                "height"            : 4.0
        ]);

        new Receivers_From_Activities_Random().exec(connection,[
                "activitiesTable"   : "ACTIVITIES",
                "buildingsTable"    : "BUILDINGS",
                "receiversTable"    : "RECEIVERS",
                "outTableName"      : "ACTIVITY_RECEIVERS"
        ]);

        new ZerodB_Source_From_Roads().exec(connection,[
                "roadsTableName"    : "MATSIM_ROADS",
                "sourcesTableName"  : "SOURCES_0DB"
        ]);

        new Noise_level_from_source().exec(connection, [
                "tableBuilding"     : "Buildings",
                "tableSources"      : "SOURCES_0DB",
                "tableReceivers"    : "ACTIVITY_RECEIVERS",
                "confMaxSrcDist"    : 250,
                "confMaxReflDist"   : 50,
                "confReflOrder"     : 1,
                "confSkipLevening"  : true,
                "confSkipLnight"    : true,
                "confSkipLden"      : true,
                "confExportSourceId": true,
                "confDiffVertical"  : false,
                "confDiffHorizontal": true,
                "confThreadNumber"  : 4
        ]);
        // End up with LDAY_GEOM Table.


        // Bug: Don't find Table "MATSIM_ROADS_STATS", the output is MATSIM_ROADS_LW.
        // Notice: There are 5 input parameters here, but on website version, it's 6 with an extra parameter "The size of
        // time bins in seconds"
        new Noise_From_Attenuation_Matrix().exec(connection, [
                "attenuationTable"  : "LDAY_GEOM",
                "outTableName"      : "RESULT_GEOM",
                "matsimRoadsLw"     : "MATSIM_ROADS_LW",
                "matsimRoads"       : "MATSIM_ROADS",
                "receiversTable"    : "ACTIVITY_RECEIVERS",
                "timeBinSize"       : 900
        ])

       String res = new Display_Database().exec(connection, []);

        File exportPath = new File("target/matsim_result_geom.shp")

        if (exportPath.exists()) {
            exportPath.delete()
        }
        new Export_Table().exec(connection, [
                "exportPath"    : exportPath,
                "tableToExport" : "RESULT_GEOM"
        ])

    }

}
