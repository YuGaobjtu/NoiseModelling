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
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_traffic
import org.noise_planet.noisemodelling.wps.NoiseModelling.Road_Emission_from_Traffic
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





        String res = new Display_Database().exec(connection, [])

        assertEquals("BUILDINGS</br></br>GROUND</br></br>ROADS</br></br>", res)
    }

}
