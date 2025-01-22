/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by Université Gustave Eiffel and CNRS
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */
/**
 * @Author Valentin Le Bescond, Université Gustave Eiffel
 */

package org.noise_planet.noisemodelling.wps.Geometric_Tools

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.utilities.GeometryMetaData
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBUtils
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection

title = 'Create 0db Source From Roads'
description = '&#10145;&#65039; Creates a SOURCE table from a ROAD table.' +
              '<hr>' +
              'The SOURCE table can then be used in the <b>Noise_level_from_source</b> WPS block with the "confExportSourceId" set to true. </br></br>' +
              'The <b>Noise_level_from_source</b> output will contain a list of "source-receiver" attenuation matrix independent of the source absolute noise power levels.'

inputs = [
        tableRoads: [
                name: 'Input table name',
                title: 'Intput table name',
                description: 'Name of the Roads table. <br/> <br/>' +
                             'Must contain at least:</br>'+
                             '- <b>PK</b>: identifier with a Primary Key constraint</br>' +
                             '- <b>THE_GEOM</b>: geometric column',
                type: String.class
        ],
        gridStep : [name : 'gridStep',
                    title : "gridStep",
                    description : "Distance between location of vehicle along the network in meters.</br> <b> Default value : 10 </b>",
                    type: Integer.class]
]

outputs = [
        result: [
                name: 'Result output string',
                title: 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type: String.class
        ]
]

static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}


def run(input) {

    // Get name of the database
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}


def exec(connection, input) {

    double h = 0.05 // height of the source (0.05 m)

    connection = new ConnectionWrapper(connection)

    Sql sql = new Sql(connection)

    String resultString = null

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info('Start : Create_0db_Source_From_Roads')
    logger.info("inputs {}", input)

    int gridStep = 10
    if (input['gridStep']) {
        gridStep = Integer.valueOf(input['gridStep'] as String)
    }


    String roadsTableName = input['tableRoads']


    sql.execute("DROP TABLE IF EXISTS SOURCESLines_0dB")
    sql.execute("CREATE TABLE SOURCESLines_0dB (ROAD_ID BIGINT, THE_GEOM GEOMETRY, HZ63 FLOAT, HZ125 FLOAT, HZ250 FLOAT, HZ500 FLOAT, HZ1000 FLOAT, HZ2000 FLOAT, HZ4000 FLOAT, HZ8000 FLOAT);");
    sql.execute("INSERT INTO SOURCESLines_0dB (ROAD_ID, THE_GEOM, HZ63, HZ125, HZ250, HZ500, HZ1000, HZ2000, HZ4000, HZ8000) SELECT r.PK AS ROAD_ID, ST_TOMULTIPOINT(ST_Densify(r.THE_GEOM, "+gridStep+"))AS THE_GEOM, 0.0 AS HZ63, 0.0 AS HZ125, 0.0 AS HZ250, 0.0 AS HZ500, 0.0 AS HZ1000, 0.0 AS HZ2000, 0.0 AS HZ4000, 0.0 AS HZ8000 FROM "+roadsTableName+" r;");
    sql.execute("DROP TABLE IF EXISTS SOURCES_0dB")
    sql.execute("CREATE TABLE SOURCES_0DB AS SELECT * FROM ST_EXPLODE('SOURCESLines_0dB');");
    sql.execute("ALTER TABLE SOURCES_0DB ADD PK INT AUTO_INCREMENT PRIMARY KEY;")
    sql.execute("DROP TABLE IF EXISTS SOURCESLines_0dB")
    sql.execute("CREATE SPATIAL INDEX ON SOURCES_0DB(THE_GEOM);")
    int srid = GeometryTableUtilities.getSRID(connection, TableLocation.parse(roadsTableName))
    table_name = "SOURCES_0DB"
    GeometryMetaData metaData = GeometryTableUtilities.getMetaData(connection, TableLocation.parse(table_name, DBUtils.getDBType(connection)), "THE_GEOM");
    metaData.setSRID(srid)
    metaData.setHasZ(true)
    metaData.initGeometryType()

    connection.createStatement().execute(String.format(Locale.ROOT, "ALTER TABLE %s ALTER COLUMN %s %s USING ST_SetSRID(ST_UPDATEZ(%s, %f),%d)",
            TableLocation.parse(table_name, DBUtils.getDBType(connection)), "THE_GEOM" , metaData.getSQL(),"THE_GEOM", h,srid))


    logger.info('End : Create_0db_Source_From_Roads')
    logger.info('Result : ' + resultString)
    return resultString
}
