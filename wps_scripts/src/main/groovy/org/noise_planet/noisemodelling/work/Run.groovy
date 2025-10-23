package org.noise_planet.noisemodelling.work

import groovy.sql.Sql
import org.apache.commons.cli.*
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.noise_planet.noisemodelling.wps.Dynamic.Flow_2_Noisy_Vehicles
import org.noise_planet.noisemodelling.wps.Geometric_Tools.Change_SRID
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_traffic
import org.noise_planet.noisemodelling.wps.Receivers.Building_Grid
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.DynamicIndicators
import org.noise_planet.noisemodelling.wps.Database_Manager.Add_Primary_Key
import org.noise_planet.noisemodelling.wps.Dynamic.Point_Source_0dB_From_Network
import org.noise_planet.noisemodelling.wps.Geometric_Tools.Set_Height
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_Symuvia
import org.noise_planet.noisemodelling.wps.Dynamic.Ind_Vehicles_2_Noisy_Vehicles
import org.noise_planet.noisemodelling.wps.Dynamic.Noise_From_Attenuation_Matrix
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalTime

class Run {

    public static void main(String[] args) {
        //RunSUMO("fcd_filtered_output_32633", "SUMO_acc",5)
        //RunSUMO("fcd_filtered_output_32633_noacc", "SUMO",5)
        //RunFlow("SPACE_MEAN_filtered")
        //RunFlow("TIME_MEAN_filtered")
        //RunFlow("Sensor_MEAN_filtered")
        //RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 90, "")
        /*RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "1")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "2")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "3")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "4")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "5")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "6")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "7")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "8")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "9")
        RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5, 300, "10")*/
        //RunDynamicFlow("TIME_MEAN_filtered", "POISSON_nohmin", 5 , 3600, "0")
        //RunDynamicFlow("Sensor_MEAN_filtered", "POISSON_nohmin", 5 , 3600, "0")
        //RunDynamicFlow("Sensor_MEAN_filtered", "PROBA", 5 , 3600, "10")
        PrintLW("TIME_MEAN_filtered", "POISSON_nohmin", 5, 600, "0")
        //PrintLW("TIME_MEAN_filtered", "PROBA", 5, 600, "0")
    }

    static void RunSUMO(String File_name, String Format, int gridStep){
        String dbName = "file:///home/gao/noise_modeling_database"
        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        // Import Buildings for your study area
        new Import_File().exec(connection,
                ["pathFile" :  "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/buildings_nm_ready.shp",
                 "inputSRID": "32633",
                 "tableName": "BUILDINGS"])

        // Import the receivers (or generate your set of receivers using Regular_Grid script for example)
        new Import_File().exec(connection,
                ["pathFile" : "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/receivers_selected.shp",
                 "inputSRID": "32633",
                 "tableName": "RECEIVERS"])

        // Set the height of the receivers
        new Set_Height().exec(connection,
                [ "tableName":"RECEIVERS",
                  "height": 1.5
                ])

        // Import the road network
        new Import_File().exec(connection,
                ["pathFile" :"//home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/TIME_MEAN_filtered.shp",
                 "inputSRID": "32633",
                 "tableName": "network_stockholm"])

        // (optional) Add a primary key to the road network
        new Add_Primary_Key().exec(connection,
                ["pkName" :"PK",
                 "tableName": "network_stockholm"])

        // Import the vehicles trajectories
        new Import_File().exec(connection,
                ["pathFile" : String.format("/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/%s.geojson", File_name) ,
                 "inputSRID": "32633",
                 "tableName": "vehicle"])

        // Create point sources from the network every 10 meters. This point source will be used to compute the noise attenuation level from them to each receiver.
        // The created table will be named SOURCES_0DB
        new Point_Source_0dB_From_Network().exec(connection,
                ["tableNetwork": "network_stockholm",
                 "gridStep" : gridStep
                ])


        // Compute the attenuation noise level from the network sources (SOURCES_0DB) to the receivers
        new Noise_level_from_source().exec(connection,
                ["tableBuilding"   : "BUILDINGS",
                 "tableSources"   : "SOURCES_0DB",
                 "tableReceivers": "RECEIVERS",
                 "confReflOrder": 1,
                 "confMaxReflDist": 500,
                 "confMaxSrcDist" : 500,
                 "confDiffHorizontal" : true,
                 "confDiffVertical" : true,
                 "confExportSourceId": true,
                 "confSkipLday":true,
                 "confSkipLevening":true,
                 "confSkipLnight":true,
                 "confSkipLden":true
                ])

        // Create a table with the noise level from the vehicles and snap the vehicles to the discretized network
        System.out.println("Start Ind_Vehicles_2_Noisy_Vehicles! Current time: " + LocalTime.now());
        new Ind_Vehicles_2_Noisy_Vehicles().exec(connection,
                ["tableVehicles": "vehicle",
                 "distance2snap" : 30,
                 // Insert "SUMO_acc" if acceleration is considered, insert "SUMO" without acceleration
                 "tableFormat" : Format
                ])

        /*new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%d_LW_GEOM.csv',File_name, gridStep),
                "tableToExport" : "LW_DYNAMIC_GEOM"
        ])

        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%d_LW_GEOM.shp',File_name, gridStep),
                "tableToExport" : "LW_DYNAMIC_GEOM"
        ])*/

        // Compute the noise level from the moving vehicles to the receivers
        // the output table is called here LT_GEOM and contains the time series of the noise level at each receiver
        System.out.println("Start Noise_From_Attenuation_Matrix! Current time: " + LocalTime.now());
        new Noise_From_Attenuation_Matrix().exec(connection,
                ["lwTable"   : "LW_DYNAMIC_GEOM",
                 "attenuationTable"   : "LDAY_GEOM",
                 "outputTable"   : "LT_GEOM"
                ])

        // This step is optional, it compute the LEQA, LEQ, L10, L50 and L90 at each receiver from the table LT_GEOM
        System.out.println("End Noise_From_Attenuation_Matrix! Current time: " + LocalTime.now());
        String res = new DynamicIndicators().exec(connection,
                ["tableName"   : "LT_GEOM",
                 "columnName"   : "LEQA"
                ])

        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%d.csv',File_name, gridStep),
                "tableToExport" : "LT_GEOM"
        ])

        connection.close();
    }

    static void RunFlow(String File_name){
        String dbName = "file:///home/gao/noise_modeling_database"
        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        // Import Buildings for your study area
        new Import_File().exec(connection,
                ["pathFile" :  "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/buildings_nm_ready.shp",
                 "inputSRID": "32633",
                 "tableName": "BUILDINGS"])

        // Import the receivers (or generate your set of receivers using Regular_Grid script for example)
        new Import_File().exec(connection,
                ["pathFile" : "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/receivers_selected.shp",
                 "inputSRID": "32633",
                 "tableName": "RECEIVERS"])

        // Set the height of the receivers
        new Set_Height().exec(connection,
                [ "tableName":"RECEIVERS",
                  "height": 1.5
                ])

        new Import_File().exec(connection,
                ["pathFile" : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/%s.shp',File_name),
                 "inputSRID": "32633",
                 "tableName" : "traffic_flow"])

        // Set the height of the receivers
        new Set_Height().exec(connection,
                [ "tableName":"traffic_flow",
                  "height": 0.05
                ])

        new Noise_level_from_traffic().exec(connection,
                ["tableBuilding" : "BUILDINGS",
                 "tableRoads" : "traffic_flow",
                 "tableReceivers" : "RECEIVERS",
                 "confReflOrder": 1,
                 "confMaxSrcDist" : 500,
                 "confMaxReflDist": 500,
                 "confDiffHorizontal" : true,
                 "confDiffVertical" : true,
                 "confSkipLevening":true,
                 "confSkipLnight":true,
                 "confSkipLden":true
                ])

        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_LDAY_GEOM.csv',File_name),
                "tableToExport" : "LDAY_GEOM"
        ])

        connection.close();
    }

    static void RunDynamicFlow(String File_name,String method, int grid, int duration, String name){
        String dbName = "file:///home/gao/noise_modeling_database"
        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        new Import_File().exec(connection,
                ["pathFile" :  "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/buildings_nm_ready.shp",
                 "inputSRID": "32633",
                 "tableName": "BUILDINGS"])

        new Import_File().exec(connection,
                ["pathFile" : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/%s.shp',File_name),
                 "inputSRID": "32633",
                 "tableName" : "ROADS"])

        // (optional) Add a primary key to the road network
        new Add_Primary_Key().exec(connection,
                ["pkName" :"PK",
                 "tableName": "ROADS"])

        // Import the receivers (or generate your set of receivers using Regular_Grid script for example)
        new Import_File().exec(connection,
                ["pathFile" : "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/receivers_selected.shp",
                 "inputSRID": "32633",
                 "tableName": "RECEIVERS"])

        // Set a height to the receivers at 1.5 m
        new Set_Height().exec(connection,
                [ "tableName":"RECEIVERS",
                  "height": 1.5
                ])

        // From the network with traffic flow to individual trajectories with associated Lw using the Poisson method
        // This method place the vehicles on the network according to the traffic flow following a poisson law
        // It keeps a coherence in the time series of the noise level
        // save start time
        System.out.println("Start Flow_2_Noisy_Vehicles! Current time: " + LocalTime.now());
        new Flow_2_Noisy_Vehicles().exec(connection,
                ["tableRoads": "ROADS",
                 "method": method,
                 "timestep": 1,
                 "gridStep" : grid,
                 //duration plus 1 when PROBA
                 "duration" : duration])


        // print time of exec Flow2noisy and write it somewhere
        // Compute the attenuation noise level from the network sources (SOURCES_0DB) to the receivers
        System.out.println("Start Noise_level_from_source! Current time: " + LocalTime.now());
        new Noise_level_from_source().exec(connection,
                ["tableBuilding"   : "BUILDINGS",
                 "tableSources"   : "SOURCES_0DB",
                 "tableReceivers": "RECEIVERS",
                 "confReflOrder": 1,
                 "confMaxReflDist": 500,
                 "confMaxSrcDist" : 500,
                 "confDiffHorizontal" : true,
                 "confDiffVertical" : true,
                 "confExportSourceId": true,
                 "confSkipLday":true,
                 "confSkipLevening":true,
                 "confSkipLnight":true,
                 "confSkipLden":true
                ])

        // Compute the noise level from the moving vehicles to the receivers
        /*new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%s_LW_GEOM_%s.csv',File_name, method, name),
                "tableToExport" : "LW_DYNAMIC_GEOM"
        ])
        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%s_LW_GEOM.shp',File_name, method),
                "tableToExport" : "LW_DYNAMIC_GEOM"
        ])*/
        // the output table is called here LT_GEOM and contains the time series of the noise level at each receiver
        System.out.println("Start Noise_From_Attenuation_Matrix! Current time: " + LocalTime.now());
        new Noise_From_Attenuation_Matrix().exec(connection,
                ["lwTable"   : "LW_DYNAMIC_GEOM",
                 "attenuationTable"   : "LDAY_GEOM",
                 "outputTable"   : "LT_GEOM"
                ])

        System.out.println("End Noise_From_Attenuation_Matrix! Current time: " + LocalTime.now());
        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%s_%s_LDAY_GEOM_%s.csv',File_name, method, duration, name),
                "tableToExport" : "LT_GEOM"
        ])

        // This step is optional, it compute the LEQA, LEQ, L10, L50 and L90 at each receiver from the table LT_GEOM
        new DynamicIndicators().exec(connection,
                ["tableName"   : "LT_GEOM",
                 "columnName"   : "LEQA"
                ])

        connection.close();
    }

    static void PrintLW(String File_name,String method, int grid, int duration, String name){
        String dbName = "file:///home/gao/noise_modeling_database"
        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        new Import_File().exec(connection,
                ["pathFile" :  "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/buildings_nm_ready.shp",
                 "inputSRID": "32633",
                 "tableName": "BUILDINGS"])

        new Import_File().exec(connection,
                ["pathFile" : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/%s.shp',File_name),
                 "inputSRID": "32633",
                 "tableName" : "ROADS"])

        // (optional) Add a primary key to the road network
        new Add_Primary_Key().exec(connection,
                ["pkName" :"PK",
                 "tableName": "ROADS"])

        // Import the receivers (or generate your set of receivers using Regular_Grid script for example)
        new Import_File().exec(connection,
                ["pathFile" : "/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/receivers_selected.shp",
                 "inputSRID": "32633",
                 "tableName": "RECEIVERS"])

        // Set a height to the receivers at 1.5 m
        new Set_Height().exec(connection,
                [ "tableName":"RECEIVERS",
                  "height": 1.5
                ])

        // From the network with traffic flow to individual trajectories with associated Lw using the Poisson method
        // This method place the vehicles on the network according to the traffic flow following a poisson law
        // It keeps a coherence in the time series of the noise level
        // save start time
        System.out.println("Start Flow_2_Noisy_Vehicles! Current time: " + LocalTime.now());
        new Flow_2_Noisy_Vehicles().exec(connection,
                ["tableRoads": "ROADS",
                 "method": method,
                 "timestep": 1,
                 "gridStep" : grid,
                 //duration plus 1 when PROBA
                 "duration" : duration])

        // Compute the noise level from the moving vehicles to the receivers
        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/Downloads/Noise/SUMO/Files_for_Yu/Sodermalm/Hornsgatan/synthetic_traffic_SUMO/syntatic/high/output/%s_%s_LW_GEOM_%s.csv',File_name, method, name),
                "tableToExport" : "LW_DYNAMIC_GEOM"
        ])

        connection.close();
    }
}