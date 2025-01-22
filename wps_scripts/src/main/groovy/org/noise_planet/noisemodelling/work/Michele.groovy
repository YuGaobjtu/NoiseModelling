package org.noise_planet.noisemodelling.work

import groovy.sql.Sql
import org.apache.commons.cli.*
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.noise_planet.noisemodelling.wps.Receivers.Building_Grid
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.DynamicIndicators
import org.noise_planet.noisemodelling.wps.Database_Manager.Add_Primary_Key
import org.noise_planet.noisemodelling.wps.Geometric_Tools.Point_Source_0dB_From_Network
import org.noise_planet.noisemodelling.wps.Geometric_Tools.Set_Height
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_Symuvia
import org.noise_planet.noisemodelling.wps.NoiseModelling.Ind_Vehicles_2_Noisy_Vehicles
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_From_Attenuation_Matrix
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.ResultSet

class Michele {

    static boolean postgis = false;
    static String postgis_db = "postgis_db_nm"
    static String postgis_user = "postgis_user"
    static String postgis_password = "postgis"

    static boolean doCleanDB = false;
    static boolean doImportOSMPbf = false;
    static boolean doExportRoads = false;
    static boolean doExportBuildings = false;
    static boolean doImportData = false;
    static boolean doExportResults = false;
    static boolean doTrafficSimulation = false;

    // all flags inside doSimulation
    static boolean doImportMatsimTraffic = true;
    static boolean doCreateReceiversFromMatsim = true;
    static boolean doCalculateNoisePropagation = true;
    static boolean doCalculateNoiseMap = true;
    static boolean doCalculateExposure = true;
    static boolean doIsoNoiseMap = true;


    static int timeBinSize = 900;
    static int timeBinMin = 0;
    static int timeBinMax = 86400;

    static String receiversMethod = "closest"  // random, closest
    static String ignoreAgents = ""

    // acoustic propagation parameters
    static boolean diffHorizontal = true
    static boolean diffVertical = false
    static int reflOrder = 1
    static int maxReflDist = 50
    static def maxSrcDist = 750

    public static void main(String[] args) {
        //runLyonEdgt100p()
        //RunMatSim(args)
        //RunSymuvia()
        //RunStutorial()
        //export_table("Remove", 5800, 5900)
        CallMichle()
    }

    static void RunMichele(String File_name, int start_time, int end_time){
        String dbName = "file:///home/gao/Michele"
        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        new Import_OSM().exec(connection, [
                "pathFile"      : "/home/gao/IdeaProjects/NoiseModelling/wps_scripts/src/test/resources/org/noise_planet/noisemodelling/wps/SymuviaTest/Lyon_L63V.pbf",
                "targetSRID"    : 2154
        ]);

        //In order to reduce the calculation costs, the potential noise point sources will be captured at designated points
        //Here the designated points are from road links with a fixed 20m distance
        new Import_File().exec(connection, [
                pathFile : "/home/gao/Downloads/Noise/Noise_Data/Michele/School/Receiver/Random_receiver_40_new.shp",
                inputSRID : 2154,
                tableName : "RECEIVERS"])


        new Import_Symuvia().exec(connection, [
                pathFile : String.format("/home/gao/jnotebook/Noise Data/Michele_Symuvia/Divided/%s_%d_%d.xml", File_name, start_time, end_time),
                inputSRID : 2154,
                tableName : 'SYMUVIA'
        ])
        //note: the final name of the output table will plus "_TRAJ", here it is "SYMUVIA_TRAJ"

        new Set_Height().exec(connection,
                [ "tableName":"RECEIVERS",
                  "height": 1,
                ])

        // create a function to define a network
        new Point_Source_0dB_From_Network().exec(connection,
                ["tableRoads": "ROADS",
                 "gridStep" : 10
                ])

        // create a function to get LW values from Vehicles
        new Ind_Vehicles_2_Noisy_Vehicles().exec(connection,
                ["tableVehicles": "SYMUVIA_TRAJ",
                 "distance2snap" : 20,
                 "fileFormat" : "SYMUVIA"])


        new Noise_level_from_source().exec(connection,
                ["tableBuilding"   : "BUILDINGS",
                 "tableSources"   : "SOURCES_0DB",
                 "tableReceivers": "RECEIVERS",
                 "maxError" : 0.0,
                 "confMaxSrcDist" : 500,
                 "confExportSourceId": true,
                 "confSkipLday":true,
                 "confSkipLevening":true,
                 "confSkipLnight":true,
                 "confSkipLden":true
                ])

        new Noise_From_Attenuation_Matrix().exec(connection,
                ["lwTable"   : "LW_DYNAMIC_GEOM",
                 "attenuationTable"   : "LDAY_GEOM",
                 "outputTable"   : "LT_GEOM_PROBA"
                ])


        new DynamicIndicators().exec(connection,
                ["tableName"   : "LT_GEOM_PROBA",
                 "columnName"   : "LEQA"
                ])
        //note: the name of the output table is "L_CARS_GEOM"

        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/jnotebook/Noise Data/Michele_Symuvia/Output/Michele_speed/%s/%s_%d_%d.csv',File_name, File_name, start_time, end_time),
                "tableToExport" : "LT_GEOM_PROBA"
        ])

        connection.close();
    }

    static void CallMichle(){
        /*RunMichele("remove_speedLim", 1, 10800);
        System.gc();
        Thread.sleep(20 * 1000);
        RunMichele("Remove", 1, 10800);*/
        RunMichele("Base", 5800, 5900);
        System.gc();
        Thread.sleep(20 * 1000);
        /*RunMichele("Lim_school", 1, 10800);
        System.gc();
        Thread.sleep(20 * 1000);
        RunMichele("Lim", 1, 10800);
        System.gc();
        //Thread.sleep(20 * 1000);
        //RunMichele("Lim_school", 6300, 10800);
        System.gc();
        Thread.sleep(20 * 1000);
        RunMichele("Lim_school", 1, 10800);
        System.gc();
        Thread.sleep(20 * 1000);
        RunMichele("Lim", 1, 10800);
        RunMichele("remove_speedLim", 6300, 10800);
        RunMichele("Lim_school", 5400, 10800)
        Thread.sleep(20 * 1000);*/
    }

    static void export_table(String File_name, int start_time, int end_time){

        String dbName = "file:///home/gao/Michele"
        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        new Export_Table().exec(connection, [
                "exportPath"    : String.format('/home/gao/jnotebook/Noise Data/Michele_Symuvia/Output/Michele_speed/%s/%s_%d_%d.csv',File_name, File_name, start_time, end_time),
                "tableToExport" : "LT_GEOM_PROBA"
        ])
    }

}
