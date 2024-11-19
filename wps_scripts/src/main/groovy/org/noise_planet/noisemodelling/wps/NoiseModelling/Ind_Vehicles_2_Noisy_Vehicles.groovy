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

/**
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Valetin Le Bescond, Université Gustave Eiffel, Ghent University
 */

package org.noise_planet.noisemodelling.wps.NoiseModelling

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.locationtech.jts.geom.*
import org.noise_planet.noisemodelling.emission.road.cnossosvar.RoadVehicleCnossosvar
import org.noise_planet.noisemodelling.emission.road.cnossosvar.RoadVehicleCnossosvarParameters

import java.sql.Connection
import java.sql.SQLException

title = 'Ind_Vehicles_2_Noisy_Vehicles including snap2roadnetwork'
description = 'Calculating dynamic road emissions based on vehicles trajectories.' +
        '</br> </br> <b> The output table is called : LW_DYNAMIC_GEOM </b> ' +
        'and contain : </br>' +
        '-  <b> T  </b> : The TIMESTAMP iteration (STRING).</br>' +
        '-  <b> IDRECEIVER  </b> : an identifier (INTEGER, PRIMARY KEY). </br>' +
        '- <b> THE_GEOM </b> : the 3D geometry of the receivers (POINT). </br> ' +
        '-  <b> HZ63, HZ125, HZ250, HZ500, HZ1000,HZ2000, HZ4000, HZ8000 </b> : 8 columns giving the day emission sound level for each octave band (FLOAT).'

inputs = [
        tableVehicles : [name : 'tableVehicles',
                  title : "tableVehicles",
                  description : "timestep, geometry, speed, acceleration, veh_type...",
                  type: String.class],
        distance2snap : [name : 'distance2snap',
                  title : "distance2snap",
                  description : "distance2snap",
                  type: Double.class],
        fileFormat : [name : 'fileFormat',
                  title : "fileFormat",
                  description : "fileFormat",
                  type: String.class],
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]

// Open Connection to Geoserver
static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}

// run the script
def run(input) {

    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}


// main function of the script
def exec(Connection connection, input) {

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // Open sql connection to communicate with the database
    Sql sql = new Sql(connection)

    // output string, the information given back to the user
    String resultString = null

    // print to command window
    System.out.println('Start ')
    def start = new Date()

    // -------------------
    // Get every inputs
    // -------------------

    String vehicles_table_name = input['tableVehicles']

    String fileFormat = input['fileFormat']

    double distance2snap = input['distance2snap']

    // do it case-insensitive
    vehicles_table_name = vehicles_table_name.toUpperCase()
    // Check if srid are in metric projection.
    sridSources = GeometryTableUtilities.getSRID(connection, TableLocation.parse(vehicles_table_name))
    if (sridSources == 3785 || sridSources == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+vehicles_table_name+".")
    if (sridSources == 0) throw new IllegalArgumentException("Error : The table "+vehicles_table_name+" does not have an associated SRID.")

    System.out.println('Start  time : ' + TimeCategory.minus(new Date(), start))


    VehicleEmissionProcessData vehicleEmissionProcessData = new VehicleEmissionProcessData();
    vehicleEmissionProcessData.setDynamicEmissionTable(vehicles_table_name, sql, fileFormat)

    sql.execute("CREATE SPATIAL INDEX ON LW_VEHICLE(the_geom);")
    sql.execute("CREATE SPATIAL INDEX ON SOURCES_0dB(the_geom);")

    sql.execute("drop table if exists LW_DYNAMIC_NEW;")
    // Associate Geometry column to the table LDEN
    sql.execute("create table LW_DYNAMIC_NEW as SELECT b.IT as T ,b.Hz63, b.Hz125, b.Hz250, b.Hz500, b.Hz1000, b.Hz2000, b.Hz4000, b.Hz8000, (SELECT a.PK FROM SOURCES_0dB a WHERE ST_EXPAND(b.the_geom,"+distance2snap+","+distance2snap+") && a.the_geom  ORDER BY ST_Distance(a.the_geom, b.the_geom) ASC LIMIT 1) PK FROM LW_VEHICLE b ;")
    sql.execute("DROP TABLE IF EXISTS LW_DYNAMIC_GEOM;")
    sql.execute("CREATE TABLE LW_DYNAMIC_GEOM AS SELECT a.*, b.THE_GEOM FROM LW_DYNAMIC_NEW a,SOURCES_0dB b WHERE a.PK = b.PK ;")
    sql.execute("DROP TABLE IF EXISTS LW_DYNAMIC_NEW;")

    sql.execute("CREATE SPATIAL INDEX ON LW_DYNAMIC_GEOM(the_geom);")

   // sql.execute("DROP TABLE IF EXISTS ROAD_POINTS")
    sql.execute("DROP TABLE IF EXISTS LW_VEHICLE")
    sql.execute("drop table LW_DYNAMIC if exists;")

    System.out.println('Intermediate  time : ' + TimeCategory.minus(new Date(), start))
    System.out.println("Export data to table")


    resultString = "Calculation Done ! The table LW_DYNAMIC_GEOM has been created."

    // print to command window
    System.out.println('Result : ' + resultString)
    System.out.println('End ')
    System.out.println('Duration : ' + TimeCategory.minus(new Date(), start))

    // print to WPS Builder
    return resultString
}



/**
 *
 */
/*class Road {

    long id
    String type
    LineString geom
    int lv
    double lv_spd
    int hv
    double hv_spd
    double length

    public List<SourcePoint> source_points = new ArrayList<SourcePoint>()

    List<LineSegment> line_segments = new ArrayList<LineSegment>()

    List<Vehicle> vehicles = new ArrayList<Vehicle>()

    Map<String, LwCorrectionGenerator> lw_corr_generators = new LinkedHashMap<>()

    int seed = 2528432

    Road(){
        line_segments.clear()
        vehicles.clear()
        lw_corr_generators.clear()
        source_points.clear()
    }

    void setRoad(long id, String type, Geometry geom, int lv, double lv_spd, int hv, double hv_spd) {
        if (geom.getGeometryType() == "MultiLineString") {
            geom = geom.getGeometryN(0)
        }
        if (geom.getGeometryType() != "LineString") {
            throw new InvalidParameterException("Only LineString Geometry is supported")
        }
        this.id = id
        this.type = type
        this.geom = (LineString) geom
        this.lv = lv
        this.lv_spd = lv_spd
        this.hv = hv
        this.hv_spd = hv_spd
        this.length = geom.getLength()

        Coordinate[] coordinates = geom.getCoordinates()
        for(int i = 1; i < coordinates.length; i++){
            line_segments.add(new LineSegment(coordinates[i-1], coordinates[i]));
        }

        /*for (String vehicle_type: [
                Vehicle.LIGHT_VEHICLE_TYPE, Vehicle.MEDIUM_VEHICLE_TYPE, Vehicle.HEAVY_VEHICLE_TYPE,
                Vehicle.MOPEDS_VEHICLE_TYPE, Vehicle.MOTORCYCLE_VEHICLE_TYPE
        ]) {
            lw_corr_generators.put(vehicle_type, new LwCorrectionGenerator(vehicle_type, 1.0, "meanEn"))
        }

        double start
        DisplacedNegativeExponentialDistribution distribution = new DisplacedNegativeExponentialDistribution(lv, 1, seed)
        start = 0
        def samples = distribution.getSamples(lv)
        for (int i = 0; i < lv; i++) {
            start += samples[i]
            vehicles.add(new Vehicle(lv_spd / 3.6, length, start, Vehicle.LIGHT_VEHICLE_TYPE, (i % 2 == 1), 0))
        }
        distribution = new DisplacedNegativeExponentialDistribution(hv, 1, seed)
        start = 0
        samples = distribution.getSamples(hv)
        for (int i = 0; i < hv; i++) {
            start += samples[i]
            vehicles.add(new Vehicle(hv_spd / 3.6, length, start, Vehicle.HEAVY_VEHICLE_TYPE, (i % 2 == 1), 0))
        }
        for (Vehicle vehicle: vehicles) {
            vehicle.lw_correction = 2 //lw_corr_generators.get(vehicle.vehicle_type).generate()
        }
    }

    void move(double time, double max_time) {
        resetSourceLevels()
        for (Vehicle vehicle in vehicles) {
            vehicle.move(time, max_time)
            if (vehicle.exists) {
                updateSourceLevels(vehicle)
            }
        }
    }

    void updateSourceLevels(Vehicle vehicle) {
        SourcePoint closest = null
        SourcePoint secondary_closest = null
        Coordinate vehicle_point = getPoint(vehicle.getPosition())
        double distance = -1
        double secondary_distance = -1
        for (SourcePoint source in source_points) {
            double dist = vehicle_point.distance(source.geom.getCoordinate())
            if (distance == -1) {
                closest = source
                distance = dist
                continue
            }
            if (dist < distance) {
                secondary_closest = closest
                secondary_distance = distance
                closest = source
                distance = dist
                continue
            }
            if (dist < secondary_distance) {
                secondary_closest = source
                secondary_distance = dist
            }
        }
        double[] vehicle_levels = vehicle.getLw()
        double primary_weight = 1.0
        double secondary_weight = 0.0
        if (secondary_closest != null) {
            primary_weight = 1 - distance / (distance + secondary_distance)
            secondary_weight = 1 - secondary_distance / (distance + secondary_distance)
        }
        for (int freq = 0; freq < closest.levels.length; freq++) {
            closest.levels[freq] = 10 * Math.log10(Math.pow(10, closest.levels[freq] / 10) + primary_weight * Math.pow(10, vehicle_levels[freq] / 10))
            if (secondary_closest != null) {
                secondary_closest.levels[freq] = 10 * Math.log10(Math.pow(10, secondary_closest.levels[freq] / 10) + secondary_weight * Math.pow(10, vehicle_levels[freq] / 10))
            }
        }
    }

    void resetSourceLevels() {
        for (SourcePoint source in source_points) {
            for (int freq = 0; freq < source.levels.length; freq++) {
                source.levels[freq] = -99.0
            }
        }
    }

    Coordinate getPoint(double position) {
        double vh_pos = position % length
        vh_pos = (vh_pos + length) % length // handle negative positions (backward vehicles)
        double accum_length = 0.0
        Coordinate result = null
        for (LineSegment line in line_segments) {
            if ((line.getLength() + accum_length) < vh_pos) {
                accum_length += line.getLength()
                continue
            }
            double vh_pos_fraction = (vh_pos - accum_length) / line.getLength()
            result = line.pointAlong(vh_pos_fraction)
            break
        }
        return result
    }

    int getCode() {
        if (!type_codes.containsKey(type)) {
            return 0
        }
        return type_codes.get(type).toInteger()
    }


}


class SourcePoint {
    long id
    Point geom
    int[] freqs = [63, 125, 250, 500, 1000, 2000, 4000, 8000];
    double[] levels = new double[freqs.length];

    SourcePoint(long id, Geometry geom) {
        if (geom.getGeometryType() != "Point") {
            throw new InvalidParameterException("Only Point Geometry is supported")
        }
        this.id = id
        this.geom = (Point) geom
    }
}
/*
class Vehicle {

    final static String LIGHT_VEHICLE_TYPE = "1"
    final static String MEDIUM_VEHICLE_TYPE = "2"
    final static String HEAVY_VEHICLE_TYPE = "3"
    final static String MOPEDS_VEHICLE_TYPE = "4"
    final static String MOTORCYCLE_VEHICLE_TYPE = "4"

    static int last_id = 0

    static do_loop = false
    static Random rand = new Random(681254665)

    String vehicle_type = LIGHT_VEHICLE_TYPE
    int id = 0
    double position = 0.0
    double max_position = 0.0
    double speed = 0.0 // m/s
    double time_offset = 10.0 // shift everything by X seconds to ensure enough traffic exists
    double time = 0.0
    double start_time = 0
    boolean exists = false
    boolean backward = false

    double lw_correction = 0.0

    static int getNextId() {
        last_id++
        return last_id
    }

    Vehicle(double speed, double length, double start, String type, boolean is_back, int road_type) {
        max_position = length
        vehicle_type = type
        start_time = start
        backward = is_back
        id = getNextId()

        if (road_type == 0 ) {
            this.speed = (3 * speed / 4) + (rand.nextGaussian() + 1) * (speed / 4)
        }
        if (this.vehicle_type == HEAVY_VEHICLE_TYPE || this.vehicle_type == MEDIUM_VEHICLE_TYPE) {
            this.speed = Math.min(this.speed, 90 / 3.6) // max 90km/h for heavy vehicles
        }
    }

    Vehicle(double speed, double length, double start) {
        this(speed, length, start, LIGHT_VEHICLE_TYPE, false, 5113)
    }

    Vehicle(double speed, double length) {
        this(speed, length, 0.0, LIGHT_VEHICLE_TYPE, false, 5113)
    }

    void move(double input_time, double max_time) {
        time = (input_time + time_offset) % max_time
        double real_speed = (backward ? (-1 * speed) : speed)
        if (do_loop) {
            exists = true
            position = ((time + max_time + start_time) % max_time) * real_speed
        }
        else {
            if (time >= start_time) {
                exists = true
                position = ((time - start_time) % max_time) * real_speed
            } else {
                exists = false
            }
            if (position > max_position || position < -max_position) {
                exists = false
            }
        }
    }

    double getPosition() {
        return position % max_position;
    }

    double[] getLw() {
        int[] freqs = [63, 125, 250, 500, 1000, 2000, 4000, 8000];
        double[] result = new double[freqs.length];
        if (!exists) {
            for (int i = 0; i < freqs.length; i++) {
                result[i] = -99.0;
            }
            return result;
        }
        for (int i = 0; i < freqs.length; i++) {

            RoadVehicleCnossosvarParameters rsParametersDynamic = new RoadVehicleCnossosvarParameters(
                    speed * 3.6, 0, vehicle_type, 0,  true, 1, id      )
            rsParametersDynamic.setRoadSurface("DEF")
            // remove lw_correction
            result[i] = RoadVehicleCnossosvar.evaluate(rsParametersDynamic) + lw_correction;
        }
        return result;
    }
}
*/

/*class LwCorrectionGenerator {

    static Random rand = new Random(546656812)


    final private static LinkedHashMap<String, List<Double> > distributions = [
            (Vehicle.LIGHT_VEHICLE_TYPE) : [0],
            (Vehicle.MEDIUM_VEHICLE_TYPE) : [0],
            (Vehicle.HEAVY_VEHICLE_TYPE) : [0],
            (Vehicle.MOPEDS_VEHICLE_TYPE) : [0],
            (Vehicle.MOTORCYCLE_VEHICLE_TYPE) : [0]
    ];

    List<Double> xy_values;
    List<Double> partition;
    double dx = 1.0


    LwCorrectionGenerator(String type, double dx, String zero_point) {
        List<Double> values = new ArrayList<Double>(distributions.get(type))
        int n = values.size()
        this.dx = dx
        List<Double> cum_dist = cumDist(values)
        partition = new ArrayList<Double>(cum_dist)
        partition.remove(n-1)
        xy_values = new ArrayList<Double>()
        double median = 0
        double meandB = 0
        double meanEn = 0
        for (double i = 0; i < n; i +=dx) {
            xy_values.add(i)
            if (median <= 0 && cum_dist[(int) i] >= 50) {
                median = i
            }
        }
        List<Double> xy_values_en = new ArrayList<Double>()
        for (int i = 0; i < xy_values.size(); i++) {
            xy_values_en.add(Math.pow(10, xy_values[i] / 10))
        }
        meandB = multiplyAndSum(values, xy_values) / sum(values)
        meanEn = 10 * Math.log10(multiplyAndSum(values, xy_values_en) / sum(values))
        if (zero_point == "median") {
            xy_values = xy_values.stream().map({ e -> e - median}).collect(Collectors.toList())
        }
        else if (zero_point == "meandB") {
            xy_values = xy_values.stream().map({ e -> e - meandB}).collect(Collectors.toList())
        }
        else {
            xy_values = xy_values.stream().map({ e -> e - meanEn}).collect(Collectors.toList())
        }
    }

    double generate() {
        def result = xy_values[rouletteRand(partition)]
        result += uniRand(-dx/2.0,dx/2.0)
        return result
    }

    static List<Double> cumDist(List<Double> array) {
        List<Double> input = new ArrayList<Double>(array)
        double divide = sum(input)
        input = input.stream().map({ e -> e / divide }).collect(Collectors.toList())
        List<Double> out = new ArrayList<Double>()
        out.add(input[0])
        for (int i = 1; i < input.size(); i++)
            out.add(out.last() + input[i])
        out = out.stream().map({ e -> e * 100.0}).collect(Collectors.toList())
        return out
    }
    static double sum(List<Double> list) {
        double sum = 0;
        for (double i : list)
            sum = sum + i;
        return sum;
    }
    static double multiplyAndSum(List<Double> list1, List<Double> list2) {
        int max = Math.min(list1.size(), list2.size());
        double sum = 0;
        for (int i = 0; i < max; i++) {
            sum += list1[i] * list2[i];
        }
        return sum;
    }

    static double uniRand(double left, double right) {
        return left + rand.nextDouble() * (right - left)
    }
    static double rouletteRand(List<Double> partition) {
        double r = rand.nextDouble() * 100.0
        double v = 0
        for (i in 0..<partition.size()) {
            if (r > partition[i]) {
                v = i + 1
            }
        }
        return v
    }

}

*/
/*
abstract class HeadwayDistribution {

    protected static int seed;
    Random random;

    HeadwayDistribution(int seed) {
        this.seed = seed;
        random = new Random(seed);
    }

    HeadwayDistribution() {
        this(1234)
    }

    abstract double inverseCumulativeProbability(double p);

    double getNext() {
        return inverseCumulativeProbability(random.nextDouble())
    }

    double[] getSamples(int n) {
        double[] result = new double[n];
        for (i in 0..<n) {
            result[i] = getNext()
        }
        return result
    }
}
/*
// De Coensel, B.; Brown, A.L.; Tomerini, D. A road traffic noise pattern simulation model that includes distributions of vehicle sound power levels. Appl. Acoust. 2016, 111, 170–178.
class DisplacedNegativeExponentialDistribution extends HeadwayDistribution {

    int hmin
    double q; // number of vehicles per second !
    double lambda;

    DisplacedNegativeExponentialDistribution(int rate, int hmin) { // rate = veh/hour
        this(rate, hmin, seed)
    }
    DisplacedNegativeExponentialDistribution(int rate, int hmin, int seed) { // rate = veh/hour
        super(seed);
        this.q = rate / 3600
        this.hmin = hmin
        this.lambda = q / (1.0 - q * hmin)
    }

    @Override
    double inverseCumulativeProbability(double p) {
        // cumulative probability: p = 1 - exp[-lambda*(t-hmin)]
        return hmin - Math.log(1.0 - p) / lambda
    }
}

/**
 *
 */
class VehicleEmissionProcessData {


    void setDynamicEmissionTable(String tablename, Sql sql, String fileFormat) throws SQLException {

        //////////////////////
        // Import file text
        //////////////////////

        sql.execute("drop table if exists LW_DYNAMIC;")
        sql.execute("create table LW_VEHICLE(IT integer, THE_GEOM geometry, Hz63 double precision, Hz125 double precision, Hz250 double precision, Hz500 double precision, Hz1000 double precision, Hz2000 double precision, Hz4000 double precision, Hz8000 double precision);")
        def qry = 'INSERT INTO LW_VEHICLE(IT , THE_GEOM,Hz63, Hz125, Hz250, Hz500, Hz1000,Hz2000, Hz4000, Hz8000) VALUES (?,?,?,?,?,?,?,?,?,?);'

        if (fileFormat.equals("SUMO")){
                // Remplissage des variables avec le contenu du fichier SUMO
                sql.eachRow('SELECT THE_GEOM, SPEED, ID, TIMESTEP FROM ' + tablename + ';') { row ->

                    Geometry the_geom = (Geometry) row[0]
                    double speed = (double) row[1]
                    String id_veh = (String) row[2]
                    int timestep = (int) row[3]

                    double[] carLevel = getCarsLevel(speed*3.6, id_veh,0,"VL")
                    sql.withBatch(100, qry) { ps ->
                        ps.addBatch(timestep as Integer, the_geom as Geometry,
                                carLevel[0] as Double, carLevel[1] as Double, carLevel[2] as Double,
                                carLevel[3] as Double, carLevel[4] as Double, carLevel[5] as Double,
                                carLevel[6] as Double, carLevel[7] as Double)
                    }

                }
        } else if (fileFormat.equals("SYMUVIA")){
            // Remplissage des variables avec le contenu du fichier SUMO
            sql.eachRow('SELECT ID_VEH,THE_GEOM, "TYPE", SPEED, ACC,  TIME,PK FROM ' + tablename + ';') { row ->

                Geometry the_geom = (Geometry) row[1]
                double speed = (double) row[3]
                double acc = (double) row[4]
                String typeVeh = (String) row[2]
                int id_veh = (int) row[0]
                int timestep = (int) row[5]

                double[] carLevel = getCarsLevel(speed*3.6, id_veh, acc, typeVeh)
                sql.withBatch(100, qry) { ps ->
                    ps.addBatch(timestep as Integer, the_geom as Geometry,
                            carLevel[0] as Double, carLevel[1] as Double, carLevel[2] as Double,
                            carLevel[3] as Double, carLevel[4] as Double, carLevel[5] as Double,
                            carLevel[6] as Double, carLevel[7] as Double)
                }

            }

        }  else    {
            System.out.println("Unknown File Format")

        }


    }

    double[] getCarsLevel(double speed, int id_veh, double acc, String typeVeh) throws SQLException {

        double[] res_LV = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

        int kk = 0
        def list = [63, 125, 250, 500, 1000, 2000, 4000, 8000]

        for (f in list) {
            String RoadSurface = "FR_R2"
            boolean Stud = false
            String veh_type = "1"
            switch(typeVeh) {
                case 'VL':
                    veh_type = '1'
                    break
                case 'PL':
                    veh_type = '3'
                    break
                case 'TypeTrolley':
                    veh_type = '3'
                    break
                case 'BUS':
                    veh_type = '3'
                    break
            }

            int acc_type= 2
            double LwStd= 0


            RoadVehicleCnossosvarParameters rsParameters = new RoadVehicleCnossosvarParameters(speed,  acc,  veh_type, acc_type, Stud,LwStd,id_veh)
            //System.println(rsParameters)
            rsParameters.setSlopePercentage(0)
            rsParameters.setRoadSurface(RoadSurface)
            rsParameters.setFileVersion(1)
            rsParameters.setTemperature(20)
            rsParameters.setFrequency(f)
            res_LV[kk] = RoadVehicleCnossosvar.evaluate(rsParameters)
            kk++
        }




        return res_LV
    }



}
