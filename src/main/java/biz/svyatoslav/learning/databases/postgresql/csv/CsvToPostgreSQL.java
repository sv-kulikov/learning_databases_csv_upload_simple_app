package biz.svyatoslav.learning.databases.postgresql.csv;

import biz.svyatoslav.learning.databases.postgresql.csv.structure.*;
import biz.svyatoslav.learning.databases.postgresql.csv.util.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Year;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// ToDo: WARNING! BOTH the database mode AND this code CONTAIN ERRORS!
// ToDo: These errors are placed here (un)intentionally :). Do NOT try to copy-paste the code mindlessly.

// ToDo: It is a nice idea to rewrite checks as real tests with TestNG or JUnit.

public class CsvToPostgreSQL {

    // 0) See the CSV data, database model and SQL in "data" folder of this project.

    // 1) On your VM enable inbound connections in firewall.
    // 2) To "C:/Program Files/PostgreSQL/15/data/pg_hba.conf" add this line (set your own host OS ip!):
    //    host    all    		all    		192.168.1.1/32    	md5
    // 3) Restart PostgreSQL service.

    // 4) To check results open pgAdmin and execute the next queries:
    //    SELECT * FROM continents ORDER BY name;
    //    SELECT * FROM countries ORDER BY name;
    //    SELECT * FROM patients ORDER BY year_of_birth;
    //    SELECT *
    //     FROM patients
    //     LEFT JOIN countries ON patients.country_id = countries.country_id
    //     LEFT JOIN continents ON countries.continent_id = continents.continent_id
    //     ORDER BY patients.year_of_birth, countries.name;


    public static void main(String[] args) {
        String jdbcURL = "jdbc:postgresql://192.168.1.102:5432/course";
        String username = "postgres";
        String password = "123456";
        String csvFilePath = "data/heart_attack_prediction_dataset.csv";

        Logger.logError("Yes, this database structure is BAD. I know, thank you for noticing. Greetings to the authors of the task solved here.", true);

        try {
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);

            Logger.logHeader("", true);
            Logger.logHeader("Processing CONTINENTS:", true);
            processContinents(connection, csvFilePath);
            Logger.logHeader("DONE processing CONTINENTS.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing COUNTRIES:", true);
            processCountries(connection, csvFilePath);
            Logger.logHeader("DONE processing COUNTRIES.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing PATIENTS:", true);
            processPatients(connection, csvFilePath);
            Logger.logHeader("DONE processing PATIENTS.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing HEALTH METRICS:", true);
            processHealthMetrics(connection, csvFilePath);
            Logger.logHeader("DONE processing HEALTH METRICS.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing DIETS:", true);
            processDiets(connection, csvFilePath);
            Logger.logHeader("DONE processing DIETS.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing LIFESTYLE FACTORS:", true);
            processLifestyleFactors(connection, csvFilePath);
            Logger.logHeader("DONE processing LIFESTYLE FACTORS.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing MEDICAL HISTORY:", true);
            processMedicalHistory(connection, csvFilePath);
            Logger.logHeader("DONE processing MEDICAL HISTORY.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing MEDICATION:", true);
            processMedication(connection, csvFilePath);
            Logger.logHeader("DONE processing MEDICATION.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing STRESS LEVELS:", true);
            processStressLevels(connection, csvFilePath);
            Logger.logHeader("DONE processing STRESS LEVELS.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing HEART ATTACK RISK:", true);
            processHeartAttackRisk(connection, csvFilePath);
            Logger.logHeader("DONE processing HEART ATTACK RISK.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Processing PATIENT-m2m-MEDICATION:", true);
            processPatientMedication(connection, csvFilePath);
            Logger.logHeader("DONE processing PATIENT-m2m-MEDICATION.", true);

            Logger.logHeader("", true);
            Logger.logHeader("Comparing all the final data...", true);
            compareAllTheFinalData(connection, csvFilePath);
            Logger.logHeader("DONE Comparing all the final data.", true);


            connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static void processContinents(Connection connection, String csvFilePath) throws Exception {
        // Reading continents from CSV
        Set<ContinentInfo> continents = readContinentsFromCSV(csvFilePath);

        // Clearing the "continents" table
        Logger.logMessage("Clearing the \"continents\" table... ");
        String truncateSql = "TRUNCATE TABLE continents CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting continents information into the "continents" table
        Logger.logMessage("Inserting continents information into the \"continents\" table... ");
        String sql = "INSERT INTO continents (name, hemisphere) VALUES (?, ?);";

        PreparedStatement statement = connection.prepareStatement(sql);

        for (ContinentInfo continent : continents) {
            statement.setString(1, continent.name);
            statement.setObject(2, continent.hemisphere, Types.OTHER);
            statement.executeUpdate();
        }
        statement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "continents" table contains the same number of records as the continents set
        Logger.logMessage("Checking if the \"continents\" table contains the same number of records as the continents set... ");
        String countSql = "SELECT COUNT(*) FROM continents;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == continents.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + continents.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all continents from the set are in the "continents" table
        Logger.logMessage("Checking if all continents from the set are in the \"continents\" table... ");
        String selectSql = "SELECT name FROM continents;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbContinents = new HashSet<>();
        while (selectResult.next()) {
            dbContinents.add(selectResult.getString("name"));
        }

        // Getting the names of continents from the continents set
        Set<String> continentNames = continents.stream().map(c -> c.name).collect(Collectors.toSet());

        // Finding the missing continents
        Set<String> missingContinents = new HashSet<>(continentNames);
        missingContinents.removeAll(dbContinents);

        if (missingContinents.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing continents in the table: " + missingContinents, true);
        }
        selectStatement.close();

        // Selecting a random continent from the table
        Logger.logMessage("Selecting a random continent... ");
        String randomSelectSql = "SELECT name, hemisphere FROM continents ORDER BY RANDOM() LIMIT 1;";
        PreparedStatement randomSelectStatement = connection.prepareStatement(randomSelectSql);
        ResultSet randomSelectResult = randomSelectStatement.executeQuery();
        String randomContinentName = null;
        String randomContinentHemisphere = null;
        if (randomSelectResult.next()) {
            randomContinentName = randomSelectResult.getString("name");
            randomContinentHemisphere = randomSelectResult.getString("hemisphere");
        }
        randomSelectStatement.close();
        Logger.logMessage("Selected: " + randomContinentName + ", Hemisphere: " + randomContinentHemisphere, true);

        // Attempting to insert a new continent with the same name and hemisphere
        Logger.logMessage("Attempting to insert a duplicate continent... ");
        String insertSql = "INSERT INTO continents (name, hemisphere) VALUES (?, ?) RETURNING continent_id;";
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        try {
            insertStatement.setString(1, randomContinentName);
            insertStatement.setObject(2, randomContinentHemisphere, Types.OTHER);
            ResultSet insertedRow = insertStatement.executeQuery();
            if (insertedRow.next()) {
                int newContinentId = insertedRow.getInt("continent_id");
                Logger.logError("FAILED! Duplicate insertion succeeded unexpectedly. Inserted continent_id: " + newContinentId, true);

                // Delete the newly inserted continent based on continent_id
                String deleteSql = "DELETE FROM continents WHERE continent_id = ?;";
                PreparedStatement deleteStatement = connection.prepareStatement(deleteSql);
                deleteStatement.setInt(1, newContinentId);
                deleteStatement.execute();
                deleteStatement.close();
            }
        } catch (SQLException e) {
            Logger.logSuccess("Passed. Duplicate insertion failed as expected.", true);
        } finally {
            insertStatement.close();
        }

    }

    private static void processCountries(Connection connection, String csvFilePath) throws Exception {
        // Reading countries from CSV
        Set<CountryInfo> countries = readCountriesFromCSV(csvFilePath);

        // Clearing the "countries" table
        Logger.logMessage("Clearing the \"countries\" table... ");
        String truncateSql = "TRUNCATE TABLE countries CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        Logger.logMessage("Reading \"continents\" data... ");
        Map<String, Integer> continentIds = new HashMap<>();
        String continentSql = "SELECT name, hemisphere, continent_id FROM continents;";
        PreparedStatement continentStatement = connection.prepareStatement(continentSql);
        ResultSet continentResultSet = continentStatement.executeQuery();
        while (continentResultSet.next()) {
            String key = continentResultSet.getString("name") + " - " + continentResultSet.getString("hemisphere");
            continentIds.put(key, continentResultSet.getInt("continent_id"));
        }
        continentStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting countries information into the "countries" table
        Logger.logMessage("Inserting countries information into the \"countries\" table... ");
        String sql = "INSERT INTO countries (name, continent_id) VALUES (?, ?);";

        PreparedStatement statement = connection.prepareStatement(sql);

        for (CountryInfo country : countries) {

            Integer continentId = continentIds.get(country.continent + " - " + country.hemisphere);

            if (continentId == null) {
                Logger.logError("Warning: No proper continent found for " + country.continent + " and " + country.name, true);
                continue; // Skip this country
            }


            statement.setString(1, country.name);
            statement.setInt(2, continentId);
            statement.executeUpdate();
        }
        statement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "countries" table contains the same number of records as the countries set
        Logger.logMessage("Checking if the \"countries\" table contains the same number of records as the countries set... ");
        String countSql = "SELECT COUNT(*) FROM countries;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == countries.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + countries.size() + "], got [ " + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all countries from the set are in the Countries table
        Logger.logMessage("Checking if all countries from the set are in the \"countries\" table... ");
        String selectSql = "SELECT name FROM countries;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbCountries = new HashSet<>();
        while (selectResult.next()) {
            dbCountries.add(selectResult.getString("name"));
        }

        // Getting the names of countries from the 'countries' set
        Set<String> countryNames = countries.stream().map(c -> c.name).collect(Collectors.toSet());

        // Finding the missing countries
        Set<String> missingCountries = new HashSet<>(countryNames);
        missingCountries.removeAll(dbCountries);

        if (missingCountries.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing countries in the table: " + missingCountries, true);
        }
        selectStatement.close();

        // Selecting a random country and its continent from the table
        Logger.logMessage("Selecting a random country... ");
        String randomCountrySelectSql = "SELECT name, continent_id FROM countries ORDER BY RANDOM() LIMIT 1;";
        PreparedStatement randomCountrySelectStatement = connection.prepareStatement(randomCountrySelectSql);
        ResultSet randomCountrySelectResult = randomCountrySelectStatement.executeQuery();
        String randomCountryName = null;
        int randomContinentId = -1;
        if (randomCountrySelectResult.next()) {
            randomCountryName = randomCountrySelectResult.getString("name");
            randomContinentId = randomCountrySelectResult.getInt("continent_id");
        }
        randomCountrySelectStatement.close();
        Logger.logMessage("Selected: " + randomCountryName + " with continent_id: " + randomContinentId, true);

        // Attempting to insert a new country with the same name and continent_id
        Logger.logMessage("Attempting to insert a duplicate country... ");
        String insertCountrySql = "INSERT INTO countries (name, continent_id) VALUES (?, ?) RETURNING country_id;";
        PreparedStatement insertCountryStatement = connection.prepareStatement(insertCountrySql);
        try {
            insertCountryStatement.setString(1, randomCountryName);
            insertCountryStatement.setInt(2, randomContinentId);
            ResultSet insertedCountryRow = insertCountryStatement.executeQuery();
            if (insertedCountryRow.next()) {
                int newCountryId = insertedCountryRow.getInt("country_id");
                Logger.logError("FAILED! Duplicate insertion succeeded unexpectedly. Inserted country_id: " + newCountryId, true);

                // Delete the newly inserted country based on country_id
                String deleteCountrySql = "DELETE FROM countries WHERE country_id = ?;";
                PreparedStatement deleteCountryStatement = connection.prepareStatement(deleteCountrySql);
                deleteCountryStatement.setInt(1, newCountryId);
                deleteCountryStatement.execute();
                deleteCountryStatement.close();
            }
        } catch (SQLException e) {
            Logger.logSuccess("Passed. Duplicate insertion failed as expected.", true);
        } finally {
            insertCountryStatement.close();
        }


    }

    private static void processPatients(Connection connection, String csvFilePath) throws Exception {

        // Reading patients data from CSV
        Set<PatientInfo> patients = readPatientsFromCSV(csvFilePath);

        // Getting country_id for each country name
        Logger.logMessage("Reading \"countries\" data... ");
        Map<String, Integer> countryIds = new HashMap<>();
        String countrySql = "SELECT country_id, name FROM countries;";
        PreparedStatement countryStatement = connection.prepareStatement(countrySql);
        ResultSet countryResultSet = countryStatement.executeQuery();
        while (countryResultSet.next()) {
            countryIds.put(countryResultSet.getString("name"), countryResultSet.getInt("country_id"));
        }
        countryStatement.close();
        Logger.logSuccess("Done.", true);

        // Clearing the "patients" table
        Logger.logMessage("Clearing the \"patients\" table... ");
        String truncateSql = "TRUNCATE TABLE patients CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting patient information into the "patients" table
        Logger.logMessage("Inserting patient information into the \"patients\" table... ");
        String insertSql = "INSERT INTO patients (patient_id, year_of_birth, sex, income, country_id) VALUES (?, ?, ?, ?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (PatientInfo patient : patients) {
            Integer countryId = countryIds.get(patient.country);
            if (countryId == null) {
                Logger.logError("Warning: No proper country found for " + patient.country, true);
                continue; // Skip this patient
            }

            insertStatement.setString(1, patient.patientId);
            insertStatement.setInt(2, patient.yearOfBirth);
            insertStatement.setObject(3, patient.sex, Types.OTHER);
            insertStatement.setInt(4, patient.income);
            insertStatement.setInt(5, countryId);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "patients" table contains the same number of records as the patients set
        Logger.logMessage("Checking if the \"patients\" table contains the same number of records as the patients set... ");
        String countSql = "SELECT COUNT(*) FROM patients;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == patients.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + patients.size() + "], got [ " + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all patient IDs from the set are in the Patients table
        Logger.logMessage("Checking if all patient IDs from the set are in the \"patients\" table... ");
        String selectSql = "SELECT patient_id FROM patients;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbPatientIds.add(selectResult.getString("patient_id"));
        }

        // Getting the patient IDs from the 'patients' set
        Set<String> patientIds = patients.stream().map(p -> p.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(patientIds);
        missingPatientIds.removeAll(dbPatientIds);

        if (missingPatientIds.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing patient IDs in the table: " + missingPatientIds, true);
        }
        selectStatement.close();
    }

    private static void processHealthMetrics(Connection connection, String csvFilePath) throws Exception {

        // Reading health metrics data from CSV
        Set<HealthMetricInfo> healthMetrics = readHealthMetricsFromCSV(csvFilePath);

        // Clearing the "health_metrics" table
        Logger.logMessage("Clearing the \"health_metrics\" table... ");
        String truncateSql = "TRUNCATE TABLE health_metrics CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting health metrics information into the "health_metrics" table
        Logger.logMessage("Inserting health metrics information into the \"health_metrics\" table... ");
        String insertSql = "INSERT INTO health_metrics (patient_id, cholesterol, blood_pressure_systolic, blood_pressure_diastolic, heart_rate, triglycerides, bmi) VALUES (?, ?, ?, ?, ?, ?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (HealthMetricInfo metric : healthMetrics) {
            insertStatement.setString(1, metric.patientId);
            insertStatement.setObject(2, metric.cholesterol, Types.INTEGER);
            insertStatement.setObject(3, metric.bloodPressureSystolic, Types.INTEGER);
            insertStatement.setObject(4, metric.bloodPressureDiastolic, Types.INTEGER);
            insertStatement.setObject(5, metric.heartRate, Types.INTEGER);
            insertStatement.setObject(6, metric.triglycerides, Types.INTEGER);
            insertStatement.setObject(7, metric.bmi, Types.FLOAT);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "health_metrics" table contains the same number of records as the healthMetrics set
        Logger.logMessage("Checking if the \"health_metrics\" table contains the same number of records as the healthMetrics set... ");
        String countSql = "SELECT COUNT(*) FROM health_metrics;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == healthMetrics.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + healthMetrics.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all health metric entries from the set are in the "health_metrics" table
        Logger.logMessage("Checking if all health metric entries from the set are in the \"health_metrics\" table... ");
        String selectSql = "SELECT patient_id FROM health_metrics;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbHealthMetricPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbHealthMetricPatientIds.add(selectResult.getString("patient_id"));
        }

        // Getting the patient IDs from the 'healthMetrics' set
        Set<String> healthMetricPatientIds = healthMetrics.stream().map(hm -> hm.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(healthMetricPatientIds);
        missingPatientIds.removeAll(dbHealthMetricPatientIds);

        if (missingPatientIds.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing health metric entries in the table for patient IDs: " + missingPatientIds, true);
        }
        selectStatement.close();

        // ToDo: as (in theory) there's 1-1 relationship between "patients" and "health_metrics" tables, try checking that it is a 1-1 for real.
    }

    private static void processDiets(Connection connection, String csvFilePath) throws Exception {
        // Reading diets from CSV
        Set<DietInfo> diets = readDietsFromCSV(csvFilePath);

        // Clearing the "diets" table
        Logger.logMessage("Clearing the \"diets\" table... ");
        String truncateSql = "TRUNCATE TABLE diets CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting diet information into the "diets" table
        Logger.logMessage("Inserting diet information into the \"diets\" table... ");
        String sql = "INSERT INTO diets (name) VALUES (?);";

        PreparedStatement statement = connection.prepareStatement(sql);

        for (DietInfo diet : diets) {
            statement.setString(1, diet.name);
            statement.executeUpdate();
        }
        statement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "diets" table contains the same number of records as the diets set
        Logger.logMessage("Checking if the \"diets\" table contains the same number of records as the diets set... ");
        String countSql = "SELECT COUNT(*) FROM diets;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == diets.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + diets.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all diets from the set are in the "diets" table
        Logger.logMessage("Checking if all diets from the set are in the \"diets\" table... ");
        String selectSql = "SELECT name FROM diets;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbDiets = new HashSet<>();
        while (selectResult.next()) {
            dbDiets.add(selectResult.getString("name"));
        }

        // Getting the names of diets from the diets set
        Set<String> dietNames = diets.stream().map(d -> d.name).collect(Collectors.toSet());

        // Finding the missing diets
        Set<String> missingDiets = new HashSet<>(dietNames);
        missingDiets.removeAll(dbDiets);

        if (missingDiets.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing diets in the table: " + missingDiets, true);
        }
        selectStatement.close();

        // Selecting a random diet from the table
        Logger.logMessage("Selecting a random diet... ");
        String randomSelectSql = "SELECT name FROM diets ORDER BY RANDOM() LIMIT 1;";
        PreparedStatement randomSelectStatement = connection.prepareStatement(randomSelectSql);
        ResultSet randomSelectResult = randomSelectStatement.executeQuery();
        String randomDietName = null;
        if (randomSelectResult.next()) {
            randomDietName = randomSelectResult.getString("name");
        }
        randomSelectStatement.close();
        Logger.logMessage("Selected: " + randomDietName, true);

        // Attempting to insert a new diet with the same name
        Logger.logMessage("Attempting to insert a duplicate diet... ");
        String insertSql = "INSERT INTO diets (name) VALUES (?) RETURNING diet_id;";
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        try {
            insertStatement.setString(1, randomDietName);
            ResultSet insertedRow = insertStatement.executeQuery();
            if (insertedRow.next()) {
                int newDietId = insertedRow.getInt("diet_id");
                Logger.logError("FAILED! Duplicate insertion succeeded unexpectedly. Inserted diet_id: " + newDietId, true);

                // Delete the newly inserted diet based on diet_id
                String deleteSql = "DELETE FROM diets WHERE diet_id = ?;";
                PreparedStatement deleteStatement = connection.prepareStatement(deleteSql);
                deleteStatement.setInt(1, newDietId);
                deleteStatement.execute();
                deleteStatement.close();
            }
        } catch (SQLException e) {
            Logger.logSuccess("Passed. Duplicate insertion failed as expected.", true);
        } finally {
            insertStatement.close();
        }

    }

    private static void processLifestyleFactors(Connection connection, String csvFilePath) throws Exception {

        // Reading lifestyle factors data from CSV
        Set<LifestyleFactorInfo> lifestyleFactors = readLifestyleFactorsFromCSV(csvFilePath);

        // Clearing the "lifestyle_factors" table
        Logger.logMessage("Clearing the \"lifestyle_factors\" table... ");
        String truncateSql = "TRUNCATE TABLE lifestyle_factors CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Getting diet_id for each diet name
        Logger.logMessage("Reading \"diets\" data... ");
        Map<String, Integer> dietIds = new HashMap<>();
        String dietSql = "SELECT diet_id, name FROM diets;";
        PreparedStatement dietStatement = connection.prepareStatement(dietSql);
        ResultSet dietResultSet = dietStatement.executeQuery();
        while (dietResultSet.next()) {
            dietIds.put(dietResultSet.getString("name"), dietResultSet.getInt("diet_id"));
        }
        dietStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting lifestyle factors information into the "lifestyle_factors" table
        Logger.logMessage("Inserting lifestyle factors information into the \"lifestyle_factors\" table... ");
        String insertSql = "INSERT INTO lifestyle_factors (patient_id, diet_id, smoking, alcohol_consumption, obesity, exercise_hours_per_week, sedentary_hours_per_day, physical_activity_days_per_week, sleep_hours_per_day) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (LifestyleFactorInfo factor : lifestyleFactors) {

            Integer dietId = dietIds.get(factor.diet);
            if (dietId == null) {
                Logger.logError("Warning: No proper diet found for " + factor.diet, true);
                continue; // Skip this lifestyle factor entry
            }

            insertStatement.setString(1, factor.patientId);
            insertStatement.setInt(2, dietId);
            insertStatement.setBoolean(3, factor.smoking != null ? factor.smoking : false); // Assuming default value as false
            insertStatement.setBoolean(4, factor.alcoholConsumption != null ? factor.alcoholConsumption : false);
            insertStatement.setBoolean(5, factor.obesity != null ? factor.obesity : false);
            insertStatement.setFloat(6, factor.exerciseHoursPerWeek != null ? factor.exerciseHoursPerWeek : 0.0f);
            insertStatement.setFloat(7, factor.sedentaryHoursPerDay != null ? factor.sedentaryHoursPerDay : 0.0f);
            insertStatement.setInt(8, factor.physicalActivityDaysPerWeek != null ? factor.physicalActivityDaysPerWeek : 0);
            insertStatement.setInt(9, factor.sleepHoursPerDay != null ? factor.sleepHoursPerDay : 0);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "lifestyle_factors" table contains the same number of records as the lifestyleFactors set
        Logger.logMessage("Checking if the \"lifestyle_factors\" table contains the same number of records as the lifestyleFactors set... ");
        String countSql = "SELECT COUNT(*) FROM lifestyle_factors;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == lifestyleFactors.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + lifestyleFactors.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all lifestyle factor entries from the set are in the "lifestyle_factors" table
        Logger.logMessage("Checking if all lifestyle factor entries from the set are in the \"lifestyle_factors\" table... ");
        String selectSql = "SELECT patient_id FROM lifestyle_factors;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbLifestyleFactorPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbLifestyleFactorPatientIds.add(selectResult.getString("patient_id"));
        }

        // Getting the patient IDs from the 'lifestyleFactors' set
        Set<String> lifestyleFactorPatientIds = lifestyleFactors.stream().map(lf -> lf.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(lifestyleFactorPatientIds);
        missingPatientIds.removeAll(dbLifestyleFactorPatientIds);

        if (missingPatientIds.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing lifestyle factor entries in the table for patient IDs: " + missingPatientIds, true);
        }
        selectStatement.close();

        // ToDo: as (in theory) there's 1-1 relationship between "patients" and "lifestyle_factors" tables, try checking that it is a 1-1 for real.

    }

    private static void processMedicalHistory(Connection connection, String csvFilePath) throws Exception {

        // Reading medical history data from CSV
        Set<MedicalHistoryInfo> medicalHistories = readMedicalHistoryFromCSV(csvFilePath);

        // Clearing the "medical_history" table
        Logger.logMessage("Clearing the \"medical_history\" table... ");
        String truncateSql = "TRUNCATE TABLE medical_history CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting medical history information into the "medical_history" table
        Logger.logMessage("Inserting medical history information into the \"medical_history\" table... ");
        String insertSql = "INSERT INTO medical_history (patient_id, diabetes, family_history, previous_heart_problems) VALUES (?, ?, ?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (MedicalHistoryInfo history : medicalHistories) {
            insertStatement.setString(1, history.patientId);
            boolean diabetesValue = history.diabetes != null ? history.diabetes : false;
            boolean familyHistoryValue = history.familyHistory != null ? history.familyHistory : false;
            boolean previousHeartProblemsValue = history.previousHeartProblems != null ? history.previousHeartProblems : false;

            insertStatement.setBoolean(2, diabetesValue);
            insertStatement.setBoolean(3, familyHistoryValue);
            insertStatement.setBoolean(4, previousHeartProblemsValue);

            insertStatement.executeUpdate();
        }


        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "medical_history" table contains the same number of records as the medicalHistories set
        Logger.logMessage("Checking if the \"medical_history\" table contains the same number of records as the medicalHistories set... ");
        String countSql = "SELECT COUNT(*) FROM medical_history;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == medicalHistories.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + medicalHistories.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all medical history entries from the set are in the "medical_history" table
        Logger.logMessage("Checking if all medical history entries from the set are in the \"medical_history\" table... ");
        String selectSql = "SELECT patient_id FROM medical_history;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbMedicalHistoryPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbMedicalHistoryPatientIds.add(selectResult.getString("patient_id"));
        }

        // Getting the patient IDs from the 'medicalHistories' set
        Set<String> medicalHistoryPatientIds = medicalHistories.stream().map(mh -> mh.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(medicalHistoryPatientIds);
        missingPatientIds.removeAll(dbMedicalHistoryPatientIds);

        if (missingPatientIds.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing medical history entries in the table for patient IDs: " + missingPatientIds, true);
        }
        selectStatement.close();

        // ToDo: as (in theory) there's 1-1 relationship between "patients" and "medical_history" tables, try checking that it is a 1-1 for real.
    }

    private static void processMedication(Connection connection, String csvFilePath) throws Exception {
        // Reading medication data from CSV
        Set<MedicationInfo> medications = readMedicationsFromCSV(csvFilePath);

        // Clearing the "medication" table
        Logger.logMessage("Clearing the \"medication\" table... ");
        String truncateSql = "TRUNCATE TABLE medication CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting medication information into the "medication" table
        Logger.logMessage("Inserting medication information into the \"medication\" table... ");
        String insertSql = "INSERT INTO medication (name) VALUES (?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (MedicationInfo medication : medications) {
            insertStatement.setString(1, medication.name);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "medication" table contains the same number of records as the medications set
        Logger.logMessage("Checking if the \"medication\" table contains the same number of records as the medications set... ");
        String countSql = "SELECT COUNT(*) FROM medication;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == medications.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + medications.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all medication entries from the set are in the "medication" table
        Logger.logMessage("Checking if all medication entries from the set are in the \"medication\" table... ");
        String selectSql = "SELECT name FROM medication;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbMedicationNames = new HashSet<>();
        while (selectResult.next()) {
            dbMedicationNames.add(selectResult.getString("name"));
        }

        // Getting the names of medications from the medications set
        Set<String> medicationNames = medications.stream().map(m -> m.name).collect(Collectors.toSet());

        // Finding the missing medications
        Set<String> missingMedications = new HashSet<>(medicationNames);
        missingMedications.removeAll(dbMedicationNames);

        if (missingMedications.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing medications in the table: " + missingMedications, true);
        }
        selectStatement.close();


        // Selecting a random medication from the table
        Logger.logMessage("Selecting a random medication... ");
        String randomSelectSql = "SELECT name FROM medication ORDER BY RANDOM() LIMIT 1;";
        PreparedStatement randomSelectStatement = connection.prepareStatement(randomSelectSql);
        ResultSet randomSelectResult = randomSelectStatement.executeQuery();
        String randomMedicationName = null;
        if (randomSelectResult.next()) {
            randomMedicationName = randomSelectResult.getString("name");
        }
        randomSelectStatement.close();
        Logger.logMessage("Selected: " + randomMedicationName, true);

        // Attempting to insert a new medication with the same name
        Logger.logMessage("Attempting to insert a duplicate medication... ");
        String insertDuplicateSql = "INSERT INTO medication (name) VALUES (?) RETURNING medication_id;";
        PreparedStatement insertDuplicateStatement = connection.prepareStatement(insertDuplicateSql);
        try {
            insertDuplicateStatement.setString(1, randomMedicationName);
            ResultSet insertedRow = insertDuplicateStatement.executeQuery();
            if (insertedRow.next()) {
                int newMedicationId = insertedRow.getInt("medication_id");
                Logger.logError("FAILED! Duplicate insertion succeeded unexpectedly. Inserted medication_id: " + newMedicationId, true);

                // Delete the newly inserted medication based on medication_id
                String deleteSql = "DELETE FROM medication WHERE medication_id = ?;";
                PreparedStatement deleteStatement = connection.prepareStatement(deleteSql);
                deleteStatement.setInt(1, newMedicationId);
                deleteStatement.execute();
                deleteStatement.close();
            }
        } catch (SQLException e) {
            Logger.logSuccess("Passed. Duplicate insertion failed as expected.", true);
        } finally {
            insertStatement.close();
        }

    }

    private static void processStressLevels(Connection connection, String csvFilePath) throws Exception {

        // Reading stress level data from CSV
        Set<StressLevelInfo> stressLevels = readStressLevelsFromCSV(csvFilePath);

        // Clearing the "stress_levels" table
        Logger.logMessage("Clearing the \"stress_levels\" table... ");
        String truncateSql = "TRUNCATE TABLE stress_levels CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting stress level information into the "stress_levels" table
        Logger.logMessage("Inserting stress level information into the \"stress_levels\" table... ");
        String insertSql = "INSERT INTO stress_levels (patient_id, stress_level) VALUES (?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (StressLevelInfo level : stressLevels) {
            insertStatement.setString(1, level.patientId);
            insertStatement.setInt(2, level.stressLevel != null ? level.stressLevel : 0);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "stress_levels" table contains the same number of records as the stressLevels set
        Logger.logMessage("Checking if the \"stress_levels\" table contains the same number of records as the stressLevels set... ");
        String countSql = "SELECT COUNT(*) FROM stress_levels;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == stressLevels.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + stressLevels.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all stress level entries from the set are in the "stress_levels" table
        Logger.logMessage("Checking if all stress level entries from the set are in the \"stress_levels\" table... ");
        String selectSql = "SELECT patient_id FROM stress_levels;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbStressLevelPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbStressLevelPatientIds.add(selectResult.getString("patient_id"));
        }

        // Getting the patient IDs from the 'stressLevels' set
        Set<String> stressLevelPatientIds = stressLevels.stream().map(sl -> sl.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(stressLevelPatientIds);
        missingPatientIds.removeAll(dbStressLevelPatientIds);

        if (missingPatientIds.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing stress level entries in the table for patient IDs: " + missingPatientIds, true);
        }
        selectStatement.close();

        // ToDo: if this should be 1-1 relationship, implement the proper check.
    }

    public static void processHeartAttackRisk(Connection connection, String csvFilePath) throws Exception {
        // Reading heart attack risk data from CSV
        Set<HeartAttackRiskInfo> heartAttackRiskSet = readHeartAttackRiskFromCSV(csvFilePath);

        // Clearing the "heart_attack_risk" table
        Logger.logMessage("Clearing the \"heart_attack_risk\" table... ");
        String truncateSql = "TRUNCATE TABLE heart_attack_risk CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Inserting heart attack risk information into the "heart_attack_risk" table
        Logger.logMessage("Inserting heart attack risk information into the \"heart_attack_risk\" table... ");
        String insertSql = "INSERT INTO heart_attack_risk (patient_id, risk_level) VALUES (?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (HeartAttackRiskInfo heartAttackRisk : heartAttackRiskSet) {
            insertStatement.setString(1, heartAttackRisk.patientId);
            insertStatement.setBoolean(2, heartAttackRisk.riskLevel != null ? heartAttackRisk.riskLevel : false);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // Checking if the "heart_attack_risk" table contains the same number of records as the heartAttackRiskSet
        Logger.logMessage("Checking if the \"heart_attack_risk\" table contains the same number of records as the heartAttackRiskSet... ");
        String countSql = "SELECT COUNT(*) FROM heart_attack_risk;";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == heartAttackRiskSet.size()) {
                Logger.logSuccess("Passed.", true);
            } else {
                Logger.logError("FAILED! Mismatch in the number of records. Expected [" + heartAttackRiskSet.size() + "], got [" + count + "].", true);
            }
        }
        countStatement.close();

        // Checking if all heart attack risk entries from the set are in the "heart_attack_risk" table
        Logger.logMessage("Checking if all heart attack risk entries from the set are in the \"heart_attack_risk\" table... ");
        String selectSql = "SELECT patient_id FROM heart_attack_risk;";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbHeartAttackRiskPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbHeartAttackRiskPatientIds.add(selectResult.getString("patient_id"));
        }

        // Getting the patient IDs from the 'heartAttackRiskSet' set
        Set<String> heartAttackRiskPatientIds = heartAttackRiskSet.stream().map(ha -> ha.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(heartAttackRiskPatientIds);
        missingPatientIds.removeAll(dbHeartAttackRiskPatientIds);

        if (missingPatientIds.isEmpty()) {
            Logger.logSuccess("Passed.", true);
        } else {
            Logger.logError("FAILED! Missing heart attack risk entries in the table for patient IDs: " + missingPatientIds, true);
        }
        selectStatement.close();
    }

    public static void processPatientMedication(Connection connection, String csvFilePath) throws Exception {
        // Reading patient medication data from CSV
        Set<PatientMedicationInfo> patientMedicalSet = readPatientMedicalFromCSV(csvFilePath);

        // Clearing the "patient_medication" table
        Logger.logMessage("Clearing the \"patient_medication\" table... ");
        String truncateSql = "TRUNCATE TABLE patient_medication CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        Logger.logSuccess("Done.", true);

        // Getting all patients IDs
        Set<String> patientIDs = new HashSet<>();
        String selectPatientIDsSQL = "SELECT patient_id FROM patients;";
        PreparedStatement selectPatientStatement = connection.prepareStatement(selectPatientIDsSQL);
        ResultSet patientResultSet = selectPatientStatement.executeQuery();
        while (patientResultSet.next()) {
            String patientID = patientResultSet.getString("patient_id");
            patientIDs.add(patientID);
        }
        selectPatientStatement.close();

        // Getting all medications IDs and names
        Map<String, Integer> medicationNameToId = new HashMap<>();
        String selectMedicationsSQL = "SELECT medication_id, name FROM medication;";
        PreparedStatement selectMedicationStatement = connection.prepareStatement(selectMedicationsSQL);
        ResultSet medicationResultSet = selectMedicationStatement.executeQuery();
        while (medicationResultSet.next()) {
            int medicationID = medicationResultSet.getInt("medication_id");
            String medicationName = medicationResultSet.getString("name");
            medicationNameToId.put(medicationName, medicationID);
        }
        selectMedicationStatement.close();

        // Inserting patient medication information into the "patient_medication" table
        Logger.logMessage("Inserting patient medication information into the \"patient_medication\" table... ");
        String insertSql = "INSERT INTO patient_medication (patient_id, medication_id) VALUES (?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (PatientMedicationInfo patientMedical : patientMedicalSet) {
            if (!patientIDs.contains(patientMedical.patientId)) {
                Logger.logError("The patient ID: " + patientMedical.patientId + " does not exist in the 'patients' table.", true);
                continue;
            }

            if (!medicationNameToId.containsKey(patientMedical.medication)) {
                Logger.logError("The medication name: " + patientMedical.medication + " does not exist in the 'medication' table.", true);
                continue;
            }

            insertStatement.setString(1, patientMedical.patientId);
            insertStatement.setInt(2, medicationNameToId.get(patientMedical.medication));
            insertStatement.executeUpdate();
        }

        insertStatement.close();
        Logger.logSuccess("Done.", true);

        // ToDo: implement all relevant checks here.

    }

    public static void compareAllTheFinalData(Connection connection, String csvFilePath) throws Exception {

        // SQL Query
        String sql = "WITH \n" +
                "patient_medication_names AS (\n" +
                "  SELECT \n" +
                "    patient_id, \n" +
                "    ARRAY_AGG(name ORDER BY name) AS medication_names\n" +
                "  FROM patient_medication \n" +
                "  JOIN medication ON patient_medication.medication_id = medication.medication_id\n" +
                "  GROUP BY patient_id\n" +
                "),\n" +
                "\n" +
                "data_query AS (\n" +
                "  SELECT \n" +
                "    patients.patient_id AS \"Patient ID\",\n" +
                "    (EXTRACT(YEAR FROM age(CURRENT_DATE, DATE(patients.year_of_birth||'-01-01')))) AS \"Age\",\n" +
                "    patients.sex AS \"Sex\",\n" +
                "    health_metrics.cholesterol AS \"Cholesterol\",\n" +
                "    health_metrics.blood_pressure_systolic || '/' || health_metrics.blood_pressure_diastolic AS \"Blood Pressure\",\n" +
                "    health_metrics.heart_rate AS \"Heart Rate\",\n" +
                "    CASE WHEN medical_history.diabetes THEN 1 ELSE 0 END AS \"Diabetes\",\n" +
                "    CASE WHEN medical_history.family_history THEN 1 ELSE 0 END AS \"Family History\",\n" +
                "\tCASE WHEN lifestyle_factors.smoking THEN 1 ELSE 0 END AS \"Smoking\",\n" +
                "    CASE WHEN lifestyle_factors.obesity THEN 1 ELSE 0 END AS \"Obesity\",\n" +
                "    CASE WHEN lifestyle_factors.alcohol_consumption THEN 1 ELSE 0 END AS \"Alcohol Consumption\",\n" +
                "    ROUND(lifestyle_factors.exercise_hours_per_week::numeric, 2) AS \"Exercise Hours Per Week\",\n" +
                "    diets.name AS \"Diet\",\n" +
                "    CASE WHEN medical_history.previous_heart_problems THEN 1 ELSE 0 END AS \"Previous Heart Problems\",  \n" +
                "    (patient_medication_names.medication_names)[1] AS \"Medication Use\",\n" +
                "    stress_levels.stress_level AS \"Stress Level\",\n" +
                "    ROUND(lifestyle_factors.sedentary_hours_per_day::numeric, 2) AS \"Sedentary Hours Per Day\",\n" +
                "    patients.income AS \"Income\",\n" +
                "    ROUND(health_metrics.bmi::numeric, 2) AS \"BMI\",\n" +
                "    health_metrics.triglycerides AS \"Triglycerides\",\n" +
                "    lifestyle_factors.physical_activity_days_per_week AS \"Physical Activity Days Per Week\",\n" +
                "    ROUND(lifestyle_factors.sleep_hours_per_day::numeric, 2) AS \"Sleep Hours Per Day\",\n" +
                "    countries.name AS \"Country\",\n" +
                "    continents.name AS \"Continent\",\n" +
                "    continents.hemisphere || ' Hemisphere' AS \"Hemisphere\",\n" +
                "    CASE WHEN heart_attack_risk.risk_level THEN 1 ELSE 0 END AS \"Heart Attack Risk\"\n" +
                "  FROM patients\n" +
                "    JOIN countries ON patients.country_id = countries.country_id\n" +
                "    JOIN continents ON countries.continent_id = continents.continent_id\n" +
                "    JOIN health_metrics ON patients.patient_id = health_metrics.patient_id\n" +
                "    JOIN lifestyle_factors ON patients.patient_id = lifestyle_factors.patient_id\n" +
                "    JOIN diets ON lifestyle_factors.diet_id = diets.diet_id\n" +
                "    JOIN medical_history ON patients.patient_id = medical_history.patient_id\n" +
                "    JOIN stress_levels ON patients.patient_id = stress_levels.patient_id\n" +
                "    JOIN heart_attack_risk ON patients.patient_id = heart_attack_risk.patient_id\n" +
                "    LEFT JOIN patient_medication_names ON patients.patient_id = patient_medication_names.patient_id\n" +
                ")\n" +
                "SELECT * FROM data_query;\n";

        // Add or modify these sets based on your actual CSV and SQL column names
        Set<String> BOOLEAN_FIELDS = Set.of("Diabetes", "Family History", "Smoking", "Obesity", "Alcohol Consumption", "Previous Heart Problems", "Medication Use", "Heart Attack Risk");
        Set<String> INTEGER_FIELDS = Set.of("Stress Level", "Income", "Triglycerides", "Physical Activity Days Per Week", "Sleep Hours Per Day");
        Set<String> DECIMAL_FIELDS = Set.of("Exercise Hours Per Week", "Sedentary Hours Per Day", "BMI");

        // Execute query and store result in a Set
        Set<String> dbData = new HashSet<>();
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                StringBuilder rowData = new StringBuilder();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);

                    if (BOOLEAN_FIELDS.contains(columnName)) {
                        rowData.append(rs.getBoolean(i)).append(",");
                    } else if (INTEGER_FIELDS.contains(columnName)) {
                        rowData.append(rs.getInt(i)).append(",");
                    } else if (DECIMAL_FIELDS.contains(columnName)) {
                        rowData.append(formatDecimalField(String.valueOf(rs.getDouble(i)))).append(",");
                    } else {
                        rowData.append(rs.getString(i)).append(",");
                    }
                }
                dbData.add(rowData.toString());
            }
        }

        // Read CSV File and store data in a Set
        Set<String> csvData = new HashSet<>();
        try (Reader in = new FileReader(csvFilePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                StringBuilder rowData = new StringBuilder();
                for (String columnName : record.toMap().keySet()) {
                    String field = record.get(columnName);

                    if (BOOLEAN_FIELDS.contains(columnName)) {
                        rowData.append(parseBooleanValue(field)).append(",");
                    } else if (INTEGER_FIELDS.contains(columnName)) {
                        rowData.append(Integer.parseInt(field)).append(",");
                    } else if (DECIMAL_FIELDS.contains(columnName)) {
                        rowData.append(formatDecimalField(field)).append(",");
                    } else {
                        rowData.append(field).append(",");
                    }
                }
                csvData.add(rowData.toString());
            }
        }


        // Compare data size
        if (dbData.size() != csvData.size()) {
            Logger.logError("Data size mismatch! Database has " + dbData.size() + " rows, CSV has " + csvData.size() + " rows.", true);
        } else {
            Logger.logSuccess("Data size matches.", true);
        }

        // Compare data row by row
        boolean mismatchFound = false;
        for (String csvRow : csvData) {
            if (!dbData.contains(csvRow)) {
                Logger.logError("Mismatch found! Row not in database: " + csvRow, true);
                mismatchFound = true;
            }
        }
        for (String dbRow : dbData) {
            if (!csvData.contains(dbRow)) {
                Logger.logError("Mismatch found! Row not in CSV: " + dbRow, true);
                mismatchFound = true;
            }
        }

        if (!mismatchFound) {
            Logger.logSuccess("All data matches!", true);
        }

        // ToDo: refine the code to detect exact mismatches.

    }

    private static Set<ContinentInfo> readContinentsFromCSV(String filePath) throws Exception {
        Set<ContinentInfo> continents = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String name = record.get("Continent");
            String hemisphereRaw = record.get("Hemisphere");

            String hemisphere = "Unknown";
            if (hemisphereRaw.contains("Northern")) {
                hemisphere = "Northern";
            } else if (hemisphereRaw.contains("Southern")) {
                hemisphere = "Southern";
            }

            if (hemisphere.equals("Unknown")) {
                Logger.logError("Warning! Unknown hemisphere name \"" + hemisphereRaw + "\" detected!", true);
            }

            continents.add(new ContinentInfo(name, hemisphere));
        }
        return continents;
    }

    private static Set<CountryInfo> readCountriesFromCSV(String filePath) throws Exception {
        Set<CountryInfo> countries = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String name = record.get("Country");
            String continent = record.get("Continent");
            String hemisphereRaw = record.get("Hemisphere");

            String hemisphere = "Unknown";
            if (hemisphereRaw.contains("Northern")) {
                hemisphere = "Northern";
            } else if (hemisphereRaw.contains("Southern")) {
                hemisphere = "Southern";
            }

            if (hemisphere.equals("Unknown")) {
                Logger.logError("Warning! Unknown hemisphere name \"" + hemisphereRaw + "\" detected!", true);
            }

            countries.add(new CountryInfo(name, continent, hemisphere));
        }

        return countries;
    }

    private static Set<PatientInfo> readPatientsFromCSV(String filePath) throws Exception {
        Set<PatientInfo> patients = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
        PatientInfo patientInfo;
        int currentYear = Year.now().getValue();

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            int age = Integer.parseInt(record.get("Age"));
            int yearOfBirth = currentYear - age;
            String sex = record.get("Sex");
            int income = Integer.parseInt(record.get("Income"));
            String country = record.get("Country");

            patientInfo = new PatientInfo(patientId, yearOfBirth, sex, income, country);

            if (patients.contains(patientInfo)) {
                Logger.logError("Warning: A patient with ID " + patientId + " already exists.", true);
            } else {
                patients.add(patientInfo);
            }

        }

        return patients;
    }

    private static Set<HealthMetricInfo> readHealthMetricsFromCSV(String filePath) throws Exception {
        Set<HealthMetricInfo> healthMetrics = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            Integer cholesterol = record.get("Cholesterol") != null ? Integer.parseInt(record.get("Cholesterol")) : null;

            String bloodPressure = record.get("Blood Pressure");
            Integer bloodPressureSystolic = null;
            Integer bloodPressureDiastolic = null;

            if (bloodPressure != null && !bloodPressure.isEmpty()) {
                String[] parts = bloodPressure.split("/");
                if (parts.length == 2) {
                    try {
                        bloodPressureSystolic = Integer.parseInt(parts[0].trim());
                        bloodPressureDiastolic = Integer.parseInt(parts[1].trim());
                    } catch (NumberFormatException e) {
                        Logger.logError("Invalid blood pressure format: " + bloodPressure, true);
                    }
                } else {
                    Logger.logError("Invalid blood pressure format: " + bloodPressure, true);
                }
            }

            Integer heartRate = record.get("Heart Rate") != null ? Integer.parseInt(record.get("Heart Rate")) : null;
            Integer triglycerides = record.get("Triglycerides") != null ? Integer.parseInt(record.get("Triglycerides")) : null;
            Float bmi = record.get("BMI") != null ? Float.parseFloat(record.get("BMI")) : null;

            HealthMetricInfo healthMetricInfo = new HealthMetricInfo(patientId, cholesterol, bloodPressureSystolic, bloodPressureDiastolic, heartRate, triglycerides, bmi);

            if (healthMetrics.contains(healthMetricInfo)) {
                Logger.logError("Warning: A health metric with ID " + patientId + " already exists.", true);
            } else {
                healthMetrics.add(healthMetricInfo);
            }
        }

        return healthMetrics;
    }

    private static Set<DietInfo> readDietsFromCSV(String filePath) throws Exception {
        Set<DietInfo> diets = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String name = record.get("Diet");
            diets.add(new DietInfo(name));
        }

        return diets;
    }

    private static Set<LifestyleFactorInfo> readLifestyleFactorsFromCSV(String filePath) throws Exception {
        Set<LifestyleFactorInfo> lifestyleFactors = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            String diet = record.get("Diet");
            Boolean smoking = parseBooleanValue(record.get("Smoking"));
            Boolean alcoholConsumption = parseBooleanValue(record.get("Alcohol Consumption"));
            Boolean obesity = parseBooleanValue(record.get("Obesity"));
            Float exerciseHoursPerWeek = record.get("Exercise Hours Per Week") != null ? Float.parseFloat(record.get("Exercise Hours Per Week")) : null;
            Float sedentaryHoursPerDay = record.get("Sedentary Hours Per Day") != null ? Float.parseFloat(record.get("Sedentary Hours Per Day")) : null;
            Integer physicalActivityDaysPerWeek = record.get("Physical Activity Days Per Week") != null ? Integer.parseInt(record.get("Physical Activity Days Per Week")) : null;
            Integer sleepHoursPerDay = record.get("Sleep Hours Per Day") != null ? Integer.parseInt(record.get("Sleep Hours Per Day")) : null;

            LifestyleFactorInfo lifestyleFactorInfo = new LifestyleFactorInfo(patientId, diet, smoking, alcoholConsumption,
                    obesity, exerciseHoursPerWeek, sedentaryHoursPerDay,
                    physicalActivityDaysPerWeek, sleepHoursPerDay);

            lifestyleFactors.add(lifestyleFactorInfo);
        }

        return lifestyleFactors;
    }

    private static Set<MedicalHistoryInfo> readMedicalHistoryFromCSV(String filePath) throws Exception {
        Set<MedicalHistoryInfo> medicalHistories = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            Boolean diabetes = parseBooleanValue(record.get("Diabetes"));
            Boolean familyHistory = parseBooleanValue(record.get("Family History"));
            Boolean previousHeartProblems = parseBooleanValue(record.get("Previous Heart Problems"));

            MedicalHistoryInfo medicalHistoryInfo = new MedicalHistoryInfo(patientId, diabetes, familyHistory, previousHeartProblems);

            medicalHistories.add(medicalHistoryInfo);
        }


        return medicalHistories;
    }

    private static Set<MedicationInfo> readMedicationsFromCSV(String filePath) throws Exception {
        Set<MedicationInfo> medications = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String name = record.get("Medication Use");
            medications.add(new MedicationInfo(name));
        }

        return medications;
    }

    private static Set<StressLevelInfo> readStressLevelsFromCSV(String filePath) throws Exception {
        Set<StressLevelInfo> stressLevels = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            Integer stressLevel = record.get("Stress Level") != null ? Integer.parseInt(record.get("Stress Level")) : null;

            StressLevelInfo stressLevelInfo = new StressLevelInfo(patientId, stressLevel);

            stressLevels.add(stressLevelInfo);
        }

        return stressLevels;
    }

    public static Set<HeartAttackRiskInfo> readHeartAttackRiskFromCSV(String filePath) throws Exception {
        Set<HeartAttackRiskInfo> heartAttackRiskSet = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            Boolean riskLevel = parseBooleanValue(record.get("Heart Attack Risk"));

            HeartAttackRiskInfo heartAttackRiskInfo = new HeartAttackRiskInfo(patientId, riskLevel);
            heartAttackRiskSet.add(heartAttackRiskInfo);
        }

        return heartAttackRiskSet;
    }

    public static Set<PatientMedicationInfo> readPatientMedicalFromCSV(String filePath) throws Exception {
        Set<PatientMedicationInfo> patientMedicalSet = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            String medication = record.get("Medication Use");

            PatientMedicationInfo patientMedicalInfo = new PatientMedicationInfo(patientId, medication);
            patientMedicalSet.add(patientMedicalInfo);
        }

        return patientMedicalSet;
    }

    // Helper method to parse boolean values
    private static Boolean parseBooleanValue(String value) {
        if (value == null) {
            return null;
        }
        String lowerValue = value.toLowerCase();
        return lowerValue.equals("true") || lowerValue.equals("1") || lowerValue.equals("yes");
    }

    private static String formatDecimalField(String field) {
        BigDecimal value = new BigDecimal(field).setScale(2, BigDecimal.ROUND_HALF_UP);
        return value.toString();
    }

}