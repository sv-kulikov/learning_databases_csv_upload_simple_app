package biz.svyatoslav.learning.databases.postgresql.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CsvToPostgreSQL {

    // 0) See the CSV data, database model and SQL in "data" folder of this project.

    // 1) On your VM enable inbound connections in firewall.
    // 2) To "C:/Program Files/PostgreSQL/15/data/pg_hba.conf" add this line (set your own host OS ip!):
    //    host    all    		all    		192.168.1.1/32    	md5
    // 3) Restart PostgreSQL service.

    // 4) To check results open pgAdmin and execute the next queries:
    //    SELECT * FROM "Countries" ORDER BY "Name";
    //    SELECT * FROM "Patients" ORDER BY "Age";
    //    SELECT *
    //     FROM "Patients"
    //     LEFT JOIN "Countries" ON "Patients"."CountryID" = "Countries"."CountryID"
    //     ORDER BY "Patients"."Age", "Countries"."Name";

    static class CountryInfo {
        String name;
        String continent;
        String hemisphere;

        CountryInfo(String name, String continent, String hemisphere) {
            this.name = name;
            this.continent = continent;
            this.hemisphere = hemisphere;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            CountryInfo that = (CountryInfo) obj;
            return name.equals(that.name);
        }
    }

    static class PatientInfo {
        String patientId;
        int age;
        String sex;
        float bmi;
        int income;
        String country;

        PatientInfo(String patientId, int age, String sex, float bmi, int income, String country) {
            this.patientId = patientId;
            this.age = age;
            this.sex = sex;
            this.bmi = bmi;
            this.income = income;
            this.country = country;
        }

        @Override
        public int hashCode() {
            return patientId.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            PatientInfo that = (PatientInfo) obj;
            return patientId.equals(that.patientId);
        }
    }

    public static void main(String[] args) {
        String jdbcURL = "jdbc:postgresql://192.168.1.102:5432/course";
        String username = "postgres";
        String password = "123456";
        String csvFilePath = "data/heart_attack_prediction_dataset.csv";

        try {
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);

            System.out.println();
            System.out.println("Processing COUNTRIES:");
            processCountries(connection, csvFilePath);
            System.out.println("DONE processing COUNTRIES.");

            System.out.println();
            System.out.println("Processing PATIENTS:");
            processPatients(connection, csvFilePath);
            System.out.println("DONE processing PATIENTS.");

            connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static void processCountries(Connection connection, String csvFilePath) throws Exception {
        // Reading countries from CSV
        Set<CountryInfo> countries = readCountriesFromCSV(csvFilePath);

        // Clearing the Countries table
        System.out.print("Clearing the \"Countries\" table... ");
        String truncateSql = "TRUNCATE TABLE \"Countries\" CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        System.out.println("Done.");

        // Inserting countries information into the Countries table
        System.out.print("Inserting countries information into the \"Countries\" table... ");
        String sql = "INSERT INTO \"Countries\" (\"Name\", \"Continent\", \"Hemisphere\") VALUES (?, ?, ?);";

        // If you've set Unique constraint on "Name" field, you can use:
        // String sql = "INSERT INTO \"Countries\" (\"Name\", \"Continent\", \"Hemisphere\") VALUES (?, ?, ?) ON CONFLICT (\"Name\") DO NOTHING;";

        PreparedStatement statement = connection.prepareStatement(sql);

        for (CountryInfo country : countries) {
            statement.setString(1, country.name);
            statement.setString(2, country.continent);
            statement.setString(3, country.hemisphere);
            statement.executeUpdate();
        }
        statement.close();
        System.out.println("Done.");

        // Checking if the Countries table contains the same number of records as the countries set
        System.out.print("Checking if the \"Countries\" table contains the same number of records as the countries set... ");
        String countSql = "SELECT COUNT(*) FROM \"Countries\";";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == countries.size()) {
                System.out.println("Passed.");
            } else {
                System.out.println("FAILED! Mismatch in the number of records. Expected [" + countries.size() + "], got [ " + count + "].");
            }
        }
        countStatement.close();

        // Checking if all countries from the set are in the Countries table
        System.out.print("Checking if all countries from the set are in the \"Countries\" table... ");
        String selectSql = "SELECT \"Name\" FROM \"Countries\";";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbCountries = new HashSet<>();
        while (selectResult.next()) {
            dbCountries.add(selectResult.getString("Name"));
        }

        // Getting the names of countries from the 'countries' set
        Set<String> countryNames = countries.stream().map(c -> c.name).collect(Collectors.toSet());

        // Finding the missing countries
        Set<String> missingCountries = new HashSet<>(countryNames);
        missingCountries.removeAll(dbCountries);

        if (missingCountries.isEmpty()) {
            System.out.println("Passed.");
        } else {
            System.out.println("FAILED! Missing countries in the table: " + missingCountries);
        }
        selectStatement.close();
    }

    private static void processPatients(Connection connection, String csvFilePath) throws Exception {

        // Reading patients data from CSV
        Set<PatientInfo> patients = readPatientsFromCSV(csvFilePath);

        // Getting CountryID for each country name
        System.out.print("Reading \"Countries\" data... ");
        Map<String, Integer> countryIds = new HashMap<>();
        String countrySql = "SELECT \"CountryID\", \"Name\" FROM \"Countries\";";
        PreparedStatement countryStatement = connection.prepareStatement(countrySql);
        ResultSet countryResultSet = countryStatement.executeQuery();
        while (countryResultSet.next()) {
            countryIds.put(countryResultSet.getString("Name"), countryResultSet.getInt("CountryID"));
        }
        countryStatement.close();
        System.out.println("Done.");

        // Clearing the Patients table
        System.out.print("Clearing the \"Patients\" table... ");
        String truncateSql = "TRUNCATE TABLE \"Patients\" CASCADE;";
        PreparedStatement truncateStatement = connection.prepareStatement(truncateSql);
        truncateStatement.execute();
        truncateStatement.close();
        System.out.println("Done.");

        // Inserting patient information into the Patients table
        System.out.print("Inserting patient information into the \"Patients\" table... ");
        String insertSql = "INSERT INTO \"Patients\" (\"PatientID\", \"Age\", \"Sex\", \"BMI\", \"Income\", \"CountryID\") VALUES (?, ?, ?, ?, ?, ?);";

        PreparedStatement insertStatement = connection.prepareStatement(insertSql);

        for (PatientInfo patient : patients) {
            Integer countryId = countryIds.get(patient.country);
            if (countryId == null) {
                System.out.println("Warning: No proper country found for " + patient.country);
                continue; // Skip this patient
            }

            insertStatement.setString(1, patient.patientId);
            insertStatement.setInt(2, patient.age);
            insertStatement.setString(3, patient.sex);
            insertStatement.setFloat(4, patient.bmi);
            insertStatement.setInt(5, patient.income);
            insertStatement.setInt(6, countryId);
            insertStatement.executeUpdate();
        }
        insertStatement.close();
        System.out.println("Done.");

        // Checking if the Patients table contains the same number of records as the patients set
        System.out.print("Checking if the \"Patients\" table contains the same number of records as the patients set... ");
        String countSql = "SELECT COUNT(*) FROM \"Patients\";";
        PreparedStatement countStatement = connection.prepareStatement(countSql);
        ResultSet countResult = countStatement.executeQuery();
        if (countResult.next()) {
            int count = countResult.getInt(1);
            if (count == patients.size()) {
                System.out.println("Passed.");
            } else {
                System.out.println("FAILED! Mismatch in the number of records. Expected [" + patients.size() + "], got [ " + count + "].");
            }
        }
        countStatement.close();

        // Checking if all patient IDs from the set are in the Patients table
        System.out.print("Checking if all patient IDs from the set are in the \"Patients\" table... ");
        String selectSql = "SELECT \"PatientID\" FROM \"Patients\";";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet selectResult = selectStatement.executeQuery();
        Set<String> dbPatientIds = new HashSet<>();
        while (selectResult.next()) {
            dbPatientIds.add(selectResult.getString("PatientID"));
        }

        // Getting the patient IDs from the 'patients' set
        Set<String> patientIds = patients.stream().map(p -> p.patientId).collect(Collectors.toSet());

        // Finding the missing patient IDs
        Set<String> missingPatientIds = new HashSet<>(patientIds);
        missingPatientIds.removeAll(dbPatientIds);

        if (missingPatientIds.isEmpty()) {
            System.out.println("Passed.");
        } else {
            System.out.println("FAILED! Missing patient IDs in the table: " + missingPatientIds);
        }
        selectStatement.close();
    }

    private static Set<CountryInfo> readCountriesFromCSV(String filePath) throws Exception {
        Set<CountryInfo> countries = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

        for (CSVRecord record : parser) {
            String name = record.get("Country");
            String continent = record.get("Continent");
            String hemisphere = record.get("Hemisphere");
            countries.add(new CountryInfo(name, continent, hemisphere));
        }

        return countries;
    }

    private static Set<PatientInfo> readPatientsFromCSV(String filePath) throws Exception {
        Set<PatientInfo> patients = new HashSet<>();
        Reader in = new FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
        PatientInfo patientInfo;

        for (CSVRecord record : parser) {
            String patientId = record.get("Patient ID");
            int age = Integer.parseInt(record.get("Age"));
            String sex = record.get("Sex");
            float bmi = Float.parseFloat(record.get("BMI"));
            int income = Integer.parseInt(record.get("Income"));
            String country = record.get("Country");

            patientInfo = new PatientInfo(patientId, age, sex, bmi, income, country);

            if (patients.contains(patientInfo)) {
                System.out.println("Warning: A patient with ID " + patientId + " already exists.");
            } else {
                patients.add(patientInfo);
            }

        }

        return patients;
    }

}