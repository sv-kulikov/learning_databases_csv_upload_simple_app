-- Creating the "Countries" table
CREATE TABLE "Countries" (
    "CountryID" SERIAL PRIMARY KEY,
    "Name" VARCHAR(255) NOT NULL,
    "Continent" VARCHAR(255),
    "Hemisphere" VARCHAR(255)
);

-- Creating the "Patients" table
CREATE TABLE "Patients" (
    "PatientID" VARCHAR(255) PRIMARY KEY,
    "Age" INT,
    "Sex" VARCHAR(50),
    "BMI" FLOAT,
    "Income" INT,
    "CountryID" INT,
    FOREIGN KEY ("CountryID") REFERENCES "Countries"("CountryID")
);

-- Creating the "HealthMetrics" table
CREATE TABLE "HealthMetrics" (
    "HealthMetricID" SERIAL PRIMARY KEY,
    "PatientID" VARCHAR(255),
    "Cholesterol" INT,
    "BloodPressureSystolic" INT,
    "BloodPressureDiastolic" INT,
    "HeartRate" INT,
    "Triglycerides" INT,
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID")
);

-- Creating the "LifestyleFactors" table
CREATE TABLE "LifestyleFactors" (
    "LifestyleID" SERIAL PRIMARY KEY,
    "PatientID" VARCHAR(255),
    "Smoking" BOOLEAN,
    "AlcoholConsumption" BOOLEAN,
    "ExerciseHoursPerWeek" FLOAT,
    "Diet" VARCHAR(255),
    "SedentaryHoursPerDay" FLOAT,
    "PhysicalActivityDaysPerWeek" INT,
    "SleepHoursPerDay" INT,
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID")
);

-- Creating the "MedicalHistory" table
CREATE TABLE "MedicalHistory" (
    "MedicalHistoryID" SERIAL PRIMARY KEY,
    "PatientID" VARCHAR(255),
    "Diabetes" BOOLEAN,
    "FamilyHistory" BOOLEAN,
    "PreviousHeartProblems" BOOLEAN,
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID")
);

-- Creating the "Medication" table
CREATE TABLE "Medication" (
    "MedicationID" SERIAL PRIMARY KEY,
    "Name" VARCHAR(255) NOT NULL
);

-- Creating the "PatientMedication" table (Many-to-Many relationship)
CREATE TABLE "PatientMedication" (
    "PatientID" VARCHAR(255),
    "MedicationID" INT,
    PRIMARY KEY ("PatientID", "MedicationID"),
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID"),
    FOREIGN KEY ("MedicationID") REFERENCES "Medication"("MedicationID")
);

-- Creating the "StressLevels" table
CREATE TABLE "StressLevels" (
    "StressLevelID" SERIAL PRIMARY KEY,
    "PatientID" VARCHAR(255),
    "StressLevel" INT,
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID")
);

-- Creating the "HeartAttackRisk" table
CREATE TABLE "HeartAttackRisk" (
    "RiskID" SERIAL PRIMARY KEY,
    "PatientID" VARCHAR(255),
    "RiskLevel" BOOLEAN,
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID")
);

-- Creating the "IncomeLevels" table
CREATE TABLE "IncomeLevels" (
    "IncomeLevelID" SERIAL PRIMARY KEY,
    "IncomeRange" VARCHAR(255)
);

-- Creating the "PatientIncome" table (Many-to-Many relationship)
CREATE TABLE "PatientIncome" (
    "PatientID" VARCHAR(255),
    "IncomeLevelID" INT,
    PRIMARY KEY ("PatientID", "IncomeLevelID"),
    FOREIGN KEY ("PatientID") REFERENCES "Patients"("PatientID"),
    FOREIGN KEY ("IncomeLevelID") REFERENCES "IncomeLevels"("IncomeLevelID")
);
