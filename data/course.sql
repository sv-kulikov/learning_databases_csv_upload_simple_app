-- Hemisphere Enumeration
CREATE TYPE hemisphere_enum AS ENUM ('Northern', 'Southern');
COMMENT ON TYPE hemisphere_enum IS 'Enumeration of hemispheres: Northern or Southern';

-- Continents Table
CREATE TABLE continents (
    continent_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    hemisphere hemisphere_enum
);
COMMENT ON TABLE continents IS 'Stores continent information including hemisphere';
COMMENT ON COLUMN continents.continent_id IS 'Primary key for continents';
COMMENT ON COLUMN continents.name IS 'Name of the continent';
COMMENT ON COLUMN continents.hemisphere IS 'Hemisphere of the continent, referencing hemisphere_enum';

-- Countries Table
CREATE TABLE countries (
    country_id SERIAL PRIMARY KEY,
    continent_id INT,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (continent_id) REFERENCES continents(continent_id)
);
COMMENT ON TABLE countries IS 'Stores country information including associated continent';
COMMENT ON COLUMN countries.country_id IS 'Primary key for countries';
COMMENT ON COLUMN countries.continent_id IS 'Foreign key referencing continent_id from continents table';
COMMENT ON COLUMN countries.name IS 'Name of the country';

-- Sex Enumeration
CREATE TYPE sex_enum AS ENUM ('Male', 'Female');
COMMENT ON TYPE sex_enum IS 'Enumeration of sexes: Male or Female';

-- Patients Table
CREATE TABLE patients (
    patient_id VARCHAR(255) PRIMARY KEY,
    year_of_birth INT,
    sex sex_enum,
    income INT,
    country_id INT,
    FOREIGN KEY (country_id) REFERENCES countries(country_id)
);
COMMENT ON TABLE patients IS 'Stores patient information including demographics';
COMMENT ON COLUMN patients.patient_id IS 'Primary key for patients';
COMMENT ON COLUMN patients.year_of_birth IS 'Birth year of the patient';
COMMENT ON COLUMN patients.sex IS 'Sex of the patient, referencing sex_enum';
COMMENT ON COLUMN patients.income IS 'Income of the patient';
COMMENT ON COLUMN patients.country_id IS 'Foreign key referencing country_id from countries table';

-- Health Metrics Table
CREATE TABLE health_metrics (
    health_metric_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(255),
    cholesterol INT,
    blood_pressure_systolic INT,
    blood_pressure_diastolic INT,
    heart_rate INT,
    triglycerides INT,
    bmi FLOAT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);
COMMENT ON TABLE health_metrics IS 'Stores various health metrics for patients';
COMMENT ON COLUMN health_metrics.health_metric_id IS 'Primary key for health metrics';
COMMENT ON COLUMN health_metrics.patient_id IS 'Foreign key referencing patient_id from patients table';
COMMENT ON COLUMN health_metrics.cholesterol IS 'Cholesterol level of the patient';
COMMENT ON COLUMN health_metrics.blood_pressure_systolic IS 'Systolic blood pressure of the patient';
COMMENT ON COLUMN health_metrics.blood_pressure_diastolic IS 'Diastolic blood pressure of the patient';
COMMENT ON COLUMN health_metrics.heart_rate IS 'Heart rate of the patient';
COMMENT ON COLUMN health_metrics.triglycerides IS 'Triglyceride level of the patient';
COMMENT ON COLUMN health_metrics.bmi IS 'Body Mass Index (BMI) of the patient';

-- Diets Table
CREATE TABLE diets (
    diet_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
COMMENT ON TABLE diets IS 'Stores different types of diets';
COMMENT ON COLUMN diets.diet_id IS 'Primary key for diets';
COMMENT ON COLUMN diets.name IS 'Name of the diet';

-- Lifestyle Factors Table
CREATE TABLE lifestyle_factors (
    lifestyle_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(255),
    diet_id INT,
    smoking BOOLEAN,
    alcohol_consumption BOOLEAN,
    obesity BOOLEAN,
    exercise_hours_per_week FLOAT,
    sedentary_hours_per_day FLOAT,
    physical_activity_days_per_week INT,
    sleep_hours_per_day INT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (diet_id) REFERENCES diets(diet_id)
);
COMMENT ON TABLE lifestyle_factors IS 'Stores lifestyle factors for patients';
COMMENT ON COLUMN lifestyle_factors.lifestyle_id IS 'Primary key for lifestyle factors';
COMMENT ON COLUMN lifestyle_factors.patient_id IS 'Foreign key referencing patient_id from patients table';
COMMENT ON COLUMN lifestyle_factors.diet_id IS 'Foreign key referencing diet_id from diets table';
COMMENT ON COLUMN lifestyle_factors.smoking IS 'Indicates if the patient smokes';
COMMENT ON COLUMN lifestyle_factors.alcohol_consumption IS 'Indicates alcohol consumption of the patient';
COMMENT ON COLUMN lifestyle_factors.obesity IS 'Indicates if the patient is obese';
COMMENT ON COLUMN lifestyle_factors.exercise_hours_per_week IS 'Weekly exercise hours of the patient';
COMMENT ON COLUMN lifestyle_factors.sedentary_hours_per_day IS 'Daily sedentary hours of the patient';
COMMENT ON COLUMN lifestyle_factors.physical_activity_days_per_week IS 'Number of days per week the patient engages in physical activity';
COMMENT ON COLUMN lifestyle_factors.sleep_hours_per_day IS 'Average sleep hours per day of the patient';

-- Medical History Table
CREATE TABLE medical_history (
    medical_history_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(255),
    diabetes BOOLEAN,
    family_history BOOLEAN,
    previous_heart_problems BOOLEAN,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);
COMMENT ON TABLE medical_history IS 'Stores medical history of patients';
COMMENT ON COLUMN medical_history.medical_history_id IS 'Primary key for medical history';
COMMENT ON COLUMN medical_history.patient_id IS 'Foreign key referencing patient_id from patients table';
COMMENT ON COLUMN medical_history.diabetes IS 'Indicates if the patient has diabetes';
COMMENT ON COLUMN medical_history.family_history IS 'Indicates if there is a family history of medical conditions';
COMMENT ON COLUMN medical_history.previous_heart_problems IS 'Indicates if the patient had previous heart problems';

-- Medication Table
CREATE TABLE medication (
    medication_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
COMMENT ON TABLE medication IS 'Stores medication information';
COMMENT ON COLUMN medication.medication_id IS 'Primary key for medication';
COMMENT ON COLUMN medication.name IS 'Name of the medication';

-- Patient Medication Table (Many-to-Many relationship)
CREATE TABLE patient_medication (
    patient_id VARCHAR(255),
    medication_id INT,
    PRIMARY KEY (patient_id, medication_id),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (medication_id) REFERENCES medication(medication_id)
);
COMMENT ON TABLE patient_medication IS 'Represents many-to-many relationship between patients and medications';
COMMENT ON COLUMN patient_medication.patient_id IS 'Foreign key referencing patient_id from patients table';
COMMENT ON COLUMN patient_medication.medication_id IS 'Foreign key referencing medication_id from medication table';

-- Stress Levels Table
CREATE TABLE stress_levels (
    stress_level_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(255),
    stress_level INT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);
COMMENT ON TABLE stress_levels IS 'Stores stress levels of patients';
COMMENT ON COLUMN stress_levels.stress_level_id IS 'Primary key for stress levels';
COMMENT ON COLUMN stress_levels.patient_id IS 'Foreign key referencing patient_id from patients table';
COMMENT ON COLUMN stress_levels.stress_level IS 'Stress level of the patient';

-- Heart Attack Risk Table
CREATE TABLE heart_attack_risk (
    risk_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(255),
    risk_level BOOLEAN,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);
COMMENT ON TABLE heart_attack_risk IS 'Stores heart attack risk assessment for patients';
COMMENT ON COLUMN heart_attack_risk.risk_id IS 'Primary key for heart attack risk';
COMMENT ON COLUMN heart_attack_risk.patient_id IS 'Foreign key referencing patient_id from patients table';
COMMENT ON COLUMN heart_attack_risk.risk_level IS 'Risk level of heart attack for the patient';

-- Indexes for continents table
CREATE INDEX idx_continents_name ON continents(name); 
-- Purpose: Improves performance of queries filtering or joining on continent name.

-- Indexes for countries table
CREATE INDEX idx_countries_name ON countries(name); 
-- Purpose: Improves performance of queries filtering or joining on country name.
CREATE INDEX idx_countries_continent_id ON countries(continent_id); 
-- Purpose: Improves performance of queries joining countries with continents.

-- Indexes for patients table
CREATE INDEX idx_patients_year_of_birth ON patients(year_of_birth); 
-- Purpose: Enhances query performance for age-related queries.
CREATE INDEX idx_patients_sex ON patients(sex); 
-- Purpose: Enhances query performance for gender-based statistics or filters.
CREATE INDEX idx_patients_country_id ON patients(country_id); 
-- Purpose: Improves performance of queries joining patients with countries.

-- Indexes for health_metrics table
CREATE INDEX idx_health_metrics_patient_id ON health_metrics(patient_id); 
-- Purpose: Speeds up queries that join health metrics with patient data.

-- Indexes for lifestyle_factors table
CREATE INDEX idx_lifestyle_factors_patient_id ON lifestyle_factors(patient_id); 
-- Purpose: Speeds up queries joining lifestyle factors with patient data.
CREATE INDEX idx_lifestyle_factors_diet_id ON lifestyle_factors(diet_id); 
-- Purpose: Enhances performance of queries filtering by diet.

-- Indexes for medical_history table
CREATE INDEX idx_medical_history_patient_id ON medical_history(patient_id); 
-- Purpose: Optimizes queries joining medical history with patient data.

-- Indexes for patient_medication table
CREATE INDEX idx_patient_medication_patient_id ON patient_medication(patient_id); 
-- Purpose: Improves performance for queries involving patient medication data.
CREATE INDEX idx_patient_medication_medication_id ON patient_medication(medication_id); 
-- Purpose: Speeds up queries joining patient medication with medication data.

-- Indexes for stress_levels table
CREATE INDEX idx_stress_levels_patient_id ON stress_levels(patient_id); 
-- Purpose: Enhances query efficiency for stress level data linked to patients.

-- Indexes for heart_attack_risk table
CREATE INDEX idx_heart_attack_risk_patient_id ON heart_attack_risk(patient_id); 
-- Purpose: Speeds up queries analyzing heart attack risk for patients.
