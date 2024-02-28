WITH
patient_medication_names AS (
  SELECT
    patient_id,
    ARRAY_AGG(name ORDER BY name) AS medication_names
  FROM patient_medication
  JOIN medication ON patient_medication.medication_id = medication.medication_id
  GROUP BY patient_id
),

data_query AS (
  SELECT
    patients.patient_id AS "Patient ID",
    (EXTRACT(YEAR FROM age(CURRENT_DATE, DATE(patients.year_of_birth||'-01-01')))) AS "Age",
    patients.sex AS "Sex",
    health_metrics.cholesterol AS "Cholesterol",
    health_metrics.blood_pressure_systolic || '/' || health_metrics.blood_pressure_diastolic AS "Blood Pressure",
    health_metrics.heart_rate AS "Heart Rate",
    CASE WHEN medical_history.diabetes THEN 1 ELSE 0 END AS "Diabetes",
    CASE WHEN medical_history.family_history THEN 1 ELSE 0 END AS "Family History",
	CASE WHEN lifestyle_factors.smoking THEN 1 ELSE 0 END AS "Smoking",
    CASE WHEN lifestyle_factors.obesity THEN 1 ELSE 0 END AS "Obesity",
    CASE WHEN lifestyle_factors.alcohol_consumption THEN 1 ELSE 0 END AS "Alcohol Consumption",
    ROUND(lifestyle_factors.exercise_hours_per_week::numeric, 2) AS "Exercise Hours Per Week",
    diets.name AS "Diet",
    CASE WHEN medical_history.previous_heart_problems THEN 1 ELSE 0 END AS "Previous Heart Problems",
    (patient_medication_names.medication_names)[1] AS "Medication Use",
    stress_levels.stress_level AS "Stress Level",
    ROUND(lifestyle_factors.sedentary_hours_per_day::numeric, 2) AS "Sedentary Hours Per Day",
    patients.income AS "Income",
    ROUND(health_metrics.bmi::numeric, 2) AS "BMI",
    health_metrics.triglycerides AS "Triglycerides",
    lifestyle_factors.physical_activity_days_per_week AS "Physical Activity Days Per Week",
    ROUND(lifestyle_factors.sleep_hours_per_day::numeric, 2) AS "Sleep Hours Per Day",
    countries.name AS "Country",
    continents.name AS "Continent",
    continents.hemisphere || ' Hemisphere' AS "Hemisphere",
    CASE WHEN heart_attack_risk.risk_level THEN 1 ELSE 0 END AS "Heart Attack Risk"
  FROM patients
    JOIN countries ON patients.country_id = countries.country_id
    JOIN continents ON countries.continent_id = continents.continent_id
    JOIN health_metrics ON patients.patient_id = health_metrics.patient_id
    JOIN lifestyle_factors ON patients.patient_id = lifestyle_factors.patient_id
    JOIN diets ON lifestyle_factors.diet_id = diets.diet_id
    JOIN medical_history ON patients.patient_id = medical_history.patient_id
    JOIN stress_levels ON patients.patient_id = stress_levels.patient_id
    JOIN heart_attack_risk ON patients.patient_id = heart_attack_risk.patient_id
    LEFT JOIN patient_medication_names ON patients.patient_id = patient_medication_names.patient_id
)
SELECT * FROM data_query;
