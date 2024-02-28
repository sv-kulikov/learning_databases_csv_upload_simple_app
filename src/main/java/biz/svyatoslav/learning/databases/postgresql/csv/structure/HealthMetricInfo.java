package biz.svyatoslav.learning.databases.postgresql.csv.structure;
public class HealthMetricInfo {
    public String patientId;
    public Integer cholesterol;
    public Integer bloodPressureSystolic;
    public Integer bloodPressureDiastolic;
    public Integer heartRate;
    public Integer triglycerides;
    public Float bmi;

    public HealthMetricInfo(String patientId, Integer cholesterol, Integer bloodPressureSystolic,
                            Integer bloodPressureDiastolic, Integer heartRate, Integer triglycerides, Float bmi) {
        this.patientId = patientId;
        this.cholesterol = cholesterol;
        this.bloodPressureSystolic = bloodPressureSystolic;
        this.bloodPressureDiastolic = bloodPressureDiastolic;
        this.heartRate = heartRate;
        this.triglycerides = triglycerides;
        this.bmi = bmi;
    }

    @Override
    public int hashCode() {
        return patientId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        HealthMetricInfo that = (HealthMetricInfo) obj;
        return patientId == that.patientId;
    }
}
