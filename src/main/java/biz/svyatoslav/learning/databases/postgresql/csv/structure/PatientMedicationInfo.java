package biz.svyatoslav.learning.databases.postgresql.csv.structure;

import java.util.Objects;

public class PatientMedicationInfo {
    public String patientId;
    public String medication;

    public PatientMedicationInfo(String patientId, String medicationId) {
        this.patientId = patientId;
        this.medication = medicationId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(patientId, medication);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PatientMedicationInfo that = (PatientMedicationInfo) obj;
        return Objects.equals(patientId, that.patientId) &&
                Objects.equals(medication, that.medication);
    }
}