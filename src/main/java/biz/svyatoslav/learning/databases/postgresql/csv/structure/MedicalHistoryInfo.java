package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class MedicalHistoryInfo {
    public String patientId;
    public Boolean diabetes;
    public Boolean familyHistory;
    public Boolean previousHeartProblems;

    public MedicalHistoryInfo(String patientId, Boolean diabetes, Boolean familyHistory, Boolean previousHeartProblems) {
        this.patientId = patientId;
        this.diabetes = diabetes;
        this.familyHistory = familyHistory;
        this.previousHeartProblems = previousHeartProblems;
    }

    @Override
    public int hashCode() {
        return (patientId != null ? patientId.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MedicalHistoryInfo that = (MedicalHistoryInfo) obj;
        return patientId != null ? patientId.equals(that.patientId) : that.patientId == null;
    }
}
