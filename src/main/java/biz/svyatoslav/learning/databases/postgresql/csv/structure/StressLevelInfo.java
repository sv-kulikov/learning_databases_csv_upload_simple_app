package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class StressLevelInfo {
    public String patientId;
    public Integer stressLevel;

    public StressLevelInfo(String patientId, Integer stressLevel) {
        this.patientId = patientId;
        this.stressLevel = stressLevel;
    }

    @Override
    public int hashCode() {
        return (patientId != null ? patientId.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        StressLevelInfo that = (StressLevelInfo) obj;
        return patientId != null ? patientId.equals(that.patientId) : that.patientId == null;
    }
}