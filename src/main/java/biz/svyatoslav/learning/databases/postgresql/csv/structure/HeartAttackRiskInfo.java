package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class HeartAttackRiskInfo {
    public String patientId;
    public Boolean riskLevel;

    public HeartAttackRiskInfo(String patientId, Boolean riskLevel) {
        this.patientId = patientId;
        this.riskLevel = riskLevel;
    }

    @Override
    public int hashCode() {
        return (patientId != null ? patientId.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        HeartAttackRiskInfo that = (HeartAttackRiskInfo) obj;
        return patientId != null ? patientId.equals(that.patientId) : that.patientId == null;
    }

}
