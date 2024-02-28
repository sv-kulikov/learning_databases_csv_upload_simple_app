package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class MedicationInfo {
    public String name;

    public MedicationInfo(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MedicationInfo that = (MedicationInfo) obj;
        return name.equals(that.name);
    }
}
