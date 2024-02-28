package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class DietInfo {
    public String name;

    public DietInfo(String name) {
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
        DietInfo that = (DietInfo) obj;
        return name.equals(that.name);
    }
}
