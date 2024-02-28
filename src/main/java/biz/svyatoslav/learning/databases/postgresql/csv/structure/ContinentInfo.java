package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class ContinentInfo {
    public String name;
    public String hemisphere;

    public ContinentInfo(String name, String hemisphere) {
        this.name = name;
        this.hemisphere = hemisphere;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ContinentInfo that = (ContinentInfo) obj;
        return name.equals(that.name) && hemisphere.equals(that.hemisphere);
    }
}
