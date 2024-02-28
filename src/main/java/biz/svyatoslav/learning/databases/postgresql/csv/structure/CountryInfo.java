package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class CountryInfo {
    public String name;
    public String continent;

    public String hemisphere;

    public CountryInfo(String name, String continent, String hemisphere) {
        this.name = name;
        this.continent = continent;
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
        CountryInfo that = (CountryInfo) obj;
        return name.equals(that.name) && continent.equals(that.continent) && hemisphere.equals(that.hemisphere);
    }
}