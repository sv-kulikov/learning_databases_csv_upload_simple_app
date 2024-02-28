package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class LifestyleFactorInfo {
    public String patientId;
    public String diet;
    public Boolean smoking;
    public Boolean alcoholConsumption;
    public Boolean obesity;
    public Float exerciseHoursPerWeek;
    public Float sedentaryHoursPerDay;
    public Integer physicalActivityDaysPerWeek;
    public Integer sleepHoursPerDay;

    public LifestyleFactorInfo(String patientId, String diet, Boolean smoking, Boolean alcoholConsumption,
                               Boolean obesity, Float exerciseHoursPerWeek, Float sedentaryHoursPerDay,
                               Integer physicalActivityDaysPerWeek, Integer sleepHoursPerDay) {
        this.patientId = patientId;
        this.diet = diet;
        this.smoking = smoking;
        this.alcoholConsumption = alcoholConsumption;
        this.obesity = obesity;
        this.exerciseHoursPerWeek = exerciseHoursPerWeek;
        this.sedentaryHoursPerDay = sedentaryHoursPerDay;
        this.physicalActivityDaysPerWeek = physicalActivityDaysPerWeek;
        this.sleepHoursPerDay = sleepHoursPerDay;
    }

    @Override
    public int hashCode() {
        return (patientId != null ? patientId.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        LifestyleFactorInfo that = (LifestyleFactorInfo) obj;
        return patientId != null ? patientId.equals(that.patientId) : that.patientId == null;
    }
}