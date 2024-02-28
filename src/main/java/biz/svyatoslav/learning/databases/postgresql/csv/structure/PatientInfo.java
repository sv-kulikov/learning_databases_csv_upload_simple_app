package biz.svyatoslav.learning.databases.postgresql.csv.structure;

public class PatientInfo {
    public String patientId;
    public int yearOfBirth;
    public String sex;
    public int income;
    public String country;

    public PatientInfo(String patientId, int yearOfBirth, String sex, int income, String country) {
        this.patientId = patientId;
        this.yearOfBirth = yearOfBirth;
        this.sex = sex;
        this.income = income;
        this.country = country;
    }

    @Override
    public int hashCode() {
        return patientId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PatientInfo that = (PatientInfo) obj;
        return patientId.equals(that.patientId);
    }
}
