package org.orcan.job;

public enum Job {
    AVERAGE_TEMP("Average Temperature by station in Kelvin", AverageTemperatureByStation.class.getName(), "avgtemp"),
    MINMAX_HUMIDITY("Min & Max Humidity", MinMaxHumidity.class.getName(), "minmaxhum"),
    MAX_PRECIP("Max Precipitation", MaxPrecip.class.getName(), "maxprecip"),
    STD_TEMP("Standard Deviation of the temperatures of each station", StdTemperatureByStation.class.getName(), "stdtemp"),
    NUM_MEASUREMENT("Number of measurements in each timestamp", NumberOfMeasurementsByTime.class.getName(), "nummes");

    private final String displayName;
    private final String className;
    private final String jarName;

    Job(String displayName, String className, String jarName) {
        this.displayName = displayName;
        this.className = className;
        this.jarName = jarName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getClassName() {
        return className;
    }

    public String getJarName() {
        return jarName;
    }
}
