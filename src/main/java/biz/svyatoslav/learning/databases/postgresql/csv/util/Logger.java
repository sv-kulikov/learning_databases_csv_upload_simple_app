package biz.svyatoslav.learning.databases.postgresql.csv.util;

public class Logger {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public static void logMessage(String message) {
        logMessage(message, false);
    }

    public static void logMessage(String message, boolean newLine) {
        if (newLine) {
            System.out.println(ANSI_RESET + message + ANSI_RESET);
        } else {
            System.out.print(ANSI_RESET + message + ANSI_RESET);
        }
    }

    public static void logError(String message) {
        logError(message, false);
    }

    public static void logError(String message, boolean newLine) {
        if (newLine) {
            System.out.println(ANSI_RED + message + ANSI_RESET);
        } else {
            System.out.print(ANSI_RED + message + ANSI_RESET);
        }
    }

    public static void logSuccess(String message) {
        logSuccess(message, false);
    }

    public static void logSuccess(String message, boolean newLine) {
        if (newLine) {
            System.out.println(ANSI_GREEN + message + ANSI_RESET);
        } else {
            System.out.print(ANSI_GREEN + message + ANSI_RESET);
        }
    }

    public static void logHeader(String message) {
        logSuccess(message, false);
    }

    public static void logHeader(String message, boolean newLine) {
        if (newLine) {
            System.out.println(ANSI_BLUE + message + ANSI_RESET);
        } else {
            System.out.print(ANSI_BLUE + message + ANSI_RESET);
        }
    }

}
