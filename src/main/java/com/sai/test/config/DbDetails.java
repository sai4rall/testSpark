package com.sai.test.config;

import com.fasterxml.jackson.annotation.JsonProperty;

    public class DbDetails {
        @JsonProperty("db_name")
        String dbName;
        @JsonProperty("db_address")
        String dbAddress;
        @JsonProperty("db_port")
        String dbPort;
        @JsonProperty("db_username")
        String dbUsername;
        @JsonProperty("db_password")
        String dbPassword;

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getDbAddress() {
            return dbAddress;
        }

        public void setDbAddress(String dbAddress) {
            this.dbAddress = dbAddress;
        }

        public String getDbPort() {
            return dbPort;
        }

        public void setDbPort(String dbPort) {
            this.dbPort = dbPort;
        }

        public String getDbUsername() {
            return dbUsername;
        }

        public void setDbUsername(String dbUsername) {
            this.dbUsername = dbUsername;
        }

        public String getDbPassword() {
            return dbPassword;
        }

        public void setDbPassword(String dbPassword) {
            this.dbPassword = dbPassword;
        }
    }