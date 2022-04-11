package com.sai.test.config;

public class Constants {

// public static String bucketUrl ="C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\";
// public static String bucketOutputPath = "\\output\\eai_objects\\";
// public static String WIFI_NNI_PATH = "C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\output\\eai_objects\\wifi_nni\\";
// public static String TEMPEST_NODE_ID_PATH = "C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\input\\TEMPEST_NODE_ID.csv";
// public static String BBH_SPREAD_SHEETS_PATH = "C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\input\\bbhSpreadsheets.csv";
// public static String SITE_TO_RFCELL_LOOKUP_PATH = "C:/Users/D985268/IdeaProjects/udmf-data-migration-service/src/main/resources/input/site_to_rfCell_lookup";
// public static String enm_nodeB_PATH = "C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\input\\enm\\enm_eNodeB.csv";
// public static String enm_nodeBS_PATH="C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\input\\enm\\enm_eNodeB.csv";
// public static String GNODEB_DU="C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\input\\enm\\enm_gNodeB-DU.csv";

public static String bucketUrl = "s3://quoll-d985268/";
public static String bucketOutputPath = "eai_objects/";
public static String WIFI_NNI_PATH= bucketUrl+"eai_objects/excluded/wifi-nni-1240";
public static String TEMPEST_NODE_ID_PATH= bucketUrl+"input/TEMPEST_NODE_ID.csv";
public static String BBH_SPREAD_SHEETS_PATH= bucketUrl+"input/bbhSpreadsheets.csv";
// public static String SITE_TO_RFCELL_LOOKUP_PATH= "s3://emrdisco/eai_objects/site_to_rfCell_lookup";
public static String enm_nodeB_PATH=bucketUrl+"input/enm/enm_eNodeB.csv";
public static String enm_nodeBS_PATH=bucketUrl+"input/enm/enm_eNodeB.csv";
public static String GNODEB_DU=bucketUrl+"input/enm/enm_gNodeB-DU.csv";
}