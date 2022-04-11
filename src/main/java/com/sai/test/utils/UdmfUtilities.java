package com.sai.test.utils;



import com.sai.test.config.ClusterConfig;
import com.sai.test.config.Constants;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StructType;

import scala.reflect.ClassTag;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UdmfUtilities implements Serializable {
static Logger logger = Logger.getLogger(UdmfUtilities.class);
UserDefinedFunctions userDefinedFunctions;
Broadcast<BroadCastLookUpData> lookUpDataBroadcast;

public UdmfUtilities(){
BroadCastLookUpData lookUpData=new BroadCastLookUpData();
this.lookUpDataBroadcast= ClusterConfig.getSparkSession().sparkContext().broadcast(lookUpData, classTag(BroadCastLookUpData.class));
this.userDefinedFunctions=new UserDefinedFunctions(this);
}

public Dataset readFile(SparkSession session, StructType schema, String path) {
if (schema == null) {
return session
.read().option("header", "true").csv(path);
} else {
return session.read().option("header", "true")
.schema(schema)
.csv(path);
}
}

public Dataset applyInitialTransformaions(Dataset qdf) {
//# Add in the sector
Dataset<Row> q = qdf.select(qdf.col("*"),
qdf.col("cell_name").substr(5, 2).alias("utchar"),
qdf.col("cell_name").substr(7, 1).alias("sector"));

// drop IGNORED columns
q = q.drop("bts_instance_display_name", "clli_number_bed", "clli_number_hex",
"do_rnc", "multi_nids", "nid", "switch_number", "xrtt_enabled", "cell_fro_id", "cid_hex", "lac_hex", "enbid_hex");
q.where(q.col("base_station_name").like("%WIFI%"))
.write()
.mode("overwrite")
.json(Constants.WIFI_NNI_PATH);
//# Remove Excluded rows
//# WiFi NNI-1240
q = q.where(functions.not(q.col("base_station_name").like("%WIFI%")));
// ===> there is no SectorNumber in the table clarification needed on this

//# cleanup the sector field so that it is in Integer (Required for EAI)
//# Note: Cells with aplha sectors are directionless, so sector does nto make sense in this context.

q = q.withColumn("sectorNumber", userDefinedFunctions.eaisectorNumber.apply(functions.col("sector")));
//# As per NNI-1292 and NNI-1801 we are abandoning cells with the status :P 'Erroneous entry', 'Removed'
q = q.where(functions.not(q.col("cell_status").isin("Erroneous entry", "Removed")));
// q.show();
return q;
}

public Dataset addAdditionalAttributes(Dataset q) {
return q.withColumn("|telstraCellAttributes|cellFunction", userDefinedFunctions.eaiCellFunction.apply(q.col("cell_function"))).
withColumn("|telstraCellAttributes|hasSpecialEvent", userDefinedFunctions.eaiBool.apply(q.col("special_event_cell"))).
withColumn("|telstraCellAttributes|hasSignificantSpecialEvent", userDefinedFunctions.eaiBool.apply(q.col("special_event"))).
withColumn("|telstraCellAttributes|hasPriorityAssistCustomers", userDefinedFunctions.eaiBool.apply(q.col("priority_assist"))).
withColumn("|telstraCellAttributes|hasHighSeasonality", userDefinedFunctions.eaiBool.apply(q.col("high_seasonality"))).
withColumn("|telstraCellAttributes|hasWirelessLocalLoopCustomers", userDefinedFunctions.eaiBool.apply(q.col("wll"))).
withColumn("|telstraCellAttributes|mobileSwitchingCentre", userDefinedFunctions.eaivalidMscNode.apply(q.col("msc_node"))).
withColumn("|telstraCellAttributes|mobileServiceArea", q.col("msa")).
// withColumn("|telstraCellAttributes|quollIndex", userDefinedFunctions.eaiInt.apply(q.col("cell_index"))).
withColumn("|telstraCellAttributes|quollIndex", userDefinedFunctions.eaiBigInt.apply(q.col("cell_index"))). //Added this because of lte data having bigdecimal in the data, handling cast exception
withColumn("|telstraCellAttributes|closedNumberArea", userDefinedFunctions.eaiAreaCode.apply(q.col("cna")))
.select(q.col("*"),
q.col("billing_name").alias("|telstraCellAttributes|billingName"),
q.col("roamer").alias("|telstracellAttributes|roamingAgreement"),
functions.col("|telstraCellAttributes|cellFunction"),
functions.col("|telstraCellAttributes|closedNumberArea"),
q.col("coverage_classification").alias("|telstracellAttributes|coverageClassification"),
functions.regexp_replace(q.col("coverage_statement"), "[\\n\\r]+", " ").alias("|telstraCellAttributes|coverageStatement"),
functions.col("|telstracellAttributes|hasPriorityAssistcustomers"),
functions.col("|telstracellAttributes|haswirelessLocalLoopCustomers"),
q.col("optimisation_cluster").alias("|telstracellAttributes|optimisationCluster"),
q.col("sac_dec").alias("|telstracellAttributes|serviceAreacode").cast(IntegerType$.MODULE$),
q.col("owner").alias("|telstracellAttributes|wirelessServiceOwner"),
functions.col("|telstraCellAttributes|hasSpecialEvent"),
functions.col("|telstracellAttributes|hassignificantSpecialEvent"),
functions.col("|telstracellattributes|mobileSwitchingCentre"),
functions.col("|telstraCellAttributes|mobileServiceArea"),
functions.col("|telstracellAttributes|quolLindex"),
// functions.col("|telstracellAttributes|Big_quollIndex"),
functions.col("|telstraCellAttributes|hasHighSeasonality"));
}

public Dataset transformBsc(Dataset q) {
return (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))
.select(q.col("bsc_rnc_node").alias("name"))
.distinct()
.withColumn("status", functions.lit("UNKNOWN"))
.withColumn("$type", functions.lit("ocw/bsc"))
.withColumn("$action", functions.lit("createrOrUpdate"))
.withColumn("$refId", functions.col("name"))
.select("$type", "$action", "$refId", "name", "status") // # datasync requires the attributes to be first
);
}

public Dataset transformRnc(Dataset q) {
return (q.where(functions.not(q.col("technology").like("GSM%")).and(q.col("bsc_rnc_node").isNotNull()))
.select(q.col("bsc_rnc_node").alias("name"))
.distinct()
.withColumn("status", functions.lit("UNKNOWN"))
.withColumn("$type", functions.lit("ocw/rnc"))
.withColumn("$action", functions.lit("createOrUpdate"))
.withColumn("$refId", functions.col("name"))
.select("$type", "$action", "$refId", "name", "status")
);
}

public Dataset transfromBts(Dataset q) {
return (q
.where(q.col("technology").like("GSM%")) // q.iub_rbsid.isNotNull() & & (q.cell_status != "Erroneous entry")
.select(q.col("cell_name").substr(1, 4).alias("name"), q.col("iub_rbsid").alias("btsId"))
.withColumn("$type", functions.lit("ocw/bts"))
.withColumn("$action", functions.lit("createOrUpdate"))
.withColumn("$refId", functions.col("name"))
.withColumn("status", functions.lit("UNKNOWN"))
.distinct()
.select("$type", "$action", "$refId", "name", "status", "btsId")); // dataSync requires the $ attributes to be first
//bts.show()
}
public Dataset transfromBscToBtsLookup(Dataset q) {
return (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))
.withColumn("$type", functions.lit("ocw/bsc"))
.withColumn("$action", functions.lit("lookup"))
.select(functions.col("$type"),
q.col("bsc_rnc_node").alias("$refId"),
functions.col("$action"),
q.col("bsc_rnc_node").alias("name"))
.distinct());
}
public Dataset transfromBscToBts(Dataset q) {
return (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))// #q.iub_rbsid.isNotNull() & & (q.cell_status != 'Erroneous entry')
.withColumn("$type", functions.lit("ocw/bts"))
.withColumn("$bsc", functions.array(functions.col("bsc_rnc_node")))
.withColumn("$action", functions.lit("createOrUpdate"))
.select(q.col("cell_name").substr(1, 4).alias("$refId"),
functions.col("$type"), functions.col("$action"), functions.col("$bsc"),
q.col("cell_name").substr(1, 4).alias("name"))
.distinct());
}

public Dataset generateBsDataset(Dataset t, Dataset b) {
Dataset n = transform4GLRAN(t);
n = n.union(transform5GNGRAN(t));
n = n.union(transform3GWRAN(t));


Dataset b2 = (b
.withColumn("type", userDefinedFunctions.eaiBbhType.apply(functions.col("technology")))
.withColumn("status", functions.lit("Unknown"))
.select(b.col("name"), functions.col("type"), b.col("node_code"), b.col("id"), b.col("virtual_rnc"), functions.col("status"))
);
return joinBsAndn(b2, n);
}

public static Integer genSectorNumber(String sector) {

if (sector != null && !sector.equalsIgnoreCase("")) {
if (Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9").indexOf(sector) > -1) {
return Integer.parseInt(sector.trim());
} else {
return null;
}
} else {
return null;
}
}


public String mapCellStatus(String qStatus) {
// # note: we cannot have a status on None as the record will not load into EAI
return this.lookUpDataBroadcast.getValue().cellStatusMapDict.get(qStatus.trim()) != null ? this.lookUpDataBroadcast.getValue().cellStatusMapDict.get(qStatus.trim()) : this.lookUpDataBroadcast.getValue().cellStatusMapDict.get("Concept");
}

public String mapStatus(String status) {
// # note: we cannot have a status on None as the record will not load into EAI
if(status != null)
return lookUpDataBroadcast.getValue().statusMapDict.get(status.trim()) != null ? this.lookUpDataBroadcast.getValue().statusMapDict.get(status.trim()) : this.lookUpDataBroadcast.getValue().statusMapDict.get("Unknown");
else
return this.lookUpDataBroadcast.getValue().statusMapDict.get("Unknown");
}

public String mapCellType(String qType) {
// # note: we cannot have a status on None as the record will not load into EAI
if(qType !=null)
return this.lookUpDataBroadcast.getValue().cellTypeMapDict.get(qType) != null ? this.lookUpDataBroadcast.getValue().cellTypeMapDict.get(qType) : this.lookUpDataBroadcast.getValue().cellTypeMapDict.get("TBA");
else
return this.lookUpDataBroadcast.getValue().cellTypeMapDict.get("TBA");
}

public String mapCellFunction(String qFunction) {
if (qFunction != null && isInteger(qFunction)) {
int temp = Integer.parseInt(qFunction.trim());
return this.lookUpDataBroadcast.getValue().cellFunctionbict.get(temp);
} else {
return null;
}
}

public String mapAreaCode(String qAc) {
if (qAc != null && isInteger(qAc)) {
Integer ac = Integer.parseInt(qAc.trim());
return this.lookUpDataBroadcast.getValue().areaCodeDict.get(ac);
} else {
// logger.info("Error converting area code" + qAc);
return null;
}
}


// # use eaiInt instead
//# convert RAC to an int
public static Integer cleanRac(String qrac) {
if (qrac != null && isInteger(qrac)) {
return Integer.parseInt(qrac.trim());
} else {
return null;
}
}

public String validateMscNode(String node) {
return node != null ? this.lookUpDataBroadcast.getValue().validiscNodeDict.get(node.trim()) : null;
}

// # convert a string flag into a Boolean
public static boolean cleanBool(String qVal) {
if (qVal != null &&
(qVal.equalsIgnoreCase("TRUE") ||
(qVal.equalsIgnoreCase("SIGNIFICANT")))) {
return true;
} else {
return false;
}
}

public static String cleanYN(String qVal) {
if (qVal != null &&
(qVal.equalsIgnoreCase("TRUE"))) {
return "YES";

} else {
return "NO";
}
}

// # the LAC needs to be converted to Int in the range 1->65,535. Values of are to be changed to None.
public static Integer cleanLac(String qval) {
if (qval != null && isInteger(qval)) {
Integer lac = Integer.parseInt(qval.trim());
if (lac > 0)
return lac;
else
return null;
} else {
logger.info("Error converting LAC:" + qval);
return null;
}
}

//TODO needto check why we have two implementations for cleanRacVal
//# the RAC needs to be converted to Int in the range 0->255. Values above 255 are changed to None
public static Integer cleanRacVal(String qval) {
if (qval != null && isInteger(qval)) {
Integer rac = Integer.parseInt(qval.trim());
if (rac < 255)
return rac;
else
return null;
} else {
logger.info("Error converting RAC::" + qval);
return null;
}
}

public static Integer cleanInt(String qval) {
try {
if (qval != null && isInteger(qval)) {
return Integer.parseInt(qval.trim());
} else {
// System.out.println("Error converting = " + qval + "to int");
// logger.info("Error converting qval = " + qval + "to int");
return null;
}
}catch(Exception e){
//logger.info("Error converting int qVal = " + qval + "to int");
return null;
}
}
    public static Integer cleanBigInt(BigDecimal qval) {
        try {
            if (qval != null)
                return qval.intValue();
            else
                return null;
        }catch (Exception e){

            logger.info("Big int Error converting BigDecimal qVal = " + qval + "to int");
            return null;
        }

    }

    public Dataset cleanlyConvertssitesToInteger(Dataset q) {
        return (q
                .select(q.col("base_station_name").alias("name"),
                        q.col("base_station_name").alias("$refId"),
                        q.col("state").alias("stateProv"),
                        q.col("nm_address_id").alias("siteId").cast(IntegerType$.MODULE$)// = # cleanly converts to an integer
                )
                .distinct()
                .withColumn("$type", functions.lit("oci/site"))
                .withColumn("status", functions.lit("Live"))
                .withColumn("type", functions.lit("OTHER"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select("$type", "name", "$refId", "$action", "status", "type", "stateProv", "siteId") // #this is just to re-ord
        );
    }

    public static Integer cleanUra(String qStr) {
        if (qStr == null) {
            return null;
        } else {
// # return just the first element in the array of URA's
// # Analysis shows that there are no cells with more than one distinct URA
            try{
                if(isInteger(qStr.split(":")[0]))
                    return Integer.parseInt(qStr.split(":")[0].trim());
                else
                    return null;
            }catch(ArrayIndexOutOfBoundsException aie){
                return null;
            }

        }
    }

    public static Integer cleanTechnology(String qStr) {
        if (qStr == null) {
            return null;
        } else {
            try {
// # 'NR26G' == 'NR26000'
                String s = qStr.replace("G", "000").substring(2);
                return Integer.parseInt(s.trim());
            } catch (Exception e) {
                logger.warn("error cleaning technology field:" + qStr);
                return null;
            }
        }
    }

    public static String cleanRepeaterTechnology(String qStr){
        if(qStr ==null)
            return null;
        else{
            try {
// # 'NR26G' == 'NR26000'
// String s = qStr.replace("G", "000").substring(2);
                if(qStr.contains("WCDMA"))
                    qStr = qStr.replace("WCDMA","UMTS");
            }catch(Exception exe){
                logger.warn("error cleaning Repeater technology field:" + qStr);
                return null;
            }
            return qStr;

        }
    }

    public static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s.trim());
        } catch (Exception e) {
            return false;
        }
// only got here if we didn't return false
        return true;
    }

    public static String technologyToType(String sTech) {
        if (sTech != null && !sTech.equalsIgnoreCase("")) {
            if (sTech.indexOf("NR") > -1)
                return "On/nrCell";
            if (sTech.indexOf("LTE") > -1)
                return "ocw/lteCell";
            else if (sTech.indexOf("WCDMA") > -1)
                return "Ocw/umtsCell";
            else if (sTech.indexOf("GSM") > -1)
                return "ocw/gsmCell";
            else
                return null;

        } else {
            return null;
        }
    }

    public static String getBbhType(String tech) {
        if (tech == null)
            return null;
        else {
            if (tech.indexOf("LTE") >= 0) {
                return "ocw/eNodeB";
            }
            if (tech.indexOf("NR") >= 0) {
//#return 'ocw/gnbcuup'
                return "ocw/gnbdu";
            }
            return null;
        }
    }

    public String genEGNodeBName(Integer du, String site, Integer nid, String nodeCode) {
/*
UDF for building the DU if there is no exsting du field.
*/

        if (du == null) {
            try {
                String incChar = "";
                Pattern lte1DigitPattern = Pattern.compile("\\(LTE[1-9]\\)");
                Matcher m1d = lte1DigitPattern.matcher(site);
                Pattern lte2DigitPattern = Pattern.compile("\\(LTE[1-9][0-9]\\)");
                Matcher m2d = lte2DigitPattern.matcher(site);
// # check for 2 digit increment numbers and convert them to letters
                if (m2d.find()) {
                    site = site.substring(m2d.start());
                    incChar = "" + site.replaceAll("[^0-9]", "");
                    incChar = this.lookUpDataBroadcast.getValue().ranNumberingDict.get(incChar); //# convert to a character
                } else if (m1d.find()) {
                    site = site.substring(m1d.start());
                    incChar = "" + site.replaceAll("[^0-9]", "");
                } else {
// # search for single digit increment numbers.
// # extract out the section within the (), assuming it is at the end of the site string
// Checking this for non existence of data in () in site and assign it to incChar
                    if (site.indexOf('(') == -1) {
                        incChar = site;
                    } else {
                        site = site.substring(site.indexOf('('));
// # replace '1-x' with just 1. This seems to be the case for all instances that have a du_number populated.
                        site = site.replace("1-2", "1");
                        site = site.replace("1-3", "1");
                        site = site.replace("1-4", "1");
// # first remove '3G', '4G', '5G'
                        site = site.replace("3G", "");
                        site = site.replace("4G", "");
                        site = site.replace("5G", "");
// # Extract out the reamining digits
                        incChar = "" + site.replaceAll("[^0-9]", "");
                    }

                }
                if (incChar != null) {
                    if (incChar.length() == 1) {
                        int id = nid / 100000;
                        return nodeCode + incChar + id;
                    } else if (incChar.length() == 0) {
// if no number found assume '1'
                        int id = nid / 100000;
                        return nodeCode + "1" + id;
                    } else {
                    }
                    return null;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return nodeCode + du;
    }


    public String genNodeBName(String site, String nodeCode) {
        try {
            Pattern pattern = Pattern.compile("\\([1-9]\\)");
            Matcher m = pattern.matcher(site);
            if(m.find()){
                site = site.substring(m.start());
                String incChar = ""+site.replaceAll("[^0-9]", "");
                return nodeCode + incChar;
            }
            pattern = Pattern.compile("\\([1-9][0-9]\\)");
            m = pattern.matcher(site);
            if(m.find()){
                site = site.substring(m.start());
                String incChar = ""+site.replaceAll("[^0-9]", "");
                return nodeCode + this.lookUpDataBroadcast.getValue().ranNumberingDict.get(incChar);
            }

            return nodeCode + "1";
        } catch (Exception e) {
            return null;
        }
    }

    public static String extractNameFromMecontext(String qStr, Boolean paddOne) {
        if (paddOne == null) {
            paddOne = false;
        }
        if (qStr == null) {
            return null;
        } else {
            try {
                if (paddOne) {
                    String tmp = qStr.split("_")[1];
                    if (tmp.length() == 4) {
                        return tmp + "1";
                    } else {
                        return tmp;
                    }
                } else {
                    return qStr.split("_")[1];
                }

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static Integer extractIdFromMecontext(String qStr) {
        if (qStr == null) {
            return null;
        } else {
            try {
                String[] qStrArr = qStr.split("_");
                return Integer.parseInt(qStr.split("_")[qStrArr.length - 1]);
            } catch (Exception e) {
                logger.warn("error extracting ID from mecontext for:" + qStr);
                return null;
            }
        }
    }

    public static String enmGnbType(String mecontext) {
        if (mecontext == null) {
            return null;
        }
        if (mecontext.indexOf("_BBH_") >= 0) {
// #return "ocw/gnbcuup"
// # ^ is not required as we now are not including the CU objects.
            return "ocw/gnbdu";
        } else {
            return "ocw/gnbdu";
        }
    }


    public Dataset transformLte(Dataset q) {
        System.out.println("LTE transformation in progress =====> ");
        return (q.where((q.col("technology").like("LTE%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/lteCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status"))) // ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //ocw:telstraCellTypePicklist
                .withColumn("|telstraLteCellAttributes|trackingAreaCode", userDefinedFunctions.eaiInt.apply(functions.col("tac")))
// # To keep here as per NNI-1336 and NNI-1622
                .withColumn("qualifiedCellId", functions.expr("conv(eci, 16, 10)"))
// # Convert eci from hex to decimal
                .select(functions.col("$type"),
                        q.col("cell_name").alias("$refId"),
                        functions.col("$action"),
                        q.col("cell_name").alias("name"),
                        functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 4, 99),
                                functions.lit("MHz")).alias("band'"),
                        functions.col("qualifiedCellId"),
                        functions.col("cellType"), q.col("sectorNumber"),
                        q.col("cid_dec").alias("cellId").cast(IntegerType$.MODULE$),
//
// # cleanly converts to an integer. However need to validate as per NNI-1630
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),
// # Dynamic Attributes
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
// q.col("|telstraCellAttributes|Big_quollIndex").as("quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),
                        functions.col("|telstraLteCellAttributes|trackingAreaCode"),
                        q.col("plmn").alias("|telstraLteCellAttributes|plmn").cast(DataTypes.IntegerType),
                        q.col("cgi").alias("|telstraLteCellAttributes|ecgi") // # To be changed to calculated field as per NNI-1627
                )
        );
    }


    public Dataset transformGsm(Dataset q){
        System.out.println("GSM transformation in progress =====> ");
        return (q.where((q.col("technology").like("GSM%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/gsmCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))// # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //# ocw:telstraCellTypePicklist
// .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("egprsActivated", userDefinedFunctions.eaiYN.apply(functions.col("edge")))// # values are Yes/No
                .withColumn("|telstraGsmCellAttributes|evdoEnabled", userDefinedFunctions.eaiBool.apply(functions.col("evdo")))
                .withColumn("gprsActivated", userDefinedFunctions.eaiYN.apply(functions.col("gprs"))) //# values are Yes/No
                .withColumn("rac", userDefinedFunctions.eaiInt.apply(functions.col("rac_dec")))
                .withColumn("|telstraGsmCellAttributes|broadcastCode", userDefinedFunctions.eaiInt.apply(functions.col("code_for_cell_broadcast")))
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"),
                        functions.col("$action"), q.col("cell_name").alias("name"),
                        functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 4, 99),
                                functions.lit(" MHz")).alias("band"),
                        functions.col("cellType"), q.col("sectorNumber"), //q.col("lac_dec.alias("lac").cast(IntegerType()), # cleanly converts to an integer.
// functions.col("lac"),//TODO fix this
                        q.col("cgi"),
                        functions.col("egprsActivated"),
                        functions.col("gprsActivated"),
                        functions.col("rac"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"), // Dynamic Attributes
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),
                        functions.col("|telstraGsmCellAttributes|broadcastCode"),
                        q.col("plmn").alias("|telstraGsmCellAttributes|plmn").cast(IntegerType$.MODULE$),
                        functions.col("|telstraGsmCellAttributes|evdoEnabled"),
                        q.col("gsm03_38_coding").alias("|telstraGsmCellAttributes|gsm338Coding")));
    }

    public Dataset transformUmts(Dataset q) {
        return (q.where((q.col("technology").like("WCDMA%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/umtsCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status"))) // # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //# ocw:telstraCellTypePicklist
// .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("rac", userDefinedFunctions.eaiRac.apply(functions.col("rac_dec")))
                .withColumn("ura", userDefinedFunctions.eaiUra.apply(functions.col("ura")))
                .withColumn("trackingAreaCode", userDefinedFunctions.eaiInt.apply(functions.col("tac"))) // # Convert string to int via udf
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"),
                        functions.col("$action"), q.col("cell_name").alias("name"), functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 6, 99),
                                functions.lit(" MHz")).alias("band"),
                        functions.col("cellType"),
                        q.col("cgi"),
// functions.col("lac"),//TODO fix this
// #q.lac_dec.alias('lac').cast(IntegerType()), # cleanly converts to an integer.
                        functions.col("rac"),
                        functions.col("ura"),
                        q.col("cid_dec").alias("cellId").cast(IntegerType$.MODULE$), // # cleanly converts to an integer.
                        q.col("cell_inservice_date").alias("originalOnAirDate"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),

// # Dynamic Attributes:
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),

                        q.col("ro").alias("|telstraUmtsCellAttributes|routingOrigin").cast(IntegerType$.MODULE$), // # cleanly converts to an integer.
                        q.col("plmn").alias("|telstraUmtsCellAttributes|plmn").cast(IntegerType$.MODULE$)
// #q.hs_support.alias('|Telstra UMTS Cell Attributes|HS Supported') # Deprecated
                ));
    }

    public Dataset transformNr(Dataset q) {
        return (q.where((q.col("technology").like("NR%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/nrCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status"))) // # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) // # ocw:telstraCellTypePicklist
                .withColumn("bsChannelBandwidthDownlink", userDefinedFunctions.eaiChannel.apply(functions.col("technology")))
                .withColumn("bsChannelBandwidthUplink", userDefinedFunctions.eaiChannel.apply(functions.col("technology")))
                .withColumn("localCellIdNci", functions.expr("conv(eci, 16, 10)")) // # Convert eci from hex to decimal
                .withColumn("trackingAreaCode", userDefinedFunctions.eaiInt.apply(functions.col("tac"))) // # Convert string to int via udf
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"), functions.col("$action"),
                        q.col("cell_name").alias("name"), functions.col("status"),
                        functions.col("bsChannelBandwidthDownlink"), functions.col("bsChannelBandwidthUplink"),
                        functions.col("cellType"), functions.col("localCellIdNci"), functions.col("trackingAreaCode"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),

// # Dynamic Attributes:
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),
                        q.col("cgi").alias("|telstraNrCellAttributes|ncgi")
                )
        );
    }

    public Dataset transformRepeater(Dataset q){

        return(
                q.where(q.col("rru_donor_node").equalTo("repeater"))
                        .withColumn("$type", functions.lit("ocw/repeater"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("status", userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))
                        .withColumn("|telstraRepeaterAttributes|cellType", userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) // # ocw:telstraCellTypePicklist
                        .withColumn("|telstraRepeaterAttributes|hasPriorityAssistCustomers", userDefinedFunctions.eaiBool.apply(functions.col("priority_assist")))
                        .withColumn("|telstraRepeaterAttributes|hasWirelessLocalLoopCustomers", userDefinedFunctions.eaiBool.apply(functions.col("wll")))
                        .withColumn("|telstraRepeaterAttributes|isSubjecttoEmbargo", userDefinedFunctions.eaiBool.apply(functions.col("embargo_flag")))
                        .withColumn("|telstraRepeaterAttributes|hasSpecialEvent", userDefinedFunctions.eaiBool.apply(functions.col("special_event_cell")))
                        .withColumn("|telstraRepeaterAttributes|hasSignificantSpecialEvent",userDefinedFunctions.eaiBool.apply(functions.col("special_event")))
                        .withColumn("|telstraRepeaterAttributes|hasHighSeasonality", userDefinedFunctions.eaiBool.apply(functions.col("high_seasonality")))
                        .withColumn("|telstraRepeaterAttributes|mobileServiceArea", functions.col("msa"))
                        .withColumn("|telstraRepeaterAttributes|quollIndex", userDefinedFunctions.eaiBigInt.apply(functions.col("cell_index")))
                        .withColumn("systemType", userDefinedFunctions.eaiSystemType.apply(functions.col("technology")))
                        .select(functions.col("$type"),
                                functions.col("$action"),
                                q.col("cell_name").alias("$refId"),
                                q.col("cell_name").alias("name"),
                                functions.col("status"),
                                functions.col("systemType"), q.col("note").alias("comments"),

// # Dynamic Attributes
                                functions.col("|telstraRepeaterAttributes|cellType"),
                                q.col("cell_inservice_date").alias("|telstraRepeaterAttributes|originalOnAirDate"),
                                q.col("coverage_classification").alias("|telstraRepeaterAttributes|coverageClassification"),
                                q.col("coverage_statement").alias("|telstraRepeaterAttributes|coverageStatement"),
                                functions.col("|telstraRepeaterAttributes|hasPriorityAssistCustomers"),
                                functions.col("|telstraRepeaterAttributes|hasWirelessLocalLoopCustomers"),
                                q.col("optimisation_cluster").alias("|telstraRepeaterAttributes|optimisationCluster"),
                                q.col("owner").alias("|telstraRepeaterAttributes|wirelessServiceOwner"),
                                functions.col("|telstraRepeaterAttributes|hasSpecialEvent"),
                                functions.col("|telstraRepeaterAttributes|hasSignificantSpecialEvent"),
                                functions.col("|telstraRepeaterAttributes|isSubjecttoEmbargo"),
                                functions.col("|telstraRepeaterAttributes|hasHighSeasonality"),
                                functions.col("|telstraRepeaterAttributes|mobileServiceArea"),
                                functions.col("|telstraRepeaterAttributes|quollIndex")
                        )
        );


    }

    public Dataset transformSiteToRfCellLookUp(Dataset q) {
        return (q.where((q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("oci/site"))
                .withColumn("$action", functions.lit("LOOKUP"))

                .select(functions.col("$type"), functions.col("$action"),
                        q.col("base_station_name").alias("$refId"),
                        q.col("base_station_name").alias("name")
                )
                .distinct()
        );

    }

    public Dataset transformSiteToRfCell(Dataset q) {
        return (q.where((q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", userDefinedFunctions.eaiTechnologyToType.apply(functions.col("technology")))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$site", functions.array(functions.col("base_station_name")))
                .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                        functions.col("$site"), q.col("cell_name").alias("name")
                )
        );
    }
    // implementing this as part of NNI-3078
    public Dataset tranformDesigner(Dataset q){
        return(q.where(q.col("designer").isNotNull())
                .withColumn("name",functions.lower(functions.col("designer")))
                .withColumn("$type", functions.lit("tam/userExtension"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(functions.col("name"),functions.col("$type"), functions.col("$action"))
        );

    }
    public Dataset transform4GLRAN(Dataset t) {
        System.out.println("transform4GLRAN =======================>");
        return (t.where(t.col("network").isin("4G (LRAN)").and(t.col("rbs_id").isNotNull()))
                .withColumn("name", userDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"),
                        functions.col("rbs_id"), functions.col("node_code")))
                .withColumn("type", functions.lit("ocw/eNodeB"))
                .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                        t.col("rbs_id").alias("id"), t.col("virtual_rnc"), // # t.site_name,
                        t.col("status")
// #t.address_id,
// #t.nodeb_allocation_id.alias("tempestId")# , t.du_number
                )
// # .where("myDu is null")
// # .where(t.node_code == "AADP')
// # .show(200, truncate = False)
// # .printSchema()
        );
    }

    public Dataset transform5GNGRAN(Dataset t) {
        System.out.println("transform5GLRAN =======================>");
        return (t.where(t.col("network").isin("5G (NGRAN)").and(t.col("gnb_id").isNotNull()))
                .withColumn("name", userDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"), functions.col("gnb_id"), functions.col("node_code")))
                .withColumn("type", functions.lit("ocw/gnbdu"))
                .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                        t.col("gnb_id").alias("id"), t.col("virtual_rnc"), // # t.site_name,
                        t.col("status")

                )
        );
    }

    public Dataset transform3GWRAN(Dataset t) {
        System.out.println("transform3GLRAN =======================>");
        return (t.where(t.col("network").isin("3G (WRAN)").and(t.col("rbs_id").isNotNull()))
                .withColumn("name", userDefinedFunctions.eaiNodeBName.apply(functions.col("site_name"), functions.col("node_code")))
                .withColumn("type", functions.lit("ocw/nodeB"))
                .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                        t.col("rbs_id").alias("id"), t.col("virtual_rnc"),// # t.site_name,
                        t.col("status")

                )
        );
    }

    public Dataset transformnbe(Dataset nb_e) {
        System.out.println("transformnbe method return nbe =======================>");
        return (nb_e
                .withColumn("name", userDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("id", userDefinedFunctions.eaiIdFromMecontext.apply(functions.col("mecontext")))
                .withColumn("type", functions.lit("ocw/nodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
    }

    public Dataset transformenbE(Dataset enb_e) {
        return (enb_e
                .withColumn("name", userDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("type", functions.lit("ocw/eNodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
    }

    public Dataset transformGnbdE(Dataset gnbd_e) {
        return (gnbd_e
                .withColumn("name", userDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .select(functions.col("name"), functions.col("id"), gnbd_e.col("mecontext"))
                .where(functions.col("id").isNotNull()));
    }

    public Dataset transformGnbE(Dataset gnb_e) {
        return (gnb_e
                .withColumn("type", userDefinedFunctions.eaiEnmGnbType.apply(functions.col("mecontext")))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
        );
    }
    public Dataset generateEnm(Dataset gnbd_e,Dataset nb_e,Dataset enb_e) {
        Dataset gnb_e = gnbd_e;
        gnb_e = gnb_e.distinct();
        gnb_e = transformGnbE(gnb_e);
        Dataset enm = nb_e;



        enm = enm.union(enb_e);

        return enm.union(gnb_e);
    }

    public Dataset transformBtsToGsmCell(Dataset q) {
        return (q
                .where((q.col("technology").like("GSM%")).and(q.col("rru_donor_node").isin(null, "remote", "neither")))
                .select(q.col("cell_name"), q.col("cell_name").substr(1, 4).alias("btsName"))
// #.withColumn('btsId', eaiInt(F.col('iub_rbsid')))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$type", functions.lit("ocw/gsmCell"))
                .withColumn("$bts", functions.array(functions.col("btsName")))
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"), functions.col("$action"),
                        q.col("cell_name").alias("name"), functions.col("$bts"))
        );
    }

    public Dataset transformBtsToGsmCelLoopUP(Dataset bts) {
        return (bts
                .select("$type", "$refId", "name")
                .withColumn("$action", functions.lit("lookup"))
                .select("$type", "$refId", "$action", "name")
                .distinct()
        );
    }


    public Dataset transformrfCellToRepeaterLookUp(Dataset q){
        return (q.where((q.col("rru_donor_node").isin("repeater")).and(q.col("active_repeater_donor_node").isNotNull()))
                .withColumn("$type", userDefinedFunctions.eaiTechnologyToType.apply(functions.col("technology")))
                .withColumn("$action", functions.lit("lookup"))
                .select(functions.col("$type"), functions.col("$action"),
                        q.col("active_repeater_donor_node").alias("$refId"),
                        q.col("active_repeater_donor_node").alias("name")
                )
                .distinct()
        );
    }

    public Dataset transformrfCellToRepeater(Dataset q){
        return (q.where((q.col("rru_donor_node").isin("repeater")).and(q.col("active_repeater_donor_node").isNotNull()))
                .withColumn("$type", functions.lit("ocw/repeater"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$refCell", functions.array(functions.col("active_repeater_donor_node")))
                .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                        q.col("cell_name").alias("name"), functions.col("$refCell")
                )
                .distinct()
        );
    }

    public Dataset transformMbs(Dataset mbs) {
        return (mbs
                .select(mbs.col("id"), mbs.col("name"), mbs.col("type"), mbs.col("status").alias("tmp"), mbs.col("nodeCode"))
                .withColumn("status", userDefinedFunctions.eaiStatus.apply(functions.col("tmp")))
                .withColumn("$refId", functions.col("name"))
                .withColumn("$type", functions.col("type"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id"), mbs.col("name"), mbs.col("type"), functions.col("status")
                        , mbs.col("nodeCode"))
        );
    }

    public Dataset transformBsToB3(Dataset bs) {
        return bs.select(bs.col("name").alias("bsname"), bs.col("id"), bs.col("type").alias("bstype"),
                bs.col("status").alias("bsstatus"), bs.col("node_code").alias("bsnodeCode"));
    }

    public Dataset transformenmToMbs(Dataset enm,Dataset b3,Dataset bs) {

// # For all of the id's that match ENM and B3, keep the ENM version
        Dataset tmp1 = enm.join(b3, enm.col("id").equalTo(b3.col("id")), "left_outer")
                .select(b3.col("id"), enm.col("name"), enm.col("type"), enm.col("status"), enm.col("nodeCode")); // #for this join get the ENM side

//# get the remaining records that are in BS but not in ENM
        Dataset tmp3 = bs.join(enm, bs.col("id").equalTo(enm.col("id")),"left_anti")
                .select(bs.col("id"), bs.col("name"), bs.col("type"), bs.col("status"),
                        bs.col("node_code").alias("nodeCode"));
        return tmp1.union(tmp3);
    }

    public Dataset joinBsAndn(Dataset b2, Dataset n) {
        return (b2
                .join(n, (b2.col("name").equalTo(n.col("name"))), "inner")
                .select(b2.col("name"), b2.col("type"), b2.col("node_code"), b2.col("id"), b2.col("virtual_rnc"),
                        b2.col("status")) // # select the bbh side
                .union(b2.join(n, b2.col("name").equalTo(n.col("name")), "left_anti"))
                .union(n.join(b2, n.col("name").equalTo(b2.col("name")), "left_anti")) // # psudo right_anti
                .where(functions.col("name").isNotNull())
                .distinct());
    }


    public Dataset nodeBToUmtsCellLookUp(Dataset nodeB) {
        return (
                nodeB.select(functions.col("$type"), nodeB.col("nodeBId").alias("$refId"), nodeB.col("name"))
                        .withColumn("$action", functions.lit("lookup"))
                        .distinct()
        );
    }

    public Dataset nodeBToUmtsCell(Dataset q) {
        return (q.where((q.col("iub_rbsid").isNotNull()).and
                        (q.col("technology").like("WCDMA%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("nodeBId", userDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid"))).where(functions.col("nodeBId").isNotNull()) // # filter out any integer conversion errors
                .withColumn("$nodeB", functions.array(userDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid"))))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$type", functions.lit("ocw/umtsCell"))
                .select(functions.col("$type"), functions.col("$action"),
                        q.col("cell_name").alias("$refId"), q.col("cell_name").alias("name"), functions.col("$nodeB"))
        );
    }

    public Dataset eNodeBToLteCellLookup(Dataset eNodeB) {
        return (
                eNodeB
                        .select(functions.col("$type"),
                                eNodeB.col("eNodeBId").alias("$refId").cast(DataTypes.StringType), eNodeB.col("name"))
                        .withColumn("$action", functions.lit("lookup"))
                        .distinct()
        );
    }

    public Dataset eNodeBToLteCell(Dataset q) {
        return (
                q.where((q.col("enbid_dec").isNotNull()).and(q.col("technology").like("LTE%")).
                                and(q.col("rru_donor_node").isin("remote", "neither", "rruDonor")))
                        .withColumn("eNodeBId", userDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")))
                        .where(functions.col("eNodeBId").isNotNull()) //# filter out any integer conversion errors
                        .withColumn("$eNodeB", functions.array(userDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")).cast(DataTypes.StringType))) //# Converting this BACK to string as I think the refid (above) really likes a string...FFS
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$type", functions.lit("ocw/lteCell"))
                        .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                                q.col("cell_name").alias("name"), functions.col("$eNodeB"))
        );
    }

    public Dataset generateC2n(Dataset q) {
        return (q.where((q.col("enbid_dec").isNotNull()).and(q.col("technology").like("NR%"))
                        .and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("gNodeBId", userDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")))
                .select(functions.col("gNodeBId"), q.col("cell_name"), q.col("technology"), q.col("cell_status"))
                .where(functions.col("gNodeBId").isNotNull())
        );
    }

    public Dataset nrCellsToGnbdu(Dataset c2n, Dataset mbs) {
        return (c2n
                .join(mbs, c2n.col("gNodeBId").equalTo(mbs.col("id")), "leftouter")
                .where(functions.col("type").equalTo("ocw/gNB-DU"))
                .select(c2n.col("gNodeBId").alias("gnbduId"),
                        functions.col("cell_name"))
//# .show(100)
//# .coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCells_to_gnbdu/csv', mode='overwrite', header=True)
//# .write.json(path='s3://emrdisco/eai_objects/nrCells_to_gnbdu', mode='overwrite')
        );
    }

    public Dataset nrCellsToGnbcuup(Dataset c2n, Dataset mbs) {
        return (c2n
                .join(mbs, c2n.col("gNodeBId").equalTo(mbs.col("id")), "leftouter")
                .where(functions.col("type").equalTo("ocw/gNB-CU-UP"))
                .select(c2n.col("gNodeBId").alias("gnbcuupId"),
                        functions.col("cell_name"))
//# .show(100)
//# .coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCells_to_gnbcuup/csv', mode='overwrite', header=True)
//# .write.json(path='s3://emrdisco/eai_objects/nrCells_to_gnbcuup', mode='overwrite')
        );
    }

    public Dataset nrCellsToGnbduLookup(Dataset c2n, Dataset gNBDU) {
        return (
                c2n
                        .join(gNBDU, (c2n.col("gNodeBId").equalTo(gNBDU.col("gnbduId"))), "inner") // # just get the gNodeB's that we need, alos to get the gNodeB's name
                        .withColumn("$type", functions.lit("ocw/gnbdu"))
                        .withColumn("$action", functions.lit("lookup"))
                        .select(
                                functions.col("$type"), functions.col("$action"),
                                c2n.col("gNodeBId").alias("$refId"), gNBDU.col("name")
                        )
                        .distinct()
        );
    }

    public Dataset nrCellsToGnbdu2(Dataset c2n, Dataset gNBDU) {
        return (
                c2n
                        .withColumn("$type", functions.lit("ocw/nrCell"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$gnbdu", functions.array(functions.col("gNodeBId")))
                        .select(
                                functions.col("$type"), functions.col("$action"),
                                c2n.col("cell_name").alias("$refId"), c2n.col("cell_name").alias("name"),
                                functions.col("$gnbdu")
                        )
                        .distinct()
        );
    }

    public Dataset wirelessNetwork(Dataset q) {
        return (q
                .where(q.col("plmn").isNotNull())
                .where(q.col("plmn").notEqual(99999))
                .withColumn("$type", functions.lit("ocw/wirelessNetwork"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("country", functions.lit("Australia"))
                .withColumn("mcc", userDefinedFunctions.eaiMcc.apply(functions.col("plmn")))
                .withColumn("mnc", userDefinedFunctions.eaiMnc.apply(functions.col("plmn")))
                .withColumn("operatorName", functions.lit("Telstra"))
                .withColumn("status", functions.lit("ACTIVE"))
                .select(functions.col("$type"), functions.col("$action"),
                        q.col("plmn").alias("$refId"), q.col("plmn").alias("name"), functions.col("status"),
                        functions.col("country"), functions.col("mcc"), functions.col("mnc"), functions.col("operatorName"))
                .distinct()
                .orderBy(q.col("plmn"))
        );
    }

    public Dataset nrCells_to_wirelessNetwork(Dataset q) {
        return (
                q
                        .where(q.col("technology").like("NR%")
                                .and(q.col("rru_donor_node").isin("remote", "neither")))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("$type", functions.lit("ocw/nrCell"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$wirelessNetworks", functions.array(functions.col("plmn")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("cell_name").alias("$refId"),
                                q.col("cell_name").alias("name"), functions.col("$wirelessNetworks"))
        );
    }

    public Dataset nrCellsToWirelessNetworkLookup(Dataset q) {
        return (
                q
                        .where(q.col("technology").like("NR%").and(q.col("rru_donor_node").isin("remote", "neither")))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("$type", functions.lit("ocw/wirelessNetwork"))
                        .withColumn("$action", functions.lit("lookup"))
// #.withColumn("$nrCells", functions.array(functions.col("cell_name")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("plmn").alias("$refId"),
                                q.col("plmn").alias("name") //#,"$nrCells"
                        )
                        .distinct()
        );
    }

    public Dataset lteCellToWirelessNetworkLookup(Dataset q) {
        return (
                q .where(q.col("technology").like("LTE%").and(q.col("rru_donor_node").isin("remote", "neither")))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("$type", functions.lit("ocw/wirelessNetwork"))
                        .withColumn("$action", functions.lit("lookup"))
// #.withColumn("$nrCells",functions.array(F.col("cell_name")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("plmn").alias("$refId"),
                                q.col("plmn").alias("name")//#,"$nrCells"
                        )
                        .distinct()
        );
    }

    public Dataset lteCellToWirelessNetwork(Dataset q) {
        return (q
                .where(q.col("technology").like("LTE%").and(q.col("rru_donor_node").isin("remote", "neither")))
                .where(q.col("plmn").isNotNull())
                .withColumn("$wireless_type", functions.lit("ocw/lteCell"))
                .withColumn("$wireless_action", functions.lit("createOrUpdate"))
                .withColumn("$wirelessNetwork", functions.array(q.col("plmn")))
                .select(functions.col("$wireless_type").alias("$type"),
                        functions.col("$wireless_action").alias("$action"),
                        q.col("cell_name").alias("$refId"),
                        q.col("cell_name"),
                        functions.col("$wirelessNetwork"))
                .distinct()
        );
    }

    public Dataset rncToNodeBLookup(Dataset q,Dataset nodeB) {
        return (
                q.where(q.col("technology").like("WCDMA%").and(q.col("rru_donor_node").isin("remote", "neither")))
                        .withColumn("qNBId", userDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid")))
                        .join(nodeB, (functions.col("qNBId").equalTo(nodeB.col("nodeBId"))), "inner")
                        .withColumn("$type", functions.lit("ocw/rnc"))
                        .withColumn("$action", functions.lit("lookup"))
                        .where(q.col("bsc_rnc_node").isNotNull())
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("bsc_rnc_node").alias("$refId"), q.col("bsc_rnc_node").alias("name")
                        )
                        .distinct()
        );
    }

    public Dataset rncToNodeB(Dataset q,Dataset nodeB) {
        return (
                q.where(q.col("technology").like("WCDMA%")
                                .and(q.col("rru_donor_node").isin("remote", "neither")))
                        .withColumn("qNBId", userDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid")))
                        .join(nodeB, (functions.col("qNBId").equalTo(nodeB.col("nodeBId"))), "inner")
                        .where(q.col("bsc_rnc_node").isNotNull())
                        .withColumn("$type", functions.lit("ocw/nodeB"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$rnc", functions.array(q.col("bsc_rnc_node")))
                        .select(functions.col("$type"), functions.col("$action"),
                                nodeB.col("name").alias("$refId"), nodeB.col("name"), functions.col("$rnc")
                        )
                        .distinct()
        );
    }
    public Integer getMcc(String plmn) {
        if(plmn==null)
        {
            return null;
        }else{
            try{
                return Integer.parseInt(plmn.trim().substring(0,3));
            }catch (Exception e){
//logger.warn("Error extracting MCC from PLMN:"+plmn);
                return null;
            }
        }
    }
    public String getMnc(String plmn) {
        if (plmn == null) {
            return null;
        } else {
            try {
                return plmn.trim().substring(3, 5);
            } catch (Exception e) {
// logger.warn("Error extracting MNC from PLMN:" + plmn);
                return null;
            }
        }
    }
}