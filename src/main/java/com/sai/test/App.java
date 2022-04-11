package com.sai.test;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;


import com.sai.test.config.ClusterConfig;
import com.sai.test.config.Constants;
import com.sai.test.config.DbDetails;
import com.sai.test.config.Schemas;
import com.sai.test.utils.SecretUtils;
import com.sai.test.utils.UdmfUtilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Base64;

public class App {
public static void main(String[] args) throws JsonProcessingException {
SecretUtils secretUtils=new SecretUtils();
ObjectMapper mapper = new ObjectMapper();
DbDetails dbDetails = mapper.readValue(secretUtils.getSecret(), DbDetails.class);

System.out.println("db_nam=> "+dbDetails.getDbName());
System.out.println("db_address=> "+dbDetails.getDbAddress());

String url =dbDetails.getDbAddress()+":"+dbDetails.getDbPort()+"/"+ dbDetails.getDbName();
//"jdbc:postgresql://vpce-0fe0813c6dd8e577d-u6k3huep.vpce-svc-0d2e537b5fdbaa0c4.ap-southeast-2.vpce.amazonaws.com:54376/umt1rodsdb";
// String url = "jdbc:postgresql://127.0.0.1:54376/umt1rodsdb";
String user = dbDetails.getDbUsername();
String password =dbDetails.getDbPassword();

Logger.getLogger("org").setLevel(Level.ERROR);
SparkSession session = ClusterConfig.getSparkSession();
UdmfUtilities udmfUtils = new UdmfUtilities();

// Dataset quollDF = udmfUtils.readFile(session, null, "C:\\gitRepoProjects\\udmf-data-migration-service\\src\\main\\resources\\input\\mndb_mn_cell_table_full.csv");
// byte[] decodedBytes = Base64.getDecoder().decode(args[2]);
// String password = new String(decodedBytes);

System.out.println("Connecting to PostgresSQL DB ===> ");

Dataset quollDF = session.read()
.format("jdbc")
.option("url", url)
// .option("query", "select * from umt1rods.mndb_mn_cell limit 10")
.option("dbtable", "umt1rods.mndb_mn_cell")
.option("user", user)
.option("password", password)
.load();

System.out.println("quolldf loaded from PostgresSQL DB ====> data set");

Dataset quollTransformedDF = udmfUtils.applyInitialTransformaions(quollDF);
System.out.println(" =====> quollTransformedDF --> transformed in applyInitialTransforms =====>");
// Load the TEMPEST Node ID Allocation data.
Dataset tempestDF = udmfUtils.readFile(session, Schemas.nodeIdSchema, Constants.TEMPEST_NODE_ID_PATH);
System.out.println("load the TEMPEST_NODE_ID_PATH look up data in t");
//Load bbh spread sheet
Dataset bbhDF = udmfUtils.readFile(session, Schemas.bbhSpreadsheetSchema, Constants.BBH_SPREAD_SHEETS_PATH);
quollTransformedDF = udmfUtils.addAdditionalAttributes(quollTransformedDF);
System.out.println("quollTransformedDF added with additional Attributes --> quollTransformedDF =====> ");
// quollTransformedDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "quollTransformedDF");

/* This will generate a simple list of sites with minimal information based on the location manager ID for each site in Quoll.
These will NOT be node codes!
Assumptions:
- The Location is in an active status. In reality we will need to ge this from Location Manager.*/

Dataset sitesDF = udmfUtils.cleanlyConvertssitesToInteger(quollTransformedDF);
sitesDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site");
System.out.println("writing sitesDF to json======> ");

Dataset bscDF = udmfUtils.transformBsc(quollTransformedDF);
bscDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");
System.out.println("writing bscDF to json ======> ");

Dataset rncDF = udmfUtils.transformRnc(quollTransformedDF);
rncDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rnc");
System.out.println("writing rncDF to json ======> ");

Dataset btsDF = udmfUtils.transfromBts(quollTransformedDF);
btsDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts");
System.out.println("writing btsDF to json ======> ");;

Dataset bsc_to_bts_lookupDF = udmfUtils.transfromBscToBtsLookup(quollTransformedDF);
bsc_to_bts_lookupDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts_lookup");
System.out.println("writing bsc_to_bts_lookupDF to json ======> ");

Dataset lteDF = udmfUtils.transformLte(quollTransformedDF);
lteDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "lteCell");
System.out.println("writing lteCell to json ======> ");

Dataset gsmDF = udmfUtils.transformGsm(quollTransformedDF);
gsmDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gsmCell");
System.out.println("writing gsmCell to json ======> ");

Dataset umtsDF = udmfUtils.transformUmts(quollTransformedDF);
umtsDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "umtsCell");
System.out.println("writing umtsCell to json ======> ");
//umts.coalesce(1).write.csv(path='s3://emrdisco/eai_objects/umtsCell/csv', mode='overwrite', header=True, quoteAll=True)

Dataset nrDF = udmfUtils.transformNr(quollTransformedDF);
nrDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCell");
System.out.println("writing nrCell to json ======> ");

Dataset repeaterDF = udmfUtils.transformRepeater(quollTransformedDF);
repeaterDF.write().mode("overwrite").json(Constants.bucketUrl+Constants.bucketOutputPath + "repeater");
System.out.println("Writing repeater to json ===> ");

// now get the list of the bts's that will link to them
Dataset bsc_to_btsDF = udmfUtils.transfromBscToBts(quollTransformedDF);
bsc_to_btsDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts");
System.out.println("writing bsc_to_btsDF to json ======> ");

Dataset rfCellToRepeaterLookUpDF = udmfUtils.transformrfCellToRepeaterLookUp(quollTransformedDF);
rfCellToRepeaterLookUpDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rfCell_to_repeater_lookUp");

Dataset rfCellToRepeaterDF = udmfUtils.transformrfCellToRepeater(quollTransformedDF);
rfCellToRepeaterDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rfCell_to_repeater");

//nr.show()
//nr.coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCell/csv', mode='overwrite', header=True, quoteAll=True)

Dataset site_to_rfCell_lookupDF = udmfUtils.transformSiteToRfCellLookUp(quollTransformedDF);
site_to_rfCell_lookupDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath +"site_to_rfCell_lookup");
System.out.println("writing site_to_rfCell_lookup to json ======> ");

Dataset site_to_rfCellDF = udmfUtils.transformSiteToRfCell(quollTransformedDF);
site_to_rfCellDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site_to_rfCell");
System.out.println("writing site_to_rfCell to json ======> ");

Dataset bsDF = udmfUtils.generateBsDataset(tempestDF,bbhDF);



// n.count = 42749
// b.count = 5698
//n.count()
//print(bs.count()) 44425
//bs.select('type').distinct().show()
//bs.orderBy(bs.name).coalesce(1).write.csv(path='s3://emrdisco/eai_objects/baseStation/csv', mode='overwrite', header=True, quoteAll=True)
//bs.write.json(path='s3://emrdisco/eai_objects/baseStation', mode='overwrite')
    Dataset nb_e = session.read().option("header", "true")
            .schema(Schemas.enmNodeBSchema)
            .csv(Constants.enm_nodeB_PATH);


    nb_e =udmfUtils.transformnbe(nb_e);

    Dataset enb_e = session.read().schema(Schemas.enmBaseStationSchema)
            .option("header", "true").csv(Constants.enm_nodeBS_PATH);

    enb_e =udmfUtils.transformenbE(enb_e);
    Dataset gnbd_e = session.read()
            .option("header", "true")
            .schema(Schemas.enmBaseStationSchema)
            .csv(Constants.GNODEB_DU);

    gnbd_e = udmfUtils.transformGnbdE(gnbd_e);

    Dataset gnb_e = gnbd_e;
    gnb_e = gnb_e.distinct();
    gnb_e = udmfUtils.transformGnbE(gnb_e);

    Dataset enm = udmfUtils.generateEnm(gnbd_e,nb_e,enb_e);

/* enm = enm.union(enb_e);
enm = enm.union(gnb_e);*/
// enm = enm.union(bts.select(bts.name, bts.btsId.alias('id'), bts.type, bts.status, bts.name.alias('nodeCode')))
    Dataset b3 = udmfUtils.transformBsToB3(bsDF);
    Dataset mbs =udmfUtils.transformenmToMbs(enm,b3,bsDF);

//mbs.orderBy(mbs.name).show()

// convert all the statuses from Tempest, BBH XLSX and ENM into valid ENM statuses values
// TPD-1275 and TPD-1328

    mbs = udmfUtils.transformMbs(mbs);

// bs gives a consolidated list of base stations from TEMPEST and BBH XLSX
// we now need to split these up again so that we can add in type specific fields
// and also tweak the fields we display

    Dataset nodeB = mbs.where(mbs.col("type").equalTo("ocw/nodeB"))
            .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                    mbs.col("id").alias("nodeBId"), mbs.col("name"), mbs.col("status"));

    Dataset eNodeB = mbs.where(mbs.col("type").equalTo("ocw/eNodeB"))
            .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                    mbs.col("id").alias("eNodeBId"), mbs.col("name"), mbs.col("status"));

    Dataset gNBDU = mbs.where(mbs.col("type").equalTo("ocw/gnbdu")).select(functions.col("$type"), functions.col("$refId"),
            functions.col("$action"), mbs.col("id").alias("gnbduId"), mbs.col("name"), mbs.col("status"));

//gNBCUUP = mbs.where(mbs.type == 'ocw/gnbcuup').select('$type', '$refId', '$action', mbs.id.alias('gnbcuupId'), mbs.name, mbs.status)
//
//
//print(nodeB.count()) 14706
//print(eNodeB.count()) 25490
//print(gNBDU.count()) 1063
//print(gNBCUUP.count()) 3162
// nodeB.show();
    nodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB");
    System.out.println("writing nodeB to json ======> ");
    eNodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB");
    System.out.println("writing eNodeB to json ======> ");
    gNBDU.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gNB_DU");
    System.out.println("writing gNB_DU to json ======> ");

//gNBCUUP.write.json(path=bucketUrl + bucketOutputPath + 'gNB-CU-UP', mode='overwrite')



    Dataset bts_to_gsmCell_lookup =udmfUtils.transformBtsToGsmCelLoopUP(btsDF);
//bts_to_gsmCell_lookup.show()
    bts_to_gsmCell_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell_lookup");
    System.out.println("writing bts_to_gsmCell_lookup to json ======> ");

// backup : (q.iub_rbsid.isNotNull()) & (q.technology.like('GSM%')) & (q.cell_status != 'Erroneous entry')
    Dataset bts_to_gsmCell = udmfUtils.transformBtsToGsmCell(quollTransformedDF);
    bts_to_gsmCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell");
    System.out.println("writing bts_to_gsmCell to json ======> ");

    Dataset nodeB_to_umtsCell_lookup = udmfUtils.nodeBToUmtsCellLookUp(nodeB);
// nodeB_to_umtsCell_lookup.show()
    nodeB_to_umtsCell_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB_to_umtsCell_lookup");
    System.out.println("writing nodeB_to_umtsCell_lookup to json ======> ");

    Dataset nodeB_to_umtsCell =udmfUtils.nodeBToUmtsCell(quollTransformedDF);
    nodeB_to_umtsCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB_to_umtsCell");
    System.out.println("writing nodeB_to_umtsCell to json ======> ");
    Dataset eNodeB_to_lteCell_lookup = udmfUtils.eNodeBToLteCellLookup(eNodeB);

//#eNodeB_to_lteCell_lookup.show()
    eNodeB_to_lteCell_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB_to_lteCell_lookup");
    System.out.println("writing eNodeB_to_lteCell_lookup to json ======> ");
    Dataset eNodeB_to_lteCell = udmfUtils.eNodeBToLteCell(quollTransformedDF);
//#eNodeB_to_lteCell.show()
    eNodeB_to_lteCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB_to_lteCell");
    System.out.println("writing eNodeB_to_lteCell to json ======> ");
//# create a list of all of the nrCell to node relationships
    Dataset c2n =udmfUtils.generateC2n(quollTransformedDF);

//# now join these with the list of base staions so that we can determine if they are du or cu
//#nrCells_to_gnbdu
    Dataset nrCells_to_gnbdu =udmfUtils.nrCellsToGnbdu(c2n,mbs);
//#nrCells_to_gnbcuup
    Dataset nrCells_to_gnbcuup =udmfUtils.nrCellsToGnbcuup(c2n,mbs);
//# Lookup the gNodB-DU objects
    Dataset nrCells_to_gnbdu_lookup = udmfUtils.nrCellsToGnbduLookup(c2n,gNBDU);
//#nrCells_to_gnbdu_lookup.show()
    nrCells_to_gnbdu_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_gnbdu_lookup");
    System.out.println("writing nrCells_to_gnbdu_lookup to json ======> ");
//#nrCells_to_gnbdu.show()
    nrCells_to_gnbdu.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_gnbdu");
    Dataset wirelessNetwork = udmfUtils.wirelessNetwork(quollTransformedDF);
//#wirelessNetwork.show()
    wirelessNetwork.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "wirelessNetwork");
    System.out.println("writing wirelessNetwork to json ======> ");
    Dataset nrCells_to_wirelessNetwork = udmfUtils.nrCells_to_wirelessNetwork(quollTransformedDF);
//#nrCells_to_wirelessNetwork.show()
    nrCells_to_wirelessNetwork.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_wirelessNetwork");
    System.out.println("writing nrCells_to_wirelessNetwork to json ======> ");
    Dataset nrCells_to_wirelessNetwork_lookup = udmfUtils.nrCellsToWirelessNetworkLookup(quollTransformedDF);
//#nrCells_to_wirelessNetwork_lookup.show()
    nrCells_to_wirelessNetwork_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_wirelessNetwork_lookup");
    System.out.println("writing nrCells_to_wirelessNetwork_lookup to json ======> ");
//# lookup RNC
    Dataset rnc_to_nodeB_lookup =udmfUtils.rncToNodeBLookup(quollTransformedDF,nodeB);
//#rnc_to_nodeB_lookup.show()
    rnc_to_nodeB_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rnc_to_nodeB_lookup");
    System.out.println("writing rnc_to_nodeB_lookup to json ======> ");
//# update nodeB ($rnc)
    Dataset rnc_to_nodeB =udmfUtils.rncToNodeB(quollTransformedDF,nodeB);
//#rnc_to_nodeB.show()
    rnc_to_nodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rnc_to_nodeB");
    System.out.println("writing rnc_to_nodeB to json ======> ");
    Dataset designerDF=udmfUtils.tranformDesigner(quollTransformedDF);
    designerDF.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "designer");
    System.out.println("writing designerDF to json ======> ");

// NNI-2801
    Dataset lTECellTo_wirelessNetworkLookup = udmfUtils.lteCellToWirelessNetworkLookup(quollTransformedDF);
    lTECellTo_wirelessNetworkLookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "lteCell_to_wirelessNetwork_Lookup");
    System.out.println("writing lTECellTo_wirelessNetworkLookup to json ======> ");
    Dataset lTECellToWirelessNetwork = udmfUtils.lteCellToWirelessNetwork(quollTransformedDF);
    lTECellToWirelessNetwork.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "lteCell_To_WirelessNetwork");
    System.out.println("writing lTECellToWirelessNetwork to json ======> ");

}

}