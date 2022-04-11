package com.sai.test;

import com.sai.test.config.Schemas;
import com.sai.test.utils.UdmfUtilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class UdmfQuollUnitTests {
SparkSession session ;
UdmfUtilities quollUtils;
Dataset testDataset;
Dataset tespest;
Dataset nb_e;

@BeforeEach
void setUp() {
session = SparkSession.builder().appName("QuollTransformationsTest")
.master("local[1]")
.getOrCreate();
quollUtils = new UdmfUtilities();
//testDataset = quollUtils.readFile(session, null, "src/test/resources/input/mndb_mn_cell_sample.csv");
//tespest = quollUtils.readFile(session, Schemas.nodeIdSchema, "src/test/resources/input/tempest.csv");
//nb_e = quollUtils.readFile(session, Schemas.enmNodeBSchema, "src/test/resources/input/testdata2.csv");

}

@AfterEach
void tearDown() {
session.stop();
}

@Test
void readFile() {
Dataset q = quollUtils.readFile(session, null, "src/test/resources/input/mndb_mn_cell_sample.csv");
assertEquals(7, q.count());
Dataset t = quollUtils.readFile(session, Schemas.nodeIdSchema, "src/test/resources/input/tempest.csv");
assertEquals(3, t.count());

}

@Test
void applyInitialTransformaions() {
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
assertEquals(6, q.count());

}

@Test
void cleanlyConvertssitesToInteger() {
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset sites = quollUtils.cleanlyConvertssitesToInteger(q);
assertEquals(5, sites.count());
}
@Test
void transformBsc(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset bscdata = quollUtils.transformBsc(q);
assertEquals(1, bscdata.count());
}
@Test
public void transformRnc(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset racData = quollUtils.transformRnc(q);
assertEquals(1, racData.count());
}
@Test
void transfromBts(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset bscdata = quollUtils.transfromBts(q);
assertEquals(3, bscdata.count());
}
@Test
void transfromBscToBtsLookup(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset btsLookup = quollUtils.transfromBscToBtsLookup(q);
assertEquals(1, btsLookup.count());
}
@Test
void transfromBscToBts(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset btsLookup = quollUtils.transfromBscToBts(q);
assertEquals(3, btsLookup.count());
}

@Test
void transformLte() {
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset lteDs = quollUtils.transformLte(q);
assertEquals(1, lteDs.count());
}

@Test
void transformrfCellToRepeaterLookUp(){
Dataset q1 = session.read()
.option("header", "true")
.csv("src/test/resources/input/repeaterTestData.csv");
assertEquals(3,quollUtils.transformrfCellToRepeaterLookUp(q1).count());

}
@Test
void transformrfCellToRepeater(){
Dataset q1 = session.read()
.option("header", "true")
.csv("src/test/resources/input/repeaterTestData.csv");
assertEquals(3,quollUtils.transformrfCellToRepeater(q1).count());
}

@Test
void transformGsm() {// TODO not done
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset gsmDs = quollUtils.transformGsm(q);
assertEquals(3, gsmDs.count());
}
@Test
void transformUmts(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset umtsDs = quollUtils.transformUmts(q);
assertEquals(1, umtsDs.count());
}

@Test
void transformNr(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset nrDs = quollUtils.transformNr(q);
assertEquals(1, nrDs.count());
}

@Test
public void transformRepeater() {
Dataset q = session.read()
.option("header", "true")
.csv("src/test/resources/input/repeaterTestData2.csv");
assertEquals(3,quollUtils.transformRepeater(q).count());
}
@Test
void transformSiteToRfCellLookUp(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset sitesToRfCellLookupDs = quollUtils.transformSiteToRfCellLookUp(q);
assertEquals(5, sitesToRfCellLookupDs.count());
}

@Test
void transformSiteToRfCell(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset sitesToRfCellDs = quollUtils.transformSiteToRfCell(q);
assertEquals(6, sitesToRfCellDs.count());
}

@Test
void transform4GLRAN(){
Dataset tempestDataset = quollUtils.transform4GLRAN(tespest);
assertEquals(1, tempestDataset.count());

}
@Test
void transform5GNGRAN() {
Dataset transformed5GDataset = quollUtils.transform5GNGRAN(tespest);
assertEquals(1, transformed5GDataset.count());
}
@Test
void transformnbe(){

Dataset transformedmnbe = quollUtils.transformnbe(nb_e);

assertEquals(4, transformedmnbe.count());
}
@Test
void transformenbE(){
Dataset enb_e = session.read().schema(Schemas.enmBaseStationSchema)
.option("header", "true").csv("src/test/resources/input/enm/enm_nodeB.csv");
assertEquals(0,quollUtils.transformenbE(enb_e).count());
}
@Test
void transformGnbdE(){
Dataset gnbd_e = session.read()
.option("header", "true")
.schema(Schemas.enmBaseStationSchema)
.csv("src/test/resources/input/enm/enm_gNodeB-DU.csv");

assertEquals(4,quollUtils.transformGnbdE(gnbd_e).count());
}
@Test
void transformGnbE(){
Dataset gnbd_e = session.read()
.option("header", "true")
.schema(Schemas.enmBaseStationSchema)
.csv("src/test/resources/input/enm/enm_gNodeB-DU.csv");
gnbd_e=quollUtils.transformGnbdE(gnbd_e);
assertEquals(4,quollUtils.transformGnbE(gnbd_e).count());

}
@Test
public void transformBtsToGsmCell(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
q = quollUtils.addAdditionalAttributes(q);
Dataset gsmBTsCell = quollUtils.transformBtsToGsmCell(q);
assertEquals(3, gsmBTsCell.count());
}

@Test
void transformBtsToGsmCelLoopUP(){
Dataset q = quollUtils.applyInitialTransformaions(testDataset);
Dataset bts = quollUtils.transfromBts(q);
Dataset brsGsm = quollUtils.transformBtsToGsmCelLoopUP(bts);
assertEquals(3, brsGsm.count());
}
@Test
public void transformMbs() {
Dataset mbsTest = quollUtils.readFile(session,null,"src/test/resources/input/mbsTestData2.csv");
Dataset mbs = quollUtils.transformMbs(mbsTest);
assertEquals(3, mbs.count());

}
@Test
void transform3GWRAN(){
// tespest.show();
Dataset transformed5GDataset = quollUtils.transform3GWRAN(tespest);
assertEquals(1, transformed5GDataset.count());
}
@Test
void genSectorNumber() {
assertEquals(1,UdmfUtilities.genSectorNumber("1"));
assertEquals(null,UdmfUtilities.genSectorNumber(" "));
assertEquals(null,UdmfUtilities.genSectorNumber(" b "));
assertEquals(null,UdmfUtilities.genSectorNumber(null));

}


//@Test
//void mapCellStatus() {
//assertEquals("PENDING DECOMMISSION",UdmfUtilities.mapCellStatus("Inactive"));
//assertEquals("IN CONCEPT",UdmfUtilities.mapCellStatus("xyz"));
//assertEquals("IN CONCEPT",UdmfUtilities.mapCellStatus("Concept"));
//
//}
//
//@Test
//void mapStatus() {
//assertEquals("PENDING",UdmfUtilities.mapStatus("Commissioning"));
//assertEquals("PENDING",UdmfUtilities.mapStatus("xyz"));
//assertEquals("PENDING DECOMMISSION",UdmfUtilities.mapStatus("Pending Delete"));
//
//}
//
//@Test
//void mapCellType() {
//assertEquals("MACRO",UdmfUtilities.mapCellType("xyz"));
//assertEquals("MACRO",UdmfUtilities.mapCellType("TBA"));
//assertEquals("IN BUILDING CELL",UdmfUtilities.mapCellType("IBC"));
//
//}


@Test
void mapCellFunction() {
assertEquals("Coverage",quollUtils.mapCellFunction("1"));
assertEquals(null,quollUtils.mapCellFunction("1a"));
}

@Test
void cleanRac() {
assertEquals(1,quollUtils.cleanRac("1"));

}

@Test
void validateMscNode() {
assertEquals("NSW-ACT-HSCPOOL",quollUtils.validateMscNode("NSW-ACT-HSCPOOL"));

}

@Test
void cleanBool() {
assertTrue(quollUtils.cleanBool("TRUE"));
assertTrue(quollUtils.cleanBool("SIGNIFICANT"));
assertFalse(quollUtils.cleanBool("test"));
}

@Test
void cleanYN() {
assertEquals("YES",quollUtils.cleanYN("TRUE"));
assertEquals("NO",quollUtils.cleanYN("NO"));
assertEquals("NO",quollUtils.cleanYN("OTHER"));

}

@Test
void cleanLac() {
assertEquals(200,quollUtils.cleanLac("200"));
assertEquals(null,quollUtils.cleanLac("-1"));
assertEquals(null,quollUtils.cleanLac("abc"));

}

@Test
void cleanRacVal() {
assertEquals(254,quollUtils.cleanRacVal("254"));
assertEquals(null,quollUtils.cleanRacVal("256"));
assertEquals(null,quollUtils.cleanRacVal("abc"));

}

@Test
void cleanInt() {
assertEquals(254,quollUtils.cleanInt("254"));
assertEquals(null,quollUtils.cleanInt("2aa54"));

}
@Test
void cleanBigInt() {
assertEquals(254,quollUtils.cleanBigInt(BigDecimal.valueOf(254.45232)));
assertEquals(null,quollUtils.cleanBigInt(null));

}

@Test
void mapAreaCode() {

assertEquals("02",quollUtils.mapAreaCode("2"));
assertEquals(null,quollUtils.mapAreaCode("abc"));

}

@Test
void cleanUra() {
assertEquals(2,quollUtils.cleanUra("2:45"));

}

@Test
void classTag() {
}

@Test
void isInteger() {
assertTrue(quollUtils.isInteger("2"));
assertFalse(quollUtils.isInteger("2a"));

}

@Test
void technologyToType() {
assertEquals("On/nrCell",quollUtils.technologyToType("NR"));
assertEquals("ocw/lteCell",quollUtils.technologyToType("LTE"));
assertEquals("Ocw/umtsCell",quollUtils.technologyToType("WCDMA"));
assertEquals("ocw/gsmCell",quollUtils.technologyToType("GSM"));
assertEquals(null,quollUtils.technologyToType(""));
assertEquals(null,quollUtils.technologyToType("other"));

}

@Test
void getBbhType() {
assertEquals("ocw/eNodeB",quollUtils.getBbhType("LTE"));
assertEquals("ocw/gnbdu",quollUtils.getBbhType("NR"));
assertEquals(null,quollUtils.getBbhType(null));
assertEquals(null,quollUtils.getBbhType("other"));

}


@Test
void cleanTechnology() {
assertEquals(26000,quollUtils.cleanTechnology("NR26G"));
assertEquals(null,quollUtils.cleanTechnology(null));
assertEquals(null,quollUtils.cleanTechnology("other nonintval"));

}

@Test
void cleanRepeaterTechnology() {
assertEquals("UMTS",quollUtils.cleanRepeaterTechnology("WCDMA"));
assertEquals(null,quollUtils.cleanRepeaterTechnology(null));
assertEquals("other nonintval",quollUtils.cleanRepeaterTechnology("other nonintval"));

}

@Test
void extractNameFromMecontext() {
assertEquals(null, quollUtils.extractNameFromMecontext("test",null));
assertEquals(null, quollUtils.extractNameFromMecontext(null,false));
assertEquals(null, quollUtils.extractNameFromMecontext(null,false));
assertEquals("est", quollUtils.extractNameFromMecontext("test_est",true));
assertEquals(null, quollUtils.extractNameFromMecontext("testeetest",true));

}

@Test
void extractIdFromMecontext() {
assertEquals(null, quollUtils.extractIdFromMecontext("test_test"));
assertEquals(null, quollUtils.extractIdFromMecontext(null));
assertEquals(123, quollUtils.extractIdFromMecontext("test_123"));

}

@Test
void enmGnbType() {
assertEquals("ocw/gnbdu", quollUtils.enmGnbType("TEST_BBH_testvak"));
assertEquals("ocw/gnbdu", quollUtils.enmGnbType("TESTtestvak"));
assertEquals(null, quollUtils.enmGnbType(null));

}

@Test
void genEGNodeBName(){

assertEquals("NODECODEtEST10",quollUtils.genEGNodeBName(10, "(LTE3)", 50000, "NODECODEtEST"));
assertEquals("NODECODEtEST30",quollUtils.genEGNodeBName(null, "(LTE3)", 50000, "NODECODEtEST"));
assertEquals("NODECODEtESTZ0",quollUtils.genEGNodeBName(null, "(LTE33)", 50000, "NODECODEtEST"));
assertEquals("NODECODEtEST10",quollUtils.genEGNodeBName(null, "(WCDMA3G)", 50000, "NODECODEtEST"));
}
@Test
void genNodeBName(){
assertEquals("NODECODEtEST3",quollUtils.genNodeBName("(3)", "NODECODEtEST"));
assertEquals("NODECODEtESTZ",quollUtils.genNodeBName("(33)", "NODECODEtEST"));
assertEquals("NODECODEtEST1",quollUtils.genNodeBName("TEST", "NODECODEtEST"));

}

@Test
void transformBsToB3(){
Dataset bs = quollUtils.readFile(session, null, "src/test/resources/input/bstestData.csv");
assertEquals(3,quollUtils.transformBsToB3(bs).count());
}
@Test
void transformenmToMbs() {
Dataset bs = quollUtils.readFile(session, null, "src/test/resources/input/bstestData.csv");
Dataset enm = quollUtils.readFile(session, null, "src/test/resources/input/enmTestData.csv");
Dataset b3 = quollUtils.transformBsToB3(bs);
assertEquals(3, quollUtils.transformenmToMbs(enm, b3, bs).count());
}
@Test
void joinBsAndn(){
Dataset b2 = quollUtils.readFile(session, null, "src/test/resources/input/b2testData.csv");
Dataset n = quollUtils.readFile(session, null, "src/test/resources/input/mbsTestData.csv");
assertEquals(3,quollUtils.joinBsAndn(b2, n).count());
}
@Test
void generateBsDataset(){
Dataset b = quollUtils.readFile(session, Schemas.bbhSpreadsheetSchema, "src/test/resources/input/tempest.csv");

Dataset d=quollUtils.generateBsDataset(tespest,b);
// d.show();
assertEquals(4,d.count());
}
@Test
void generateEnm(){
Dataset gnbd_e = session.read()
.option("header", "true")
.csv("src/test/resources/input/enm/gnbd_e.csv");
Dataset nbe=session.read().option("header","true")
.csv("src/test/resources/input/enm/nb_e.csv");
Dataset enb_e=session.read().option("header","true")
.csv("src/test/resources/input/enm/enb_e.csv");

// gnbd_e.show();
assertEquals(7,quollUtils.generateEnm(gnbd_e,nbe,enb_e).count());
}
@Test
void tranformDesigner(){
Dataset q = session.read()
.option("header", "true")
.csv("src/test/resources/input/enm/designer.csv");
Dataset op=quollUtils.tranformDesigner(q);
assertEquals(3,op.count());
}
@Test
public void rncToNodeB(Dataset q,Dataset nodeB) {

}

@Test
public void getMcc() {
    assertEquals(123,quollUtils.getMcc("123456"));
}
@Test
    public void getMnc() {
    assertEquals("45",quollUtils.getMnc("123456"));
    }
}