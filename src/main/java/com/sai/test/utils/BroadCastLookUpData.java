package com.sai.test.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BroadCastLookUpData implements Serializable {

// # Map the Quell cell_status to the EAI status
//# Note: The status picklist for wireless deployments; OCN WirelessDeployment StatusPicklist and arcollDeployment StatusPacklist
public Map<String, String> cellStatusMapDict;

public Map<String, String> statusMapDict;


//# Map Quoll base_station_type to the EAI-collType
// # Note: TBA is mapped to Macro as there should not be TBA type in the solution
// # Note: A number of these cellTypes will need to be created in ocw:telstraCellTypePicklist
public Map<String, String> cellTypeMapDict;

//# Map Quoll cell function to EAT cellFunction (New field: OCW/rfCell/cellFunction)
public Map<Integer, String> cellFunctionbict;

// # Only the following MSC_NODE values are valid. All others are to be ignored. # Note: Implementing this as a Dict in case the names need to be mapped in the future. That's my excuse and I'm sticking to it!
public Map<String, String> validiscNodeDict;


public Map<Integer, String> areaCodeDict;

public Map<String, String> ranNumberingDict;

public BroadCastLookUpData() {
cellStatusMapDict =new HashMap<>();

cellStatusMapDict.put("Commissioning", "IN CONSTRUCTION");
cellStatusMapDict.put("Concept", "IN CONCEPT");
cellStatusMapDict.put("Inactive", "PENDING DECOMMISSION");
cellStatusMapDict.put("In Service", "IN SERVICE");
cellStatusMapDict.put("In Service - Not Accepted", "NOT ACCEPTED");
cellStatusMapDict.put("Planned Create", "IN DESIGN");
ranNumberingDict =new HashMap<>();

ranNumberingDict.put("10", "A");
ranNumberingDict.put("11", "B");
ranNumberingDict.put("12", "C");
ranNumberingDict.put("13", "D");
ranNumberingDict.put("14", "E");
ranNumberingDict.put("15", "F");
ranNumberingDict.put("16", "G");
ranNumberingDict.put("17", "H");
ranNumberingDict.put("18", "J");
ranNumberingDict.put("19", "K");
ranNumberingDict.put("20", "L");
ranNumberingDict.put("21", "M");
ranNumberingDict.put("22", "N");
ranNumberingDict.put("23", "P");
ranNumberingDict.put("24", "Q");
ranNumberingDict.put("25", "R");
ranNumberingDict.put("26", "S");
ranNumberingDict.put("27", "T");
ranNumberingDict.put("28", "U");
ranNumberingDict.put("29", "V");
ranNumberingDict.put("30", "W");
ranNumberingDict.put("31", "X");
ranNumberingDict.put("32", "Y");
ranNumberingDict.put("33", "Z");
validiscNodeDict = new HashMap<>();
validiscNodeDict.put("NSW-ACT-HSCPOOL", "NSW-ACT-HSCPOOL");
validiscNodeDict.put("NSW-PAR-MSCPOOL", "NSW-PAR-HSCPOOL");
validiscNodeDict.put("QLD-NSCPOOL", "OLD-HSCPOOL");
validiscNodeDict.put("SA-NT-NS.CPOOL", "SA-NT-HSCPOOL");
validiscNodeDict.put("VIC-TAS-NSCPOOL", "VIC-TAS-HSCPOOL");
validiscNodeDict.put("WA-NSCPOOL", "NA-HSCPOOL");
areaCodeDict = new HashMap<>();
areaCodeDict.put(2, "02");
areaCodeDict.put(3, "03");
areaCodeDict.put(7, "07");
areaCodeDict.put(8, "08");
areaCodeDict.put(9, "09");
cellTypeMapDict = new HashMap<>();
cellTypeMapDict.put("Dedicated Donor", "DEDICATED DONOR");
cellTypeMapDict.put("IBC", "IN BUILDING CELL");
cellTypeMapDict.put("Macro", "MACRO");
cellTypeMapDict.put("Micro", "MICRO");
cellTypeMapDict.put("Mixed", "MIXED");
cellTypeMapDict.put("Remote Macro", "REMOTE MACRO");
cellTypeMapDict.put("Remote Micro", "REMOTE MICRO");
cellTypeMapDict.put("Remote Transceiver", "REMOTE TRANSCEIVER");
cellTypeMapDict.put("TBA", "MACRO");
cellTypeMapDict.put("Temporary", "TEMPORARY");
cellTypeMapDict.put("Test", "TEST");

statusMapDict = new HashMap<>();
statusMapDict.put("Commissioning", "PENDING");
statusMapDict.put("Concept", "PENDING");
statusMapDict.put("Erroneous entry", "PENDING DECOMMISSION");
statusMapDict.put("Inactive", "PENDING DECOMMISSION");
statusMapDict.put("In Service", "ACTIVE");
statusMapDict.put("In Service - Not Accepted", "PENDING");
statusMapDict.put("Planned Create", "PENDING");
statusMapDict.put("Removed", "DECOMMISSIONED");
statusMapDict.put("Not Allocated", "PENDING");
statusMapDict.put("Pending Delete", "PENDING DECOMMISSION");
statusMapDict.put("Planned", "PENDING");
statusMapDict.put("Unknown", "PENDING");
statusMapDict.put("Working", "ACTIVE");

cellFunctionbict = new HashMap<>();
cellFunctionbict.put(1, "Coverage");
cellFunctionbict.put(2, "Capacity");
cellFunctionbict.put( 3, "Other");
}
}