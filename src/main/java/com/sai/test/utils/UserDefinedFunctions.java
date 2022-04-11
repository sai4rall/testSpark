package com.sai.test.utils;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.math.BigDecimal;

public class UserDefinedFunctions implements Serializable {

UdmfUtilities udmfUtilities;
UserDefinedFunctions(UdmfUtilities udmfUtilities){
this.udmfUtilities=udmfUtilities;
}

public UserDefinedFunction eaisectorNumber = functions.udf((String s) -> udmfUtilities.genSectorNumber(s), DataTypes.IntegerType);
public UserDefinedFunction eaiCellStatus = functions.udf((String s) -> udmfUtilities.mapCellStatus(s), DataTypes.StringType);
public UserDefinedFunction eaiStatus = functions.udf((String s) -> udmfUtilities.mapStatus(s), DataTypes.StringType);
public UserDefinedFunction eaiCellType = functions.udf((String s) -> udmfUtilities.mapCellType(s), DataTypes.StringType);
public UserDefinedFunction eaiCellFunction = functions.udf((String s) -> udmfUtilities.mapCellFunction(s), DataTypes.StringType);
public UserDefinedFunction eaiRac = functions.udf((String s) -> udmfUtilities.cleanRac(s), DataTypes.IntegerType);
public UserDefinedFunction eaivalidMscNode = functions.udf((String s) -> udmfUtilities.validateMscNode(s), DataTypes.StringType);
public UserDefinedFunction eaiBool = functions.udf((String s) -> udmfUtilities.cleanBool(s), DataTypes.BooleanType);
public UserDefinedFunction eaiYN = functions.udf((String s) -> udmfUtilities.cleanYN(s), DataTypes.StringType);
public UserDefinedFunction eaiRacVal = functions.udf((String s) -> udmfUtilities.cleanRacVal(s), DataTypes.IntegerType);
public UserDefinedFunction eaiInt = functions.udf((String s) -> udmfUtilities.cleanInt(s), DataTypes.IntegerType);
public UserDefinedFunction eaiBigInt = functions.udf((BigDecimal s) -> udmfUtilities.cleanBigInt(s), DataTypes.IntegerType);
public UserDefinedFunction eaiTechnologyToType = functions.udf((String s) -> udmfUtilities.technologyToType(s), DataTypes.StringType);
public UserDefinedFunction eaiAreaCode = functions.udf((String s) -> udmfUtilities.mapAreaCode(s), DataTypes.StringType);
public UserDefinedFunction eaiUra = functions.udf((String s) -> udmfUtilities.cleanUra(s),DataTypes.IntegerType);
public UserDefinedFunction eaiChannel = functions.udf((String s) -> udmfUtilities.cleanTechnology(s),DataTypes.IntegerType);
public UserDefinedFunction eaiNodeBName = functions.udf((String s1,String s2) -> udmfUtilities.genNodeBName(s1,s2),DataTypes.StringType);
public UserDefinedFunction eaiBbhType = functions.udf((String s) -> udmfUtilities.getBbhType(s),DataTypes.StringType);
public UserDefinedFunction eaiEGNodeBName = functions.udf((Integer du,String site,Integer nid,String nodeCode) -> udmfUtilities.genEGNodeBName(du,site,nid,nodeCode),DataTypes.StringType);
public UserDefinedFunction eaiNameFromMecontext = functions.udf((String s1,Boolean b1) -> udmfUtilities.extractNameFromMecontext(s1,b1),DataTypes.StringType);
public UserDefinedFunction eaiIdFromMecontext = functions.udf((String s1) -> udmfUtilities.extractIdFromMecontext(s1),DataTypes.IntegerType);
public UserDefinedFunction eaiEnmGnbType = functions.udf((String s1) -> udmfUtilities.enmGnbType(s1),DataTypes.StringType);
public UserDefinedFunction eaiSystemType = functions.udf((String s1) -> udmfUtilities.cleanRepeaterTechnology(s1),DataTypes.StringType);
public UserDefinedFunction eaiMcc = functions.udf((String s1) -> udmfUtilities.getMcc(s1),DataTypes.IntegerType);
public UserDefinedFunction eaiMnc = functions.udf((String s1) -> udmfUtilities.getMnc(s1),DataTypes.StringType);
}