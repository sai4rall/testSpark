package com.sai.test.utils;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;

import java.util.Base64;

public class SecretUtils {

public String getSecret() {

String secretName = "lab/disco/pgsecret";
String region = "ap-southeast-2";

// Create a Secrets Manager client
AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
.withRegion(region)
.build();

String secret, decodedBinarySecret;
GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
.withSecretId(secretName);
GetSecretValueResult getSecretValueResult = null;

try {
getSecretValueResult = client.getSecretValue(getSecretValueRequest);
} catch (DecryptionFailureException e) {
e.printStackTrace();
throw e;
} catch (InternalServiceErrorException e) {
e.printStackTrace();
throw e;
} catch (InvalidParameterException e) {
e.printStackTrace();
throw e;
} catch (InvalidRequestException e) {
e.printStackTrace();
throw e;
} catch (ResourceNotFoundException e) {
e.printStackTrace();
throw e;
}

if (getSecretValueResult.getSecretString() != null) {
secret = getSecretValueResult.getSecretString();
return secret;

}
else {
decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
return decodedBinarySecret;
}
}

}