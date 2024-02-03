# Enc & Dec File

```
dbutils.fs.ls("/test/")
databricks fs cp -r dbfs:/test .

%scala

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.bouncycastle.bcpg.{ArmoredOutputStream, CompressionAlgorithmTags}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory
import org.bouncycastle.openpgp.operator.jcajce.{JcaKeyFingerprintCalculator, JcePBESecretKeyDecryptorBuilder, JcePGPDataEncryptorBuilder, JcePublicKeyDataDecryptorFactoryBuilder, JcePublicKeyKeyEncryptionMethodGenerator}

import java.security.{SecureRandom, Security}
import org.bouncycastle.openpgp.{PGPCompressedData, PGPCompressedDataGenerator, PGPEncryptedData, PGPEncryptedDataGenerator, PGPEncryptedDataList, PGPException, PGPLiteralData, PGPOnePassSignatureList, PGPPrivateKey, PGPPublicKey, PGPPublicKeyEncryptedData, PGPPublicKeyRing, PGPPublicKeyRingCollection, PGPSecretKey, PGPSecretKeyRing, PGPSecretKeyRingCollection, PGPUtil}
import org.bouncycastle.util.io.Streams

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream, IOException, InputStream, OutputStream}

// #######################################
// ### case class
// #######################################
case class SecretData(sourceFilePath: String,
                      targetFilePath: String,
                      keyPath: String,
                      keyStream: String,
                      passPhrase: String,
                      armor: Boolean,
                      withIntegrityCheck: Boolean)

// #######################################
// ### case class
// #######################################
val publicKey = dbutils.secrets.get(scope = "az-kv-name", key = "app-public-key")
val privateKey = dbutils.secrets.get(scope = "az-kv-name", key = "app-private-key")
val passPhrase = dbutils.secrets.get(scope = "az-kv-name", key = "app-pass-phrase")

val encSourceFilePath = s"/dbfs/test/s1.json"
val encTargetFilePath = s"/dbfs/test/s2.encr"
val decTargetFilePath = s"/dbfs/test/s2.decr"

// #######################################
// ### encrypt call
// #######################################
val encryptFileSecretData = new SecretData(sourceFilePath = encSourceFilePath,
      targetFilePath = encTargetFilePath, keyStream = null,
      keyPath = publicKey, passPhrase = passPhrase, 
      armor = false, withIntegrityCheck = false)
val encFlag = encryptFile(encryptFileSecretData);
println(s"Source File (${encSourceFilePath}) => encr status ::: " +encFlag)

// #######################################
// ### decrypt call
// #######################################
val decryptFileSecretData = new SecretData(sourceFilePath = encTargetFilePath,
      targetFilePath = decTargetFilePath, keyStream = privateKey,
      keyPath = null, passPhrase = passPhrase, 
      armor = false, withIntegrityCheck = false)
val decFlag = decryptFile(decryptFileSecretData);
println(s"Source File (${encTargetFilePath}) => decr status ::: " +decFlag)


// #######################################
// ### methods
// #######################################
def decryptFile(secretData: SecretData): Boolean = {
  var encryptedDataList: PGPEncryptedDataList = null
  var pbe: PGPPublicKeyEncryptedData = null
  var outputFileStream: OutputStream = null
  var inputFileStream: InputStream = null
  var keyInputStream: InputStream = null
  var sKey: PGPPrivateKey = null
  var result: Boolean = false
  try {
    //
    Security.addProvider(new BouncyCastleProvider())
    //
    if (secretData.keyPath == null && secretData.keyStream != null) {
      keyInputStream = getInputStream(secretData.keyStream, "UTF-8")
    } else if (secretData.keyPath != null && secretData.keyStream == null) {
      keyInputStream = new BufferedInputStream(new FileInputStream(secretData.keyPath))
    } else {
      throw new Exception("Private KeyPath or KeyStream is not present to decrypt a File")
    }

    //
    inputFileStream = PGPUtil.getDecoderStream(new BufferedInputStream(new FileInputStream(secretData.sourceFilePath)))
    val pgpF = new JcaPGPObjectFactory(inputFileStream)
    val o = pgpF.nextObject

    if (o.isInstanceOf[PGPEncryptedDataList]) {
      encryptedDataList = o.asInstanceOf[PGPEncryptedDataList]
    }
    else {
      encryptedDataList = pgpF.nextObject.asInstanceOf[PGPEncryptedDataList]
    }

    //
    val it = encryptedDataList.getEncryptedDataObjects
    val pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyInputStream),
      new JcaKeyFingerprintCalculator());
    while (sKey == null && it.hasNext) {
      pbe = it.next.asInstanceOf[PGPPublicKeyEncryptedData]
      sKey = findSecretKey(pgpSec, pbe.getKeyID, secretData.passPhrase.toCharArray)
    }

    //
    if (sKey == null) {
      throw new IllegalArgumentException("secret key for message not found.")
    }

    //
    val clear = pbe
      .getDataStream(new JcePublicKeyDataDecryptorFactoryBuilder().setProvider("BC").build(sKey));
    val plainFact = new JcaPGPObjectFactory(clear)
    var message = plainFact.nextObject
    outputFileStream = new FileOutputStream(secretData.targetFilePath)

    message match {
      case cData: PGPCompressedData =>
        val pgpFact = new JcaPGPObjectFactory(cData.getDataStream)
        message = pgpFact.nextObject
      case _ =>
    }

    if (message.isInstanceOf[PGPLiteralData]) {
      val ld = message.asInstanceOf[PGPLiteralData]
      val unc = ld.getInputStream
      Streams.pipeAll(unc, outputFileStream)
    }
    else if (message.isInstanceOf[PGPOnePassSignatureList]) {
      throw new PGPException("encrypted message contains a signed message - not literal data.")
    }
    else {
      throw new PGPException("message is not a simple encrypted file - type unknown.")
    }

    if (pbe.isIntegrityProtected) {
      if (!pbe.verify) {
        System.out.println("message failed integrity check ")
      }
      else {
        System.out.println("no message integrity check ")
      }
    }
    result = true
  } catch {
    case e: Exception => 
      print("gpg decryptFile failed with error: " + e.toString)
      throw new Exception("Error decrypting the file => Error Message ::: " + e.toString);
  } finally {
    if (outputFileStream != null) {
      try {
        outputFileStream.close()
      } catch {
        case e: Exception =>
          System.out.println("Not able to close outputFileStream:" + e)
      }
    }
    if (keyInputStream != null) {
      try {
        keyInputStream.close()
      } catch {
        case e: Exception =>
          System.out.println("Not able to close keyInputStream:" + e)
      }
    }
    if (inputFileStream != null) {
      try {
        inputFileStream.close()
      } catch {
        case e: Exception =>
          System.out.println("Not able to close inputFileStream:" + e)
      }
    }
  }
  return result
}

def encryptFile(secretData: SecretData): Boolean = {
  try {
    Security.addProvider(new BouncyCastleProvider())
    var encKey: PGPPublicKey = null
    if (secretData.keyPath == null && secretData.keyStream != null) {
      val inputStream: InputStream = getInputStream(secretData.keyStream, "UTF-8")
      encKey = readPublicKeyUsingStream(inputStream)
    }
    else if (secretData.keyPath != null && secretData.keyStream == null) {
      encKey = readPublicKeyUsingString(secretData.keyPath);
    } else {
      throw new Exception("Error encrypting the file, missing mandatory information (Either KeyPath or KeyStream) to encrypt a file, both can not be null");
    }
    val result = encryptFile(encKey, secretData);
    println(s"encryptFile(secretData: SecretData) +> "+result)
    return result;
  } catch {
    case e: Exception => 
      print("gpg encryptFile failed with error: " + e.toString)
      throw new Exception("Error encrypting the file => Error Message ::: " + e.toString);
  }
}

  
def encryptFile(encKey: PGPPublicKey, secretData: SecretData): Boolean = {
  var cOut: OutputStream = null
  val inputFileStream: InputStream = null
  var outputFileStream: OutputStream = null

  try {
    val sourceFilePath = secretData.sourceFilePath
    val targetFilePath = secretData.targetFilePath
    val withIntegrityCheck = secretData.withIntegrityCheck

    var result = false
    val inputFileStream: InputStream = null
    
    val inputBytes = compressFile2ByteArray(sourceFilePath, CompressionAlgorithmTags.ZIP)
    outputFileStream = new BufferedOutputStream(new FileOutputStream(targetFilePath))
    //if (armor) outputFileStream = new ArmoredOutputStream(outputFileStream)

    val encGen = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(3).setWithIntegrityPacket(withIntegrityCheck).setSecureRandom(new SecureRandom()).setProvider("BC"));
    encGen.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encKey).setProvider("BC"));
    cOut = encGen.open(outputFileStream, inputBytes.length);
    cOut.write(inputBytes);
    result = true;
    return result;

  } catch {
    case e: Exception => 
      print("gpg encryptFile failed with error: " + e.toString)
      throw new Exception("Error encrypting the file => Error Message ::: " + e.toString);
  } finally {
    if (inputFileStream != null) {
      try {
        inputFileStream.close();
      } catch {
        case e:Exception =>
          println("Not able to close inputFileStream: " + e);
          e.printStackTrace();
      }
    }
    if (cOut != null) {
      try {
        cOut.close();
      } catch {
        case e:Exception =>
          println("Not able to close OutPutStream: " + e);
          e.printStackTrace();
      }
    }
    if (outputFileStream != null) {
      try {
        outputFileStream.close();
      } catch  {
        case e:Exception =>
          println("Not able to close OutPutStream: " + e);
          e.printStackTrace();

      }
    }
    if (inputFileStream != null) {
      try {
        inputFileStream.close();
      } catch {
        case e:Exception =>
          println("Not able to close OutPutStream: " + e);
          e.printStackTrace();

      }
    }
  }
}

def readPublicKeyUsingStream(inputStream: InputStream): PGPPublicKey = {
  val pgpPub: PGPPublicKeyRingCollection = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(inputStream),
    new JcaKeyFingerprintCalculator());
  readPublicKey(pgpPub)
}

def readPublicKeyUsingFile(inputStream: InputStream): PGPPublicKey = {
  val pgpPub: PGPPublicKeyRingCollection = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(inputStream),new JcaKeyFingerprintCalculator());
  readPublicKey(pgpPub)
}

def readPublicKeyUsingString(str: String): PGPPublicKey  = {
  val byteArrayInputStream: ByteArrayInputStream = new ByteArrayInputStream(str.getBytes)
  val inputStream: InputStream = org.bouncycastle.openpgp.PGPUtil.getDecoderStream(byteArrayInputStream)

  val pgpPub = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(inputStream),
    new JcaKeyFingerprintCalculator());
  inputStream.close();

  return readPublicKey(pgpPub)
}

def readPublicKey(pgpPub: PGPPublicKeyRingCollection): PGPPublicKey  = {
  val keyRingOuterIterator = pgpPub.getKeyRings();
  while (keyRingOuterIterator.hasNext) {
    val outerKeyRing = keyRingOuterIterator.next
    val outerKeyRingObj = outerKeyRing.asInstanceOf[PGPPublicKeyRing]

    val publicKeysIterator = outerKeyRingObj.getPublicKeys;
    while (publicKeysIterator.hasNext) {
      val publicKeyIteratorItem = publicKeysIterator.next()
      val keyObj = publicKeyIteratorItem.asInstanceOf[PGPPublicKey]
      if (keyObj.isEncryptionKey) {
        return keyObj;
      }
    }
  }
  throw new IllegalArgumentException("Error - Cannot find encryption key in key ring.")
}

def getInputStream(key: String, encoding: String): InputStream = {
  try {
    var byteArrayInputStream = new ByteArrayInputStream(key.getBytes(encoding))
    return byteArrayInputStream
  } catch {
    case e: Exception => 
      print("gpg encryptFile failed with error: " + e.toString)
      throw new Exception("Error occurred creating input stream ::: " + e.toString);
  } 
}

def compressFile2ByteArray(fileName: String, algorithm: Int): Array[Byte] = {
  val byteArrayOutputStream = new ByteArrayOutputStream
  val comData = new PGPCompressedDataGenerator(algorithm)
  PGPUtil.writeFileToLiteralData(comData.open(byteArrayOutputStream), PGPLiteralData.BINARY, new File(fileName))
  comData.close()
  byteArrayOutputStream.toByteArray
}

def findSecretKey(pgpSec: PGPSecretKeyRingCollection, keyID: Long, pass: Array[Char]): PGPPrivateKey = {
  val pgpSecKey = pgpSec.getSecretKey(keyID)
  if (pgpSecKey == null) {
    return null
  }
  pgpSecKey.extractPrivateKey(new JcePBESecretKeyDecryptorBuilder().setProvider("BC").build(pass))
}

def readSecretKey(fileName: String): PGPSecretKey = {
  val keyIn = new BufferedInputStream(new FileInputStream(fileName))
  val secKey = readSecretKey(keyIn)
  keyIn.close()
  secKey
}

def readSecretKey(input: InputStream): PGPSecretKey = {
  val pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(input), new JcaKeyFingerprintCalculator)
  val keyRingIter = pgpSec.getKeyRings
  while (keyRingIter.hasNext) {
    val keyRing = keyRingIter.next.asInstanceOf[PGPSecretKeyRing]
    val keyIter = keyRing.getSecretKeys
    while (keyIter.hasNext) {
      val key = keyIter.next.asInstanceOf[PGPSecretKey]
      if (key.isSigningKey) {
        return key
      }
    }
  }
  throw new IllegalArgumentException("Can't find signing key in key ring.")
}
```

