## Merge Azure Multiple Storage Files into One DataFrame

```
%scala
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

var mountPoint = "/mnt/qtest/"
val vaultScope = "az-kv-scope-name"

val storageAccount = "strg-name"
val blobContainer = "cntnr-name"
val storageAccountSasKey = "strg-acc-adls-sas-key-name"
val sourceFolder = "merge-files/output"

val sourcePath = s"wasbs://$blobContainer@$storageAccount.blob.core.windows.net/$sourceFolder"
val sasConfigKey = s"fs.azure.sas.$blobContainer.$storageAccount.blob.core.windows.net"

//  1,Mounting
var sas = dbutils.secrets.get(scope = vaultScope, key = storageAccountSasKey)
var remountRequired = true
performMount(remountRequired, sourcePath, sasConfigKey, sas, mountPoint)

val files = listFiles(dir = mountPoint)
val arr_df = files.map(spark.read
                            .format("csv")
                            .option("delimeter", ",")
                            .option("header", true)
                            .load(_))
val df = arr_df.reduce(_ union _)
df.show(false)

def listFiles(dir: String): mutable.MutableList[String] = {
	val fileList = mutable.MutableList[String]()
	dbutils.fs.ls(dir).map(file => {
		val path = file.path.replace("%25", "%").replace("%25", "%")
		if (file.isFile && file.name.endsWith(".csv")) {
			fileList += path
		}
	})
	return fileList;
}


def listDirectories(dir: String, recurse: Boolean): Array[String] = {
	dbutils.fs.ls(dir).map(file => {
		val path = file.path.replace("%25", "%").replace("%25", "%")
		if (file.isDir) listDirectories(path, recurse)
		else Array[String](path.substring(0, path.lastIndexOf("/") + 1))
	}).reduceOption(_ union _).getOrElse(Array()).distinct
}

def performMount(remountRequired: Boolean, source: String, sasConfigKey: String, sas: String, mountPoint: String): Unit = {
	if (remountRequired) {
		if (dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mountPoint)) {
			dbutils.fs.unmount(mountPoint)
			println(s"unmounted successfully ")
		}
	}

	if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mountPoint)) {
		println(s"${mountPoint} not mounted, mounting now...")
		dbutils.fs.mount(
			source = source,
			mountPoint = mountPoint,
			extraConfigs = Map(sasConfigKey -> sas)
		)
		println(s"${mountPoint} successfully mounted")
	} else {
		println(s"${mountPoint} already mounted")
	}
}
```
