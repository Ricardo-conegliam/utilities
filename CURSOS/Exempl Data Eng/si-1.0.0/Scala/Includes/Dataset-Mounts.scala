// Databricks notebook source
val tags = com.databricks.logging.AttributionContext.current.tags

//*******************************************
// GET VERSION OF APACHE SPARK
//*******************************************

// Get the version of spark
val Array(sparkMajorVersion, sparkMinorVersion, _) = spark.version.split("""\.""")

// Set the major and minor versions
spark.conf.set("com.databricks.training.spark.major-version", sparkMajorVersion)
spark.conf.set("com.databricks.training.spark.minor-version", sparkMinorVersion)

//*******************************************
// GET VERSION OF DATABRICKS RUNTIME
//*******************************************

// Get the version of the Databricks Runtime
val version = {
  val dbr = com.databricks.spark.util.SparkServerContext.serverVersion.replace("dbr-", "")
  val scalaMinMajVer = util.Properties.versionNumberString
  val index = scalaMinMajVer.lastIndexOf(".")
  val len = scalaMinMajVer.length
  dbr + ".x-scala" + scalaMinMajVer.dropRight(len - index)
}

val runtimeVersion = if (version != "") {
  spark.conf.set("com.databricks.training.job", "false")
  version
} else {
  spark.conf.set("com.databricks.training.job", "true")
  dbutils.widgets.get("sparkVersion")
}

val runtimeVersions = runtimeVersion.split("""-""")
// The GPU and ML runtimes push the number of elements out to 5
// so we need to account for every scenario here. There should
// never be a case in which there is less than two so we can fail
// with an helpful error message for <2 or >5
val (dbrVersion, scalaVersion) = {
  runtimeVersions match {
    case Array(d, _, _, _, s) => (d, s.replace("scala", ""))
    case Array(d, _, _, s)    => (d, s.replace("scala", ""))
    case Array(d, _, s)       => (d, s.replace("scala", ""))
    case Array(d, s)          => (d, s.replace("scala", ""))
    case _ =>
      throw new IllegalArgumentException(s"""Dataset-Mounts: Cannot parse version(s) from "${runtimeVersions.mkString(", ")}".""")
  }
}
val Array(dbrMajorVersion, dbrMinorVersion, _*) = dbrVersion.split("""\.""")

// Set the the major and minor versions
spark.conf.set("com.databricks.training.dbr.version", version)
spark.conf.set("com.databricks.training.dbr.major-version", dbrMajorVersion)
spark.conf.set("com.databricks.training.dbr.minor-version", dbrMinorVersion)

//*******************************************
// GET USERNAME AND USERHOME
//*******************************************

// Get the user's name
val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")

val userhome = s"dbfs:/user/$username"

// Set the user's name and home directory
spark.conf.set("com.databricks.training.username", username)
spark.conf.set("com.databricks.training.userhome", userhome)

//**********************************
// GET TAG VALUE
// Find a given tag's value or return a supplied default value if not found
//**********************************

def getTagValue(tagName: String, defaultValue: String = null): String = {
  val tags = com.databricks.logging.AttributionContext.current.tags
  val values = tags.collect({ case (t, v) if t.name == tagName => v }).toSeq
  values.size match {
    case 0 => defaultValue
    case _ => values.head.toString
  }
}

//**********************************
// GET EXPERIMENT ID
// JobId fallback in production mode
//**********************************

def getExperimentId(): Long = {
  val notebookId = getTagValue("notebookId", null)
  val jobId = getTagValue("jobId", null)
  
  (notebookId != null) match { 
      case true => notebookId.toLong
      case false => (jobId != null) match { 
        case true => jobId.toLong
        case false => 0
      }
  }
}


spark.conf.set("com.databricks.training.experimentId", getExperimentId())

//**********************************
// VARIOUS UTILITY FUNCTIONS
//**********************************

def assertSparkVersion(expMajor:Int, expMinor:Int):String = {
  val major = spark.conf.get("com.databricks.training.spark.major-version")
  val minor = spark.conf.get("com.databricks.training.spark.minor-version")

  if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor)) {
    throw new Exception(s"This notebook must be ran on Spark version $expMajor.$expMinor or better, found Spark $major.$minor")
  }
  return s"$major.$minor"
}

def assertDbrVersion(expMajor:Int, expMinor:Int):String = {
  val major = spark.conf.get("com.databricks.training.dbr.major-version")
  val minor = spark.conf.get("com.databricks.training.dbr.minor-version")

  if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor)) {
    throw new Exception(s"This notebook must be ran on Databricks Runtime (DBR) version $expMajor.$expMinor or better, found $major.$minor.")
  }
  return s"$major.$minor"
}

def assertIsMlRuntime():Unit = {
  if (version.contains("-ml-") == false) {
    throw new RuntimeException(s"This notebook must be ran on a Databricks ML Runtime, found $version.")
  }
}

// **********************************
//  GET AZURE DATASOURCE
// **********************************

def getAzureDataSource(): (String,String,String) = {
  val datasource = spark.conf.get("com.databricks.training.azure.datasource").split("\t")
  val source = datasource(0)
  val sasEntity = datasource(1)
  val sasToken = datasource(2)
  return (source, sasEntity, sasToken)
}

def initializeBrowserSideStats(): Unit = {
  import java.net.URLEncoder.encode
  import scala.collection.Map
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization.write
  import org.json4s.JsonDSL._

  implicit val formats: DefaultFormats = DefaultFormats

  val tags = com.databricks.logging.AttributionContext.current.tags

  // Get the user's name and home directory
  val username = spark.conf.get("com.databricks.training.username", "unknown-username")
  val userhome = spark.conf.get("com.databricks.training.userhome", "unknown-userhome")
  
  val courseName = spark.conf.get("com.databricks.training.courseName", "unknown-course")
  val moduleName = spark.conf.get("com.databricks.training.moduleName", "unknown-module")

  // Get the the major and minor versions
  val dbrVersion = spark.conf.get("com.databricks.training.dbr.version", "0.0")
  val dbrMajorVersion = spark.conf.get("com.databricks.training.dbr.major-version", "0")
  val dbrMinorVersion = spark.conf.get("com.databricks.training.dbr.minor-version", "0")

  val sessionId = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_SESSION_ID, "unknown-sessionId")
  val hostName = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_HOST_NAME, "unknown-host-name")
  val clusterMemory = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_CLUSTER_MEMORY, "unknown-cluster-memory")
  val clientBranchName = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_BRANCH_NAME, "unknown-branch-name")
  val notebookLanguage = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_NOTEBOOK_LANGUAGE, "unknown-notebook-language")
  val browserUserAgent = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER_AGENT, "unknown-user-agent")
  val browserHostName = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_HOST_NAME, "unknown-host-name")

// Need to find docs or JAR file for com.databricks.logging.BaseTagDefinitions.TAG_ definitions  
// Guessing TAG_BRANCH_NAME == clientBranchName
//   val clientBranchName = (tags.filter(tup => tup._1.toString.contains("clientBranchName")).map(tup => tup._2).head)
// Guessing TAG_USER_AGENT == browserUserAgent
//   val browserUserAgent = (tags.filter(tup => tup._1.toString.contains("browserUserAgent")).map(tup => tup._2).head)
// Guessing TAG_HOST_NAME == browserHostName
//   val browserHostName = (tags.filter(tup => tup._1.toString.contains("browserHostName")).map(tup => tup._2).head)

// No TAG_ matches for these - wrap in try/catch if necessary
  val sourceIpAddress = try { (tags.filter(tup => tup._1.toString.contains("sourceIpAddress")).map(tup => tup._2).head) } catch { case e: Exception => "unknown-source-ip"}
  val browserHash = try { (tags.filter(tup => tup._1.toString.contains("browserHash")).map(tup => tup._2).head) } catch { case e: Exception => "unknown-browser-hash"}

  val json = Map(
    "time" -> java.time.Instant.now.toEpochMilli,
    "username" -> username,
    "userhome" -> userhome,
    "dbrVersion" -> s"$dbrMajorVersion.$dbrMinorVersion",
    "tags" -> tags.map(tup => (tup._1.name, tup._2))
  )

  val jsonString = write(json)
  val tags_dump = write(tags.map(tup => (tup._1.name, tup._2)))
  
  val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
  
  val html = s"""
<html>
<head>
  <script src="https://files.training.databricks.com/static/js/classroom-support.min.js"></script>
  <script>
<!--  
    window.setTimeout( // Defer until bootstrap has enough time to async load
      function(){ 
          Cookies.set("_academy_username", "$username", {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_module_name", "$moduleName", {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_course_name", "$courseName", {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_sessionId", "$sessionId", {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_hostName", '$hostName', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_clusterMemory", '$clusterMemory', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_clientBranchName", '$clientBranchName', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_notebookLanguage", '$notebookLanguage', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_sourceIpAddress", '$sourceIpAddress', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_browserUserAgent", '$browserUserAgent', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_browserHostName", '$browserHostName', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_browserHash", '$browserHash', {"domain":".databricksusercontent.com"});
          Cookies.set("_academy_tags", $jsonString, {"domain":".databricksusercontent.com"});
      }, 2000
    );
-->    
  </script>
</head>
<body>
  Class Setup Complete
<script>
</script>  
</body>
</html>
"""
displayHTML(html)
  
}

def showStudentSurvey():Unit = {
  import java.net.URLEncoder.encode
  val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
  val username = encode(spark.conf.get("com.databricks.training.username", "unknown-user"), utf8)
  val courseName = encode(spark.conf.get("com.databricks.training.courseName", "unknown-course"), utf8)
  val moduleNameUnencoded = spark.conf.get("com.databricks.training.moduleName", "unknown-module")
  val moduleName = encode(moduleNameUnencoded, utf8)

  import scala.collection.Map
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization.write
  import org.json4s.JsonDSL._

  implicit val formats: DefaultFormats = DefaultFormats

  val json = Map(
    "courseName" -> courseName,
    "moduleName" -> moduleName, // || "unknown",
    "name" -> name,
    "time" -> java.time.Instant.now.toEpochMilli,
    "username" -> username,
    "userhome" -> userhome,
    "dbrVersion" -> s"$dbrMajorVersion.$dbrMinorVersion",
    "tags" -> tags.map(tup => (tup._1.name, tup._2))
  )

  val jsonString = write(json)
  val feedbackUrl = s"https://engine-prod.databricks.training/feedback/$username/$courseName/$moduleName/";
  
  val html = s"""
  <html>
  <head>
    <script src="https://files.training.databricks.com/static/js/classroom-support.min.js"></script>
    <script>
<!--    
      window.setTimeout( // Defer until bootstrap has enough time to async load
        () => { 
        //console.log($jsonString);
        //console.log(JSON.stringify("$jsonString");
          var allCookies = Cookies.get();
          Cookies.set("_academy_module_name", "$moduleName", {"domain":".databricksusercontent.com"});

          $$("#divComment").css("display", "visible");

          // Emulate radio-button like feature for multiple_choice
          $$(".multiple_choicex").on("click", (evt) => {
                const container = $$(evt.target).parent();
                $$(".multiple_choicex").removeClass("checked"); 
                $$(".multiple_choicex").removeClass("checkedRed"); 
                $$(".multiple_choicex").removeClass("checkedGreen"); 
                container.addClass("checked"); 
                if (container.hasClass("thumbsDown")) { 
                    container.addClass("checkedRed"); 
                } else { 
                    container.addClass("checkedGreen"); 
                };
                
                // Send the like/dislike before the comment is shown so we at least capture that.
                // In analysis, always take the latest feedback for a module (if they give a comment, it will resend the like/dislike)
                var json = { data: { liked: $$(".multiple_choicex.checked").attr("value"), cookies: Cookies.get() } };
                $$.ajax({
                  type: 'PUT', 
                  url: '$feedbackUrl', 
                  data: JSON.stringify(json),
                  dataType: 'json',
                  processData: false
                });
                $$("#divComment").show("fast");
          });


           // Set click handler to do a PUT
          $$("#btnSubmit").on("click", (evt) => {
              // Use .attr("value") instead of .val() - this is not a proper input box
              var json = { data: { liked: $$(".multiple_choicex.checked").attr("value"), comment: $$("#taComment").val(), cookies: Cookies.get() } };

              const msgThanks = "Thank you for your feedback!";
              const msgError = "There was an error submitting your feedback";
              const msgSending = "Sending feedback...";

              $$("#feedback").hide("fast");
              $$("#feedback-response").html(msgSending);

              $$.ajax({
                type: 'PUT', 
                url: '$feedbackUrl', 
                data: JSON.stringify(json),
                dataType: 'json',
                processData: false
              })
                .done(function() {
                  $$("#feedback-response").html(msgThanks);
                })
                .fail(function() {
                  $$("#feedback-response").html(msgError);
                })
                ; // End of .ajax chain
          });
        }, 2000
      );
-->
    </script>    
    <style>
.multiple_choicex > img:hover {    
    background-color: white;
    border-width: 0.15em;
    border-radius: 5px;
    border-style: solid;
}
.multiple_choicex.choice1 > img:hover {    
    border-color: green;
    background-color: green;
}
.multiple_choicex.choice2 > img:hover {    
    border-color: red;
    background-color: red;
}
.multiple_choicex {
    margin: 1em;
    padding: 0em;
    background-color: white;
    border: 0em;
    border-style: solid;
    border-color: green;
}
.multiple_choicex.checked {
    border: 0.15em solid black;
    background-color: white;
    border-width: 0.5em;
    border-radius: 5px;
}
.multiple_choicex.checkedGreen {
    border-color: green;
    background-color: green;
}
.multiple_choicex.checkedRed {
    border-color: red;
    background-color: red;
}
    </style>
  </head>
  <body>
    <h2 style="font-size:28px; line-height:34.3px"><img style="vertical-align:middle" src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/>What did you think?</h2>
    <p>Please let us know how if you liked this module, <b>$moduleNameUnencoded</b></p>
    <div id="feedback" style="clear:both;display:table;">
      <span class="multiple_choicex choice1 thumbsUp" value="true"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-up.png"/></span>
      <span class="multiple_choicex choice2 thumbsDown" value="false"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-down.png"/></span>
      <span>
        <div id="divComment" style="display:none">
          <textarea id="taComment" placeholder="How can we make this module better? (optional)" style="margin:1em;width:100%;height:200px;display:block"></textarea>
          <button id="btnSubmit">Send</button>
        </div>
      </span>
    </div>
    <div id="feedback-response" style="color:green; margin-top: 1em">&nbsp;</div>
  </body>
  </html>
  """
  displayHTML(html);  
}

class StudentsStatsService() extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler._

  val hostname = "engine-prod.databricks.training"

  def logEvent(eventType: String):Unit = {
    import org.apache.http.entity._
    import org.apache.http.impl.client.{HttpClients}
    import org.apache.http.client.methods.HttpPost
    import java.net.URLEncoder.encode
    import org.json4s.jackson.Serialization
    implicit val formats = org.json4s.DefaultFormats

    var client:org.apache.http.impl.client.CloseableHttpClient = null

    try {
      val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
      val username = encode(spark.conf.get("com.databricks.training.username", "unknown-user"), utf8)
      val courseName = encode(spark.conf.get("com.databricks.training.courseName", "unknown-course"), utf8)
      val moduleName = encode(spark.conf.get("com.databricks.training.moduleName", "unknown-module"), utf8)
      val event = encode(eventType, utf8)
      val url = s"https://$hostname/tracking/$courseName/$moduleName/$username/$event"
    
      val content = Map(
        "tags" -> tags.map(tup => (tup._1.name, s"$tup._2")),
        "courseName" -> courseName, 
        "moduleName" -> moduleName,
        "username" -> username,
        "eventType" -> eventType,
        "eventTime" -> s"${System.currentTimeMillis}"
      )
      
      val output = Serialization.write(content)
    
      // Future: (clues from Brian) 
      // Threadpool - don't use defaultExecutionContext; create our own EC; EC needs to be in scope as an implicit (Future calls will pick it up)
      // apply() on Future companion
      // onSuccess(), onFailure() (get exception from failure); map over future, final future, onComplete() gives Try object (can )
      //    Future {
      val client = HttpClients.createDefault()
      val httpPost = new HttpPost(url)
      val entity = new StringEntity(Serialization.write(Map("data" -> content)))      

      httpPost.setEntity(entity)
      httpPost.setHeader("Accept", "application/json")
      httpPost.setHeader("Content-type", "application/json")

      client.execute(httpPost)
      
    } catch {
      case e:Exception => org.apache.log4j.Logger.getLogger(getClass).error("Databricks Academey stats service failure", e)
      
    } finally {
      if (client != null) {
        try { client.close() } 
        catch { case _:Exception => () }
      }
    }
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = logEvent("JobEnd" + jobEnd.jobId)
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = logEvent("JobStart: " + jobStart.jobId)
}

val studentStatsService = new StudentsStatsService()
if (spark.conf.get("com.databricks.training.studentStatsService.registered", null) != "registered") {
  sc.addSparkListener(studentStatsService)
  spark.conf.set("com.databricks.training.studentStatsService", "registered")
}
studentStatsService.logEvent("Classroom-Setup")
  
//*******************************************
// CHECK FOR REQUIRED VERIONS OF SPARK & DBR
//*******************************************

assertDbrVersion(4, 0)
assertSparkVersion(2, 3)

displayHTML("Initialized classroom variables & functions...")

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC #**********************************
// MAGIC # VARIOUS UTILITY FUNCTIONS
// MAGIC #**********************************
// MAGIC
// MAGIC def assertSparkVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Spark version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC
// MAGIC def assertDbrVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Databricks Runtime (DBR) version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC
// MAGIC def assertIsMlRuntime():
// MAGIC   version = spark.conf.get("com.databricks.training.dbr.version")
// MAGIC   if "-ml-" not in version:
// MAGIC     raise Exception("This notebook must be ran on a Databricks ML Runtime, found {}.".format(version))
// MAGIC
// MAGIC     
// MAGIC #**********************************
// MAGIC # GET AZURE DATASOURCE
// MAGIC #**********************************
// MAGIC
// MAGIC
// MAGIC def getAzureDataSource(): 
// MAGIC   datasource = spark.conf.get("com.databricks.training.azure.datasource").split("\t")
// MAGIC   source = datasource[0]
// MAGIC   sasEntity = datasource[1]
// MAGIC   sasToken = datasource[2]
// MAGIC   return (source, sasEntity, sasToken)
// MAGIC
// MAGIC     
// MAGIC #**********************************
// MAGIC # GET EXPERIMENT ID
// MAGIC #**********************************
// MAGIC
// MAGIC def getExperimentId():
// MAGIC   return spark.conf.get("com.databricks.training.experimentId")
// MAGIC
// MAGIC #**********************************
// MAGIC # INIT VARIOUS VARIABLES
// MAGIC #**********************************
// MAGIC
// MAGIC username = spark.conf.get("com.databricks.training.username", "unknown-username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome", "unknown-userhome")
// MAGIC
// MAGIC import sys
// MAGIC pythonVersion = spark.conf.set("com.databricks.training.python-version", sys.version[0:sys.version.index(" ")])
// MAGIC
// MAGIC None # suppress output

// COMMAND ----------


//**********************************
// CREATE THE MOUNTS
//**********************************

def getAwsRegion():String = {
  try {
    import scala.io.Source
    import scala.util.parsing.json._

    val jsonString = Source.fromURL("http://169.254.169.254/latest/dynamic/instance-identity/document").mkString // reports ec2 info
    val map = JSON.parseFull(jsonString).getOrElse(null).asInstanceOf[Map[Any,Any]]
    map.getOrElse("region", null).asInstanceOf[String]

  } catch {
    // We will use this later to know if we are Amazon vs Azure
    case _: java.io.FileNotFoundException => null
  }
}

def getAzureRegion():String = {
  import com.databricks.backend.common.util.Project
  import com.databricks.conf.trusted.ProjectConf
  import com.databricks.backend.daemon.driver.DriverConf

  new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
}

val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
val awsAuth = s"${awsAccessKey}:${awsSecretKey}"

def getAwsMapping(region:String):(String,Map[String,String]) = {

  val MAPPINGS = Map(
    "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
    "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
    "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
    "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
    "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
    "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
    "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
    "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),

    // eu-west-3 in Paris isn't supported by Databricks yet - not supported by the current version of the AWS library
    // "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-3/common", Map[String,String]()),
    
    // Use Frankfurt in EU-Central-1 instead
    "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    
    "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
    "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
    "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
    "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
    "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
  )

  MAPPINGS.getOrElse(region, MAPPINGS("_default"))
}

def getAzureMapping(region:String):(String,Map[String,String]) = {

  var MAPPINGS = Map(
    "australiacentral"    -> ("dbtrainaustraliasoutheas",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiacentral2"   -> ("dbtrainaustraliasoutheas",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiaeast"       -> ("dbtrainaustraliaeast",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=FM6dy59nmw3f4cfN%2BvB1cJXVIVz5069zHmrda5gZGtU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiasoutheast"  -> ("dbtrainaustraliasoutheas",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "canadacentral"       -> ("dbtraincanadacentral",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=dwAT0CusWjvkzcKIukVnmFPTmi4JKlHuGh9GEx3OmXI%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "canadaeast"          -> ("dbtraincanadaeast",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SYmfKBkbjX7uNDnbSNZzxeoj%2B47PPa8rnxIuPjxbmgk%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "centralindia"        -> ("dbtraincentralindia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=afrYm3P5%2BB4gMg%2BKeNZf9uvUQ8Apc3T%2Bi91fo/WOZ7E%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "centralus"           -> ("dbtraincentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=As9fvIlVMohuIV8BjlBVAKPv3C/xzMRYR1JAOB%2Bbq%2BQ%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastasia"            -> ("dbtraineastasia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=sK7g5pki8bE88gEEsrh02VGnm9UDlm55zTfjZ5YXVMc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastus"              -> ("dbtraineastus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=tlw5PMp1DMeyyBGTgZwTbA0IJjEm83TcCAu08jCnZUo%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastus2"             -> ("dbtraineastus2",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=Y6nGRjkVj6DnX5xWfevI6%2BUtt9dH/tKPNYxk3CNCb5A%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "japaneast"           -> ("dbtrainjapaneast",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=q6r9MS/PC9KLZ3SMFVYO94%2BfM5lDbAyVsIsbBKEnW6Y%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "japanwest"           -> ("dbtrainjapanwest",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=M7ic7/jOsg/oiaXfo8301Q3pt9OyTMYLO8wZ4q8bko8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northcentralus"      -> ("dbtrainnorthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northcentralus"      -> ("dbtrainnorthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northeurope"         -> ("dbtrainnortheurope",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=35yfsQBGeddr%2BcruYlQfSasXdGqJT3KrjiirN/a3dM8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southcentralus"      -> ("dbtrainsouthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southcentralus"      -> ("dbtrainsouthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southindia"          -> ("dbtrainsouthindia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=0X0Ha9nFBq8qkXEO0%2BXd%2B2IwPpCGZrS97U4NrYctEC4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southeastasia"       -> ("dbtrainsoutheastasia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=H7Dxi1yqU776htlJHbXd9pdnI35NrFFsPVA50yRC9U0%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "uksouth"             -> ("dbtrainuksouth",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SPAI6IZXmm%2By/WMSiiFVxp1nJWzKjbBxNc5JHUz1d1g%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "ukwest"              -> ("dbtrainukwest",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=olF4rjQ7V41NqWRoK36jZUqzDBz3EsyC6Zgw0QWo0A8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westcentralus"       -> ("dbtrainwestcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=UP0uTNZKMCG17IJgJURmL9Fttj2ujegj%2BrFN%2B0OszUE%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westeurope"          -> ("dbtrainwesteurope",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=csG7jGsNFTwCArDlsaEcU4ZUJFNLgr//VZl%2BhdSgEuU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westindia"           -> ("dbtrainwestindia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=fI6PNZ7YvDGKjArs1Et2rAM2zgg6r/bsKEjnzQxgGfA%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westus"              -> ("dbtrainwestus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=%2B1XZDXbZqnL8tOVsmRtWTH/vbDAKzih5ThvFSZMa3Tc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westus2"             -> ("dbtrainwestus2",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "_default"            -> ("dbtrainwestus2",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z")
  )

  val (account: String, sasKey: String) = MAPPINGS.getOrElse(region, MAPPINGS("_default"))

  val blob = "training"
  val source = s"wasbs://$blob@$account.blob.core.windows.net/"
  val configMap = Map(
    s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
  )

  (source, configMap)
}

def mountFailed(msg:String): Unit = {
  println(msg)
}

def retryMount(source: String, mountPoint: String): Unit = {
  try { 
    // Mount with IAM roles instead of keys for PVC
    dbutils.fs.mount(source, mountPoint)
  } catch {
    case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
  try {
    dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
  } catch {
    case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
    case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def autoMount(fix:Boolean = false, failFast:Boolean = false, mountDir:String = "/mnt/training"): Unit = {
  var awsRegion = getAwsRegion()

  val (source, extraConfigs) = if (awsRegion != null)  {
    spark.conf.set("com.databricks.training.region.name", awsRegion)
    getAwsMapping(awsRegion)

  } else {
    val azureRegion = getAzureRegion()
    spark.conf.set("com.databricks.training.region.name", azureRegion)
    initAzureDataSource(azureRegion)
  }
  
  val resultMsg = mountSource(fix, failFast, mountDir, source, extraConfigs)
  displayHTML(resultMsg)
}

def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
  val mapping = getAzureMapping(azureRegion)
  val (source, config) = mapping
  val (sasEntity, sasToken) = config.head

  val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
  spark.conf.set("com.databricks.training.azure.datasource", datasource)

  return mapping
}

def mountSource(fix:Boolean, failFast:Boolean, mountDir:String, source:String, extraConfigs:Map[String,String]): String = {
  val mntSource = source.replace(awsAuth+"@", "")

  if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
    val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
    if (mount.source == mntSource) {
      return s"""Datasets are already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""
      
    } else if (failFast) {
      throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
      
    } else if (fix) {
      println(s"Unmounting existing datasets ($mountDir from $mntSource)")
      dbutils.fs.unmount(mountDir)
      mountSource(fix, failFast, mountDir, source, extraConfigs)

    } else {
      return s"""<b style="color:red">Invalid Mounts!</b></br>
                      <ul>
                      <li>The training datasets you are using are from an unexpected source</li>
                      <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
                      <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
                      <ol>
                        <li>Insert a new cell after this one</li>
                        <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
                        <li>Verify that the problem has been resolved.</li>
                      </ol>"""
    }
  } else {
    println(s"""Mounting datasets to $mountDir from $mntSource""")
    mount(source, extraConfigs, mountDir)
    return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
  }
}

def fixMounts(): Unit = {
  autoMount(true)
}

autoMount(true)
