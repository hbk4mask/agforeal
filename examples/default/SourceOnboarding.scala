// Spark 3.5.x, Scala binary version must match the runtime (GCP 2.2: Scala 2.13). Build with your cluster's matching Spark version.
// Arguments: <input-parquet-uri> <new-output-uri>. Never overwrites a destination.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SourceOnboarding {
  def main(args: Array[String]): Unit = {
    require(args.length == 2 || args.length == 4, "Expected input, output, optional runtime SQL URI and dedup flag")
    val spark = SparkSession.builder.appName("Source onboarding").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    // Lexical guards prevent hostnames from reaching the IP parser (no DNS lookup).
    spark.udf.register("normalize_ip", (value: String) => {
      if (value == null) null
      else try {
        if (value.matches("[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+")) {
          val parts = value.split("[.]")
          if (parts.forall(p => p.toInt <= 255 && (p == "0" || !p.startsWith("0")))) parts.map(_.toInt).mkString(".") else null
        } else if (value.contains(":") && value.matches("[0-9a-fA-F:.]+")) {
          java.net.InetAddress.getByName(value).getHostAddress.toLowerCase
        } else null
      } catch { case _: Exception => null }
    })
    val input = spark.read.parquet(args(0))
    require(!input.columns.exists(c => Set("_source_row", "_raw_json").contains(c)), "Reserved source column")
    val source = input.withColumn("_raw_json", to_json(struct(input.columns.map(col): _*)))
      .withColumn("_source_row", monotonically_increasing_id()).cache()
    val sourceCount = source.count()
    source.createOrReplaceTempView("source_input")
    val runtimeQuery = if (args.length == 4) spark.read.textFile(args(2)).collect().mkString("\n") else """WITH raw_values AS (
SELECT CASE WHEN LOWER(TRIM(CAST(`maid` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`maid` AS STRING)) END AS maid,
  CASE WHEN LOWER(TRIM(CAST(`ip` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`ip` AS STRING)) END AS ip,
  CASE WHEN LOWER(TRIM(CAST(`email` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`email` AS STRING)) END AS email,
  CASE WHEN LOWER(TRIM(CAST(`device_type` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`device_type` AS STRING)) END AS device_type,
  CASE WHEN LOWER(TRIM(CAST(`device_os` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`device_os` AS STRING)) END AS device_os,
  CASE WHEN LOWER(TRIM(CAST(`timestamp` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`timestamp` AS STRING)) END AS timestamp, _source_row, _raw_json FROM source_input
),
normalized AS (
SELECT CASE WHEN maid RLIKE r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' AND LOWER(maid) <> '00000000-0000-0000-0000-000000000000' THEN LOWER(maid) END AS maid, maid AS raw_maid,
  normalize_ip(ip) AS ip, ip AS raw_ip,
  CASE WHEN LENGTH(email) <= 254 AND email RLIKE r'^[^ @]+@[^ @.]+([.][^ @.]+)+$' AND NOT email RLIKE r'\s' THEN LOWER(email) END AS email, email AS raw_email,
  CASE LOWER(device_type) WHEN 'phone' THEN 'mobile' WHEN 'smartphone' THEN 'mobile' WHEN 'mobile' THEN 'mobile' WHEN 'tablet' THEN 'tablet' WHEN 'ipad' THEN 'tablet' WHEN 'desktop' THEN 'desktop' WHEN 'pc' THEN 'desktop' WHEN 'laptop' THEN 'desktop' WHEN 'tv' THEN 'tv' WHEN 'smart tv' THEN 'tv' END AS device_type, device_type AS raw_device_type,
  CASE LOWER(device_os) WHEN 'iphone' THEN 'ios' WHEN 'ipad' THEN 'ipados' WHEN 'ipad os' THEN 'ipados' WHEN 'ipados' THEN 'ipados' WHEN 'ios' THEN 'ios' WHEN 'android' THEN 'android' WHEN 'windows' THEN 'windows' WHEN 'win' THEN 'windows' WHEN 'mac' THEN 'macos' WHEN 'mac os' THEN 'macos' WHEN 'osx' THEN 'macos' WHEN 'macos' THEN 'macos' WHEN 'linux' THEN 'linux' END AS device_os, device_os AS raw_device_os,
  CASE WHEN try_cast(timestamp AS DOUBLE) IS NOT NULL THEN CASE WHEN try_cast(timestamp AS DOUBLE) * (CASE WHEN ABS(try_cast(timestamp AS DOUBLE)) < 1e11 THEN 1000000 WHEN ABS(try_cast(timestamp AS DOUBLE)) < 1e14 THEN 1000 WHEN ABS(try_cast(timestamp AS DOUBLE)) < 1e17 THEN 1 ELSE 0.001 END) BETWEEN -62135596800000000 AND 253402300799999999 THEN timestamp_micros(try_cast(try_cast(timestamp AS DOUBLE) * (CASE WHEN ABS(try_cast(timestamp AS DOUBLE)) < 1e11 THEN 1000000 WHEN ABS(try_cast(timestamp AS DOUBLE)) < 1e14 THEN 1000 WHEN ABS(try_cast(timestamp AS DOUBLE)) < 1e17 THEN 1 ELSE 0.001 END) AS BIGINT)) END ELSE try_to_timestamp(timestamp) END AS timestamp, timestamp AS raw_timestamp, _source_row, _raw_json FROM raw_values
),
checked AS (
SELECT maid, ip, email, CASE WHEN COALESCE(((device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))), FALSE) THEN device_type ELSE device_type END AS device_type, device_os, timestamp, _source_row, _raw_json, raw_maid IS NOT NULL AND maid IS NULL AS invalid_maid,
  raw_ip IS NOT NULL AND ip IS NULL AS invalid_ip,
  raw_email IS NOT NULL AND email IS NULL AS invalid_email,
  raw_device_type IS NOT NULL AND device_type IS NULL AS invalid_device_type,
  raw_device_os IS NOT NULL AND device_os IS NULL AS invalid_device_os,
  timestamp IS NULL AS invalid_timestamp,
  COALESCE(((device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))), FALSE) AND NOT (FALSE) AS device_os_type_mismatch,
  COALESCE(((device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))), FALSE) AND (FALSE) AS repaired_device_type,
  maid IS NULL AND ip IS NULL AND email IS NULL AS missing_identifier FROM normalized
),
runner_0 AS (
SELECT maid, COUNT(DISTINCT ip) AS fanout FROM checked WHERE maid IS NOT NULL GROUP BY maid
),
runner_1 AS (
SELECT ip, COUNT(DISTINCT maid) AS fanout FROM checked WHERE ip IS NOT NULL GROUP BY ip
),
runner_2 AS (
SELECT email, COUNT(DISTINCT maid) AS fanout FROM checked WHERE email IS NOT NULL GROUP BY email
),
flagged AS (
SELECT c.*, COALESCE(r0.fanout > 1000, FALSE) AS high_ips_per_maid, COALESCE(r1.fanout > 20000, FALSE) AS high_maids_per_ip, COALESCE(r2.fanout > 30000, FALSE) AS high_maids_per_email FROM checked c
LEFT JOIN runner_0 r0 ON c.maid = r0.maid
LEFT JOIN runner_1 r1 ON c.ip = r1.ip
LEFT JOIN runner_2 r2 ON c.email = r2.email
)
SELECT * FROM flagged"""
    val flagged = spark.sql(runtimeQuery).cache()
    val canonical = Seq("maid", "ip", "email", "device_type", "device_os", "timestamp")
    val scored = flagged.withColumn("_rejection_reasons", filter(array(
      when(col("invalid_maid"), lit("invalid_maid")),
      when(col("invalid_ip"), lit("invalid_ip")),
      when(col("invalid_email"), lit("invalid_email")),
      when(col("invalid_device_type"), lit("invalid_device_type")),
      when(col("invalid_device_os"), lit("invalid_device_os")),
      when(col("invalid_timestamp"), lit("invalid_timestamp")),
      when(col("device_os_type_mismatch"), lit("device_os_type_mismatch")),
      when(col("missing_identifier"), lit("missing_identifier")),
      when(col("high_ips_per_maid"), lit("high_ips_per_maid")),
      when(col("high_maids_per_ip"), lit("high_maids_per_ip")),
      when(col("high_maids_per_email"), lit("high_maids_per_email"))
    ), x => x.isNotNull)).cache()
    val eligible = scored.filter(size(col("_rejection_reasons")) === 0)
    val invalid = scored.filter(size(col("_rejection_reasons")) > 0)
    val quarantineColumns = Seq("_source_row", "_raw_json", "_rejection_reasons")
    val shouldDeduplicate = if (args.length == 4) args(3).toBoolean else true
    val (clean, quarantine) = if (shouldDeduplicate) {
      val ranking = Window.partitionBy(canonical.map(col): _*).orderBy(col("_source_row"))
      val ranked = eligible.withColumn("_rank", row_number().over(ranking))
      val accepted = ranked.filter(col("_rank") === 1).select(canonical.map(col): _*)
      val duplicates = ranked.filter(col("_rank") > 1).withColumn("_rejection_reasons", array(lit("duplicate_row"))).select(quarantineColumns.map(col): _*)
      (accepted, invalid.select(quarantineColumns.map(col): _*).unionByName(duplicates))
    } else {
      (eligible.select(canonical.map(col): _*), invalid.select(quarantineColumns.map(col): _*))
    }
    clean.cache(); quarantine.cache()
    val acceptedCount = clean.count(); val rejectedCount = quarantine.count()
    require(acceptedCount + rejectedCount == sourceCount, "Row conservation failed")
    require(clean.filter(expr("""(device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))""")).count() == 0, "Post-cleaning device consistency check failed")
    // Stage in a new destination; consumers must wait for _ONBOARDING_COMPLETE.
    val destination = new org.apache.hadoop.fs.Path(args(1))
    val fs = destination.getFileSystem(spark.sparkContext.hadoopConfiguration)
    require(!fs.exists(destination), "Output destination already exists")
    clean.write.mode("errorifexists").parquet(args(1) + "/clean")
    quarantine.write.mode("errorifexists").parquet(args(1) + "/quarantine")
    import spark.implicits._
    Seq((sourceCount, acceptedCount, rejectedCount)).toDF("source_rows", "accepted_rows", "quarantined_rows")
      .write.mode("errorifexists").json(args(1) + "/report/counts")
    quarantine.select(explode(col("_rejection_reasons")).as("rule")).groupBy("rule").count()
      .write.mode("errorifexists").json(args(1) + "/report/issues")
    val profiles = canonical.map { field =>
      clean.agg(countDistinct(col(field)).as("distinct_values"), sum(when(col(field).isNull, 1L).otherwise(0L)).as("null_count"))
        .withColumn("field", lit(field))
    }.reduce(_.unionByName(_))
    profiles.write.mode("errorifexists").json(args(1) + "/report/after")
    val done = fs.create(new org.apache.hadoop.fs.Path(args(1) + "/_ONBOARDING_COMPLETE"), false)
    done.close()
    spark.stop()
  }
}
