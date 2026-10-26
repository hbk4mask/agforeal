"""Generate reviewable BigQuery analysis and Spark 3.5+ Scala cleaning artifacts."""
import re
from .engine import FIELDS, OS, TYPES, ALLOWED, policy_with_defaults, InputError

def literal(value):
    return "'" + value.replace("'", "''") + "'"

def column(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise InputError('Code generation requires simple column names (letters, digits, underscores)')
    return '`'+value+'`'

def mapping_case(field, values):
    return 'CASE '+field+' '+ ' '.join('WHEN '+literal(k)+' THEN '+literal(v) for k,v in values.items())+' END'

def ctes(mapping, policy, dialect, source):
    bq=dialect=='bigquery'
    cast_type='STRING'
    raw=[]
    for f in FIELDS:
        expr='CAST('+column(mapping[f])+' AS '+cast_type+')' if mapping.get(f) else 'CAST(NULL AS STRING)'
        raw.append(f"CASE WHEN LOWER(TRIM({expr})) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM({expr}) END AS {f}")
    extra='' if bq else ', _source_row, _raw_json'
    raw_sql='SELECT '+',\n  '.join(raw)+extra+' FROM '+source
    regex=lambda field,pattern: (f'REGEXP_CONTAINS({field}, r"{pattern}")' if bq else f"{field} RLIKE r'{pattern}'")
    uuid_pattern='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    mail_pattern='^[^ @]+@[^ @.]+([.][^ @.]+)+$'
    if bq:
        ip='NET.IP_TO_STRING(NET.SAFE_IP_FROM_STRING(ip))'
        number='SAFE_CAST(timestamp AS FLOAT64)'
    else:
        ip='normalize_ip(ip)'
        number='try_cast(timestamp AS DOUBLE)'
    scales={'s':'1000000','ms':'1000','us':'1','ns':'0.001'}
    scale=scales.get(policy['timestamp_unit'],f'CASE WHEN ABS({number}) < 1e11 THEN 1000000 WHEN ABS({number}) < 1e14 THEN 1000 WHEN ABS({number}) < 1e17 THEN 1 ELSE 0.001 END')
    micro=f'{number} * ({scale})'
    timestamp=(f"CASE WHEN {number} IS NOT NULL THEN SAFE.TIMESTAMP_MICROS(SAFE_CAST({micro} AS INT64)) ELSE SAFE_CAST(timestamp AS TIMESTAMP) END" if bq else f"CASE WHEN {number} IS NOT NULL THEN CASE WHEN {micro} BETWEEN -62135596800000000 AND 253402300799999999 THEN timestamp_micros(try_cast({micro} AS BIGINT)) END ELSE try_to_timestamp(timestamp) END")
    values={'maid':f"CASE WHEN {regex('maid',uuid_pattern)} AND LOWER(maid) <> '00000000-0000-0000-0000-000000000000' THEN LOWER(maid) END",'ip':ip,'email':f"CASE WHEN LENGTH(email) <= 254 AND {regex('email',mail_pattern)} AND NOT {regex('email', chr(92)+'s')} THEN LOWER(email) END",'device_type':mapping_case('LOWER(device_type)',TYPES),'device_os':mapping_case('LOWER(device_os)',OS),'timestamp':timestamp}
    normalized='SELECT '+',\n  '.join(f'{values[f]} AS {f}, {f} AS raw_{f}' for f in FIELDS)+extra+' FROM raw_values'
    mismatch=' OR '.join(f"(device_os = '{os}' AND device_type NOT IN ({','.join(literal(t) for t in sorted(types))}))" for os,types in ALLOWED.items())
    # Missing one of the device attributes is not itself a contradiction.
    mismatch=f'COALESCE(({mismatch}), FALSE)'
    repair="CASE WHEN device_os = 'ipados' THEN 'tablet' WHEN device_os IN ('macos','linux') THEN 'desktop' ELSE device_type END" if policy['mismatch_action']=='repair' else 'device_type'
    repair = f'CASE WHEN {mismatch} THEN {repair} ELSE device_type END'
    repairable="device_os IN ('ipados','macos','linux')" if policy['mismatch_action']=='repair' else 'FALSE'
    flags=[f"raw_{f} IS NOT NULL AND {f} IS NULL AS invalid_{f}" for f in ['maid','ip','email','device_type','device_os']]
    flags+=['timestamp IS NULL AS invalid_timestamp',f'{mismatch} AND NOT ({repairable}) AS device_os_type_mismatch',f'{mismatch} AND ({repairable}) AS repaired_device_type']
    flags += [('maid IS NULL AND ip IS NULL AND email IS NULL' if policy['require_identifier'] else 'FALSE')+' AS missing_identifier']
    checked='SELECT '+', '.join(repair+' AS device_type' if f=='device_type' else f for f in FIELDS)+extra+', '+',\n  '.join(flags)+' FROM normalized'
    window=', DATE(timestamp) AS event_day' if policy['window']=='day' else ''
    groupday=', DATE(timestamp)' if window else ''
    dayjoin=' AND DATE(c.timestamp) = r{n}.event_day' if window else ''
    runners=[('maid','ip','max_ips_per_maid','high_ips_per_maid'),('ip','maid','max_maids_per_ip','high_maids_per_ip'),('email','maid','max_maids_per_email','high_maids_per_email')]
    parts=[('raw_values',raw_sql),('normalized',normalized),('checked',checked)]
    joins=[]; runnerflags=[]
    for n,(key,value,setting,reason) in enumerate(runners):
        parts.append((f'runner_{n}',f'SELECT {key}{window}, COUNT(DISTINCT {value}) AS fanout FROM checked WHERE {key} IS NOT NULL GROUP BY {key}{groupday}'))
        joins.append(f'LEFT JOIN runner_{n} r{n} ON c.{key} = r{n}.{key}'+dayjoin.format(n=n))
        runnerflags.append(f'COALESCE(r{n}.fanout > {policy[setting]}, FALSE) AS {reason}')
    parts.append(('flagged','SELECT c.*, '+', '.join(runnerflags)+' FROM checked c\n'+'\n'.join(joins)))
    return 'WITH '+',\n'.join(name+' AS (\n'+sql+'\n)' for name,sql in parts)

REASONS=['invalid_maid','invalid_ip','invalid_email','invalid_device_type','invalid_device_os','invalid_timestamp','device_os_type_mismatch','missing_identifier','high_ips_per_maid','high_maids_per_ip','high_maids_per_email']

def generate_bigquery(table, mapping, policy):
    if not re.fullmatch(r'[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+',table):
        raise InputError('BigQuery table must be project.dataset.table')
    policy=policy_with_defaults(policy)
    base=ctes(mapping,policy,'bigquery','`'+table+'`')
    cols=',\n'.join("STRUCT('"+f+"' AS field, (SELECT COUNT(DISTINCT "+f+") FROM raw_values) AS distinct_values, (SELECT COUNTIF("+f+" IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT "+f+") FROM normalized) AS normalized_distinct_values)" for f in FIELDS)
    issues=',\n'.join(f"STRUCT('{r}' AS rule, COUNTIF({r}) AS affected_rows)" for r in REASONS)
    return '-- Standard SQL: exact full-table profiling. Read-only; review estimated bytes before execution.\n-- No identifier samples are returned. Fanout counts precede filtering and deduplication.\n'+base+f'''\nSELECT COUNT(*) AS total_rows,
 COUNTIF(NOT ({' OR '.join(REASONS)})) AS valid_rows_before_deduplication,
 COUNTIF(repaired_device_type) AS repairable_device_rows,
 [{cols}] AS columns,
 [{issues}] AS issues
FROM flagged;
'''

def generate_scala(mapping, policy):
    policy=policy_with_defaults(policy)
    query=ctes(mapping,policy,'spark','source_input')+'\nSELECT * FROM flagged'
    rule_array=',\n      '.join(f'when(col("{r}"), lit("{r}"))' for r in REASONS)
    dedup = """val shouldDeduplicate = if (args.length == 4) args(3).toBoolean else """ + str(policy['deduplicate']).lower() + """
    val (clean, quarantine) = if (shouldDeduplicate) {
      val ranking = Window.partitionBy(canonical.map(col): _*).orderBy(col("_source_row"))
      val ranked = eligible.withColumn("_rank", row_number().over(ranking))
      val accepted = ranked.filter(col("_rank") === 1).select(canonical.map(col): _*)
      val duplicates = ranked.filter(col("_rank") > 1).withColumn("_rejection_reasons", array(lit("duplicate_row"))).select(quarantineColumns.map(col): _*)
      (accepted, invalid.select(quarantineColumns.map(col): _*).unionByName(duplicates))
    } else {
      (eligible.select(canonical.map(col): _*), invalid.select(quarantineColumns.map(col): _*))
    }"""
    mismatch_check = ' OR '.join(f"(device_os = '{os}' AND device_type NOT IN ({','.join(literal(t) for t in sorted(types))}))" for os,types in ALLOWED.items())
    return '''// Spark 3.5.x, Scala binary version must match the runtime (GCP 2.2: Scala 2.13). Build with your cluster's matching Spark version.
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
    val runtimeQuery = if (args.length == 4) spark.read.textFile(args(2)).collect().mkString("\\n") else """'''+query+'''"""
    val flagged = spark.sql(runtimeQuery).cache()
    val canonical = Seq("maid", "ip", "email", "device_type", "device_os", "timestamp")
    val scored = flagged.withColumn("_rejection_reasons", filter(array(
      '''+rule_array+'''
    ), x => x.isNotNull)).cache()
    val eligible = scored.filter(size(col("_rejection_reasons")) === 0)
    val invalid = scored.filter(size(col("_rejection_reasons")) > 0)
    val quarantineColumns = Seq("_source_row", "_raw_json", "_rejection_reasons")
    '''+dedup+'''
    clean.cache(); quarantine.cache()
    val acceptedCount = clean.count(); val rejectedCount = quarantine.count()
    require(acceptedCount + rejectedCount == sourceCount, "Row conservation failed")
    require(clean.filter(expr("""'''+mismatch_check+'''""")).count() == 0, "Post-cleaning device consistency check failed")
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
'''
