const cloudConfigFields=['project','dataset','location','region','runtime','service_account','subnet','jar_uri','staging_uri','maximum_bytes_billed','ttl_seconds'];
let cloudPlan=null, cloudWorking=false, managedCloud=false;
function invalidateCloud(){cloudPlan=null;$('run-cloud').disabled=true;$('cloud-plan').hidden=true}
function cloudMessage(text,error=false){$('cloud-test-result').textContent=text;message(text,error)}
function cloudBusy(value){cloudWorking=value;for(const id of ['save-cloud','test-bigquery','test-spark','prepare-cloud','refresh-cloud','register-source-table'])$(id).disabled=value;$('run-cloud').disabled=value||!cloudPlan;document.querySelectorAll('#connectors input,#connectors select').forEach(e=>e.disabled=value||(managedCloud&&e.id.startsWith('gcp-')))}
async function cloudAction(fn){cloudBusy(true);try{await fn()}catch(e){cloudMessage(e.message,true)}finally{cloudBusy(false)}}
for(const f of fields){const label=document.createElement('label');label.textContent=f;const input=document.createElement('input');input.id='cloud-map-'+f;input.value=f;input.placeholder='Source column, or blank if absent';input.oninput=invalidateCloud;label.append(input);$('cloud-mapping').append(label)}
function cloudSettings(){const config={};for(const key of cloudConfigFields){config[key]=$('gcp-'+key).value.trim();if(['maximum_bytes_billed','ttl_seconds'].includes(key)){config[key]=Number(config[key]);if(!Number.isSafeInteger(config[key])||config[key]<1)throw Error('Billing cap and batch timeout must be positive integers.')}}return config}
async function saveCloud(){await cloudConnectionReady;if(managedCloud)return;if(!/^[a-z][a-z0-9-]{4,61}[a-z0-9]$/.test($('gcp-project').value.trim()))throw Error('Enter a valid GCP project ID before saving or testing.');const result=await api('/api/cloud/connection',cloudSettings());$('cloud-connection-state').textContent='SAVED · ACCESS UNTESTED';return result}
$('save-cloud').onclick=()=>cloudAction(async()=>{invalidateCloud();await saveCloud();cloudMessage('Connection configuration saved. Test each connector to verify access.')});
for(const backend of ['bigquery','spark'])$('test-'+backend).onclick=()=>cloudAction(async()=>{invalidateCloud();await saveCloud();cloudMessage('Testing '+backend+' access…');const result=await api('/api/cloud/test/'+backend,{});cloudMessage(result.message);$('cloud-connection-state').textContent=backend.toUpperCase()+' ACCESS VERIFIED'});
for(const key of cloudConfigFields)$('gcp-'+key).oninput=()=>{invalidateCloud();$('cloud-connection-state').textContent='UNSAVED CHANGES'};
for(const id of ['cloud-table','cloud-input','cloud-output'])$(id).oninput=invalidateCloud;
$('cloud-backend').onchange=()=>{invalidateCloud();const spark=$('cloud-backend').value==='spark';$('cloud-table-label').hidden=spark;$('cloud-input-label').hidden=!spark;$('cloud-output-label').hidden=!spark;$('cloud-explanation').textContent=spark?'Prepare builds a batch request. Run uploads this policy to GCS and starts a billable serverless Spark batch using your runner JAR. The workload identity needs access to input, staging, JAR and output objects.':'Prepare performs a BigQuery dry run and estimates bytes processed. Run submits a billable analysis query with your configured cap.'};
document.querySelectorAll('#policy input,#policy select').forEach(el=>el.addEventListener('change',invalidateCloud));
async function prepareCloudJob(){invalidateCloud();await saveCloud();const body={backend:$('cloud-backend').value,table:$('cloud-table').value.trim(),input_uri:$('cloud-input').value.trim(),output_uri:$('cloud-output').value.trim(),policy:policy(),mapping:Object.fromEntries(fields.map(f=>[f,$('cloud-map-'+f).value.trim()||null]))};cloudMessage('Preparing '+body.backend+' job…');cloudPlan=await api('/api/cloud/prepare',body);$('cloud-plan').hidden=false;$('cloud-plan-summary').textContent=cloudPlan.backend==='bigquery'?`Dry run passed · ${cloudPlan.estimated_bytes.toLocaleString()} bytes estimated · ${cloudPlan.config.maximum_bytes_billed.toLocaleString()} byte cap`:`Prepared batch ${cloudPlan.job_id} · timeout ${cloudPlan.config.ttl_seconds}s`;$('cloud-plan-code').textContent=cloudPlan.sql+'\n\n'+JSON.stringify(cloudPlan.payload||{project:cloudPlan.config.project,location:cloudPlan.config.location,policy:cloudPlan.policy,mapping:cloudPlan.mapping},null,2);cloudMessage('Prepared a fixed snapshot of the current settings. Review it, then run.');}
$('prepare-cloud').onclick=()=>cloudAction(prepareCloudJob);
async function listCloudJobs(selected){const jobs=await api('/api/cloud/jobs');$('cloud-job-select').replaceChildren(new Option('Choose a submitted job',''));jobs.reverse().forEach(j=>$('cloud-job-select').add(new Option(`${j.backend} · ${j.job_id} · ${j.status}`,j.id)));if(selected)$('cloud-job-select').value=selected}
async function refreshCloudJob(){const id=$('cloud-job-select').value;if(!id)return;const result=await api('/api/cloud/jobs/'+id);$('cloud-job-state').textContent=result.status.toUpperCase();$('cloud-console').hidden=!result.console_url;if(result.console_url)$('cloud-console').href=result.console_url;$('cloud-output-location').textContent=result.output_uri?'Output: '+result.output_uri:'';$('cloud-job-result').textContent=JSON.stringify({status:result.status,job_id:result.job_id,report:result.report,error:result.error,message:result.message,statistics:result.statistics,runtime_info:result.runtime_info,policy:result.policy,mapping:result.mapping},null,2);}
$('run-cloud').onclick=()=>cloudAction(async()=>{if(!cloudPlan)return;const result=await api('/api/cloud/submit',{plan_id:cloudPlan.id});cloudPlan=null;$('run-cloud').disabled=true;await listCloudJobs(result.id);$('cloud-job-state').textContent=result.status.toUpperCase();cloudMessage('Job submitted to GCP. Refresh status to track its progress.');await refreshCloudJob()});
$('refresh-cloud').onclick=()=>cloudAction(async()=>{const selected=$('cloud-job-select').value;await listCloudJobs(selected);await refreshCloudJob()});$('cloud-job-select').onchange=()=>cloudAction(refreshCloudJob);
const cloudConnectionReady=(async()=>{try{const value=await api('/api/cloud/connection');managedCloud=!!value.managed;if(value.configured){for(const k of cloudConfigFields)if(value.settings[k]!==undefined)$('gcp-'+k).value=value.settings[k];$('cloud-connection-state').textContent='SAVED · ACCESS UNTESTED'}if(managedCloud){document.querySelector('#connectors details').hidden=true;$('save-cloud').hidden=true;for(const k of cloudConfigFields)$('gcp-'+k).disabled=true;$('cloud-connection-state').textContent='CLOUD RUN SERVICE IDENTITY';$('cloud-test-result').textContent='Settings are managed at deployment. No credential setup is needed here.';document.querySelector('[data-tab=connectors] span').textContent='Cloud jobs';}await listCloudJobs()}catch(e){cloudMessage(e.message,true);throw e}})();

(async()=>{try{const runner=await api("/api/cloud/runner");$("download-runner").hidden=!runner.ready}catch(e){$("download-runner").hidden=true}})();

async function createSourceTableAndPrepare(){
  if(cloudWorking||busy)return;
  switchTab('connectors');
  if(!gcsSource&&!source){cloudMessage('Select a GCS source path or upload a Parquet sample first.',true);return;}
  const registration=gcsSource?{uri:gcsSource.uri}:{source_id:source.id};
  setBusy(true);
  try { await cloudAction(async()=>{
    invalidateCloud();await saveCloud();
    cloudMessage(registration.source_id?'Staging the uploaded sample in GCS and creating its BigQuery table…':'Creating the BigQuery external table and detecting its Parquet schema…');
    const result=await api('/api/cloud/sources',registration);
    $('cloud-input').value=result.uri;$('cloud-table').value=result.table;$('table').value=result.table;
    $('cloud-backend').value='bigquery';$('cloud-backend').onchange();
    for(const f of fields)$('cloud-map-'+f).value=result.mapping[f]||'';
    $('registered-source-info').textContent=(result.created?'Created ':'Reused ')+result.table+' · '+result.schema.length+' columns · '+result.uri;
    if(gcsSource)gcsSource.table=result.table;
    if(result.warnings.length||!result.mapping.timestamp||!['maid','ip','email'].some(f=>result.mapping[f])){
      cloudMessage('Table is ready. Review the detected source mappings, then select Prepare & validate. '+result.warnings.join(' '));return;
    }
    await prepareCloudJob();
    cloudMessage('Source table is ready and analysis has been prepared. Review the estimate and select Run prepared job.');
  }); } finally { setBusy(false); }
}
$('register-source-table').onclick=createSourceTableAndPrepare;
