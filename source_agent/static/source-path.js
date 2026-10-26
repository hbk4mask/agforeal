// Selecting a remote path never downloads records or starts a cloud job.
function setSourceMode(mode){
  const remote=mode==='gcs';
  $('drop').hidden=remote;
  $('gcs-source-form').hidden=!remote;
  $('create-upload-table').hidden=remote;
  for(const [id,selected] of [['source-local-mode',!remote],['source-gcs-mode',remote]]){
    $(id).classList.toggle('selected',selected);$(id).setAttribute('aria-pressed',String(selected));
  }
}
$('source-local-mode').onclick=()=>{setSourceMode('local');if(gcsSource){gcsSource=null;$('analyze').disabled=true;$('analyze').textContent='Analyze local source →';$('scope').textContent='NO SOURCE SELECTED';$('agent-status').textContent='READY';$('health-description').textContent='Your full-file assessment will appear after analysis.';}};
$('source-gcs-mode').onclick=()=>setSourceMode('gcs');
function continueWithGCS(){
  if(gcsSource)createSourceTableAndPrepare();
}
$('use-gcs-source').onclick=()=>{
  if(busy)return;
  const uri=$('source-gcs-uri').value.trim();
  const table='';
  if(!/^gs:\/\/[a-z0-9][a-z0-9._-]+\/\S+$/.test(uri)){
    $('gcs-source-status').textContent='Enter a gs://bucket/path URI, such as gs://your-bucket/events/*.parquet.';return;
  }
  invalidate();invalidateCloud();source=null;gcsSource={uri,table};mappingUI();
  $('source-title').textContent='Bring your next source';
  $('source-detail').textContent='Drop a Parquet file here, or click to browse';
  $('scope').textContent='GCS PATH SELECTED';$('agent-status').textContent='CLOUD SETUP';
  $('health-description').textContent='Cloud source selected. Prepare and run a cloud job to inspect its data.';
  $('gcs-source-status').textContent='Selected: '+uri+' · access and schema not yet verified.';
  $('analyze').disabled=false;$('analyze').textContent='Create table & prepare analysis →';
  message('GCS path selected. The agent will create its BigQuery table and prepare analysis using your cloud connection.');
};
for(const id of ['source-gcs-uri'])$(id).oninput=()=>{
  if(!gcsSource)return;
  gcsSource=null;$('analyze').disabled=!source;$('analyze').textContent='Analyze local source →';
  $('scope').textContent='SELECT GCS PATH';$('gcs-source-status').textContent='Path settings changed. Select Use GCS source again.';
};

$('create-upload-table').onclick=()=>createSourceTableAndPrepare();
