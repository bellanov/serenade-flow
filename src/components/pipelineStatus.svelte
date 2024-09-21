<script lang="ts">
export let status: 'idle' | 'extracting' |
'transforming' | 'loading' | 'complete' |
'error' = 'idle';
</script>

<div class="pipeline-status">
<h2>Pipeline Status: {status}</h2>
<div class="status-bar">
<div class="status-step {status === 'extracting' || status === 'transforming' || status === 'loading' || status === 'complete' ? 'active' : ''}">Extract</div>
<div class="status-step {status === 'transforming' || status === 'loading' || status === 'complete' ? 'active' : ''}">Transform</div>
<div class="status-step {status === 'loading' || status === 'complete' ? 'active' : ''}">Load</div>
</div>
</div>
<style>
.pipeline-status {
margin: 20px 0;
}
.status-bar {
display: flex;
justify-content: space-between;
margin-top: 10px;
}
.status-step {
flex: 1;
text-align: center;
padding: 10px;
background-color: #ddd;
margin: 0 5px;
}
.status-step.active {
background-color: #4CAF50;
color: white;
}
</style>
