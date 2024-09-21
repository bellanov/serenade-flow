<script lang="ts">
  import { ApolloClient, InMemoryCache, createHttpLink } from '@apollo/client/core';
  import { setClient } from 'svelte-apollo';
  import { query, mutation } from 'svelte-apollo';
  import { GET_PIPELINE_STATUS, START_PIPELINE, STOP_PIPELINE } from './graphql/queries';

  const httpLink = createHttpLink({
    uri: 'http://127.0.0.1:5000/graphql',
    credentials: 'same-origin'  // Changed from 'include' to 'same-origin'
  });

  const client = new ApolloClient({
    link: httpLink,
    cache: new InMemoryCache()
  });

  setClient(client);

  const pipelineStatus = query(GET_PIPELINE_STATUS);
  const startPipeline = mutation(START_PIPELINE);
  const stopPipeline = mutation(STOP_PIPELINE);

  async function handleStartPipeline() {
    try {
      const result = await startPipeline();
      alert(result.data.startPipeline);
    } catch (error) {
      alert(`Error: ${error.message}`);
    }
  }

  async function handleStopPipeline() {
    try {
      const result = await stopPipeline();
      alert(result.data.stopPipeline);
    } catch (error) {
      alert(`Error: ${error.message}`);
    }
  }
</script>

<main>
  <h1>SerenadeFlow ETL Pipeline</h1>
  {#if $pipelineStatus.loading}
    <p>Loading status...</p>
  {:else if $pipelineStatus.error}
    <p>Error: {$pipelineStatus.error.message}</p>
  {:else}
    <p>Pipeline Status: {$pipelineStatus.data.pipelineStatus}</p>
  {/if}
  <button on:click={handleStartPipeline}>Start Pipeline</button>
  <button on:click={handleStopPipeline}>Stop Pipeline</button>
</main>

<style>
  main {
    text-align: center;
    padding: 1em;
    max-width: 100%;
    margin: 0 auto;
  }
  h1 {
    color: #ff3e00;
    text-transform: uppercase;
    font-size: 2em;
    font-weight: 100;
    word-wrap: break-word;
  }
  button {
    margin-top: 20px;
    padding: 10px 20px;
    font-size: 1em;
  }
</style>