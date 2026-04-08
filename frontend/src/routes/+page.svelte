<script lang="ts">
	import { streamQuery, fetchCorpora, type SSEEvent, type Source, type Corpus } from '$lib/api';

	let question = $state('');
	let corpusId = $state('');
	let retriever = $state('vector');
	let topK = $state(5);
	let loading = $state(false);
	let statusMessages = $state<string[]>([]);
	let answer = $state('');
	let sources = $state<Source[]>([]);
	let errorMessage = $state('');
	let corpora = $state<Corpus[]>([]);
	let corporaLoading = $state(true);

	const retrievers = ['vector', 'keyword', 'hybrid', 'multi_query', 'agent'];

	async function loadCorpora() {
		try {
			corpora = await fetchCorpora();
			if (corpora.length > 0 && !corpusId) {
				corpusId = corpora[0]._id;
			}
		} catch (e: unknown) {
			errorMessage = e instanceof Error ? e.message : 'Failed to load corpora';
		} finally {
			corporaLoading = false;
		}
	}

	loadCorpora();

	async function handleSubmit() {
		if (!question.trim() || !corpusId) return;

		loading = true;
		statusMessages = [];
		answer = '';
		sources = [];
		errorMessage = '';

		try {
			for await (const event of streamQuery({
				question: question.trim(),
				corpus_id: corpusId,
				top_k: topK,
				retriever
			})) {
				switch (event.type) {
					case 'status':
						statusMessages = [...statusMessages, event.message];
						break;
					case 'sources':
						if (Array.isArray(event.data)) {
							sources = event.data;
						}
						statusMessages = [...statusMessages, event.message];
						break;
					case 'answer':
						if (event.data && 'answer' in event.data) {
							answer = event.data.answer;
							sources = event.data.sources;
						}
						break;
					case 'error':
						errorMessage = event.message;
						break;
					case 'done':
						statusMessages = [...statusMessages, event.message];
						break;
				}
			}
		} catch (e: unknown) {
			errorMessage = e instanceof Error ? e.message : 'An unexpected error occurred';
		} finally {
			loading = false;
		}
	}
</script>

<div class="container">
	<header>
		<h1>Rag5 Search</h1>
		<p class="subtitle">Retrieval-Augmented Generation</p>
	</header>

	<form onsubmit={(e) => { e.preventDefault(); handleSubmit(); }}>
		<div class="form-group">
			<label for="corpus">Corpus</label>
			{#if corporaLoading}
				<select id="corpus" disabled>
					<option>Loading corpora...</option>
				</select>
			{:else if corpora.length === 0}
				<select id="corpus" disabled>
					<option>No corpora available</option>
				</select>
			{:else}
				<select id="corpus" bind:value={corpusId}>
					{#each corpora as c}
						<option value={c._id}>{c.name}</option>
					{/each}
				</select>
			{/if}
		</div>

		<div class="form-row">
			<div class="form-group" style="flex:1">
				<label for="retriever">Retriever</label>
				<select id="retriever" bind:value={retriever}>
					{#each retrievers as r}
						<option value={r}>{r.replace('_', ' ')}</option>
					{/each}
				</select>
			</div>
			<div class="form-group" style="flex:0 0 100px">
				<label for="topk">Top K</label>
				<input id="topk" type="number" bind:value={topK} min="1" max="50" />
			</div>
		</div>

		<div class="form-group">
			<label for="question">Question</label>
			<textarea
				id="question"
				bind:value={question}
				placeholder="Ask a question about your documents..."
				rows="3"
			></textarea>
		</div>

		<button type="submit" disabled={loading || !question.trim() || !corpusId}>
			{loading ? 'Searching...' : 'Search'}
		</button>
	</form>

	{#if statusMessages.length > 0}
		<div class="status-log">
			<h3>Status</h3>
			{#each statusMessages as msg, i}
				<div class="status-line" class:latest={i === statusMessages.length - 1 && loading}>
					<span class="dot" class:pulse={i === statusMessages.length - 1 && loading}></span>
					{msg}
				</div>
			{/each}
		</div>
	{/if}

	{#if errorMessage}
		<div class="error">{errorMessage}</div>
	{/if}

	{#if answer}
		<div class="answer-section">
			<h3>Answer</h3>
			<div class="answer">{answer}</div>
		</div>
	{/if}

	{#if sources.length > 0}
		<div class="sources-section">
			<h3>Sources ({sources.length})</h3>
			{#each sources as source, i}
				<details class="source-card">
					<summary>
						<span class="source-num">#{i + 1}</span>
						<span class="source-file">{source.filename}</span>
						<span class="source-score">{(source.score * 100).toFixed(1)}%</span>
					</summary>
					<p class="chunk-text">{source.chunk_text}</p>
				</details>
			{/each}
		</div>
	{/if}
</div>

<style>
	:global(body) {
		margin: 0;
		font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
		background: #f5f7fa;
		color: #1a1a2e;
	}

	.container {
		max-width: 800px;
		margin: 0 auto;
		padding: 2rem 1rem;
	}

	header {
		text-align: center;
		margin-bottom: 2rem;
	}

	h1 {
		margin: 0;
		font-size: 2rem;
		color: #1a1a2e;
	}

	.subtitle {
		color: #666;
		margin: 0.25rem 0 0;
	}

	form {
		background: white;
		padding: 1.5rem;
		border-radius: 12px;
		box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
	}

	.form-group {
		margin-bottom: 1rem;
	}

	.form-row {
		display: flex;
		gap: 1rem;
	}

	label {
		display: block;
		margin-bottom: 0.35rem;
		font-weight: 600;
		font-size: 0.875rem;
		color: #444;
	}

	select,
	input,
	textarea {
		width: 100%;
		padding: 0.5rem 0.75rem;
		border: 1px solid #ddd;
		border-radius: 6px;
		font-size: 0.95rem;
		box-sizing: border-box;
		background: #fafafa;
	}

	textarea {
		resize: vertical;
		font-family: inherit;
	}

	select:focus,
	input:focus,
	textarea:focus {
		outline: none;
		border-color: #4a6cf7;
		box-shadow: 0 0 0 2px rgba(74, 108, 247, 0.15);
	}

	button {
		width: 100%;
		padding: 0.75rem;
		background: #4a6cf7;
		color: white;
		border: none;
		border-radius: 8px;
		font-size: 1rem;
		font-weight: 600;
		cursor: pointer;
		transition: background 0.2s;
	}

	button:hover:not(:disabled) {
		background: #3b5de7;
	}

	button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.status-log {
		margin-top: 1.5rem;
		background: #1a1a2e;
		color: #a4e8a4;
		padding: 1rem 1.25rem;
		border-radius: 10px;
		font-family: 'SF Mono', 'Fira Code', monospace;
		font-size: 0.85rem;
	}

	.status-log h3 {
		margin: 0 0 0.5rem;
		color: #ccc;
		font-size: 0.8rem;
		text-transform: uppercase;
		letter-spacing: 0.05em;
	}

	.status-line {
		padding: 0.2rem 0;
		display: flex;
		align-items: center;
		gap: 0.5rem;
	}

	.status-line.latest {
		color: #7df87d;
	}

	.dot {
		width: 6px;
		height: 6px;
		border-radius: 50%;
		background: #a4e8a4;
		flex-shrink: 0;
	}

	.dot.pulse {
		animation: pulse 1s infinite;
		background: #7df87d;
	}

	@keyframes pulse {
		0%,
		100% {
			opacity: 1;
		}
		50% {
			opacity: 0.3;
		}
	}

	.error {
		margin-top: 1.5rem;
		padding: 1rem;
		background: #fee;
		color: #c00;
		border-radius: 8px;
		border: 1px solid #fcc;
	}

	.answer-section {
		margin-top: 1.5rem;
		background: white;
		padding: 1.5rem;
		border-radius: 12px;
		box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
	}

	.answer-section h3 {
		margin: 0 0 0.75rem;
		color: #333;
	}

	.answer {
		line-height: 1.7;
		white-space: pre-wrap;
	}

	.sources-section {
		margin-top: 1.5rem;
	}

	.sources-section h3 {
		margin: 0 0 0.75rem;
	}

	.source-card {
		background: white;
		border-radius: 10px;
		margin-bottom: 0.5rem;
		box-shadow: 0 1px 4px rgba(0, 0, 0, 0.06);
		overflow: hidden;
	}

	.source-card summary {
		padding: 0.75rem 1rem;
		cursor: pointer;
		display: flex;
		align-items: center;
		gap: 0.75rem;
		font-size: 0.9rem;
	}

	.source-card summary:hover {
		background: #f8f9fb;
	}

	.source-num {
		color: #4a6cf7;
		font-weight: 700;
		font-size: 0.8rem;
	}

	.source-file {
		flex: 1;
		font-weight: 500;
	}

	.source-score {
		color: #888;
		font-size: 0.8rem;
	}

	.chunk-text {
		margin: 0;
		padding: 0.75rem 1rem 1rem;
		font-size: 0.85rem;
		color: #555;
		line-height: 1.6;
		border-top: 1px solid #eee;
		white-space: pre-wrap;
	}
</style>
