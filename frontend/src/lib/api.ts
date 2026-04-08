export interface QueryRequest {
	question: string;
	corpus_id: string;
	top_k?: number;
	document_ids?: string[];
	retriever?: string;
}

export interface Source {
	document_id: string;
	filename: string;
	chunk_text: string;
	score: number;
}

export interface SSEEvent {
	type: 'status' | 'sources' | 'answer' | 'error' | 'done';
	message: string;
	data?: Source[] | { answer: string; sources: Source[] };
}

export interface Corpus {
	_id: string;
	name: string;
}

export async function fetchCorpora(): Promise<Corpus[]> {
	const res = await fetch('/api/v1/corpora');
	if (!res.ok) throw new Error(`Failed to fetch corpora: ${res.statusText}`);
	return res.json();
}

export async function* streamQuery(req: QueryRequest): AsyncGenerator<SSEEvent> {
	const res = await fetch('/api/v1/query/stream', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(req)
	});

	if (!res.ok) {
		throw new Error(`Query failed: ${res.statusText}`);
	}

	const reader = res.body!.getReader();
	const decoder = new TextDecoder();
	let buffer = '';

	while (true) {
		const { done, value } = await reader.read();
		if (done) break;

		buffer += decoder.decode(value, { stream: true });
		const lines = buffer.split('\n');
		buffer = lines.pop() ?? '';

		for (const line of lines) {
			const trimmed = line.trim();
			if (trimmed.startsWith('data: ')) {
				const json = trimmed.slice(6);
				try {
					yield JSON.parse(json) as SSEEvent;
				} catch {
					// skip malformed events
				}
			}
		}
	}
}
