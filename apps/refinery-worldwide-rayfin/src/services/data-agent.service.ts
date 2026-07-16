//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

/**
 * Thin client for a deployed Microsoft Fabric **Data Agent** (a.k.a. AI Skill)
 * endpoint. When `VITE_DATA_AGENT_URL` is configured, `Ask Fabric IQ` routes the
 * natural-language question to the real agent (which reasons over the Lakehouse /
 * Eventhouse behind it) instead of the in-browser fallback engine.
 *
 * The exact request/response contract differs between Data Agent surfaces, so the
 * response parsing here is deliberately tolerant ({@link extractAgentAnswer}). If
 * the call fails or no endpoint is configured, the caller falls back to the
 * ontology-grounded local engine — the app keeps working either way.
 */

export interface DataAgentAnswer {
    summary: string;
    queryText: string;
    confidence?: number;
    evidence?: string[];
    transport?: "legacy" | "mcp";
}

type AgentTransport = "legacy" | "mcp";

/** True when a real Data Agent endpoint has been wired up via env. */
export function isDataAgentConfigured(): boolean {
    return Boolean(import.meta.env.VITE_DATA_AGENT_URL);
}

/**
 * Pull a human-readable answer string out of the many shapes a Data Agent /
 * AI-Skill / OpenAI-compatible endpoint may return. Returns an empty string when
 * nothing usable is found so the caller can treat it as a failure.
 */
export function extractAgentAnswer(payload: unknown): string {
    if (payload == null) {
        return "";
    }
    if (typeof payload === "string") {
        return payload.trim();
    }
    if (typeof payload !== "object") {
        return "";
    }

    const obj = payload as Record<string, unknown>;

    // Common flat shapes: { answer | summary | content | output | text }.
    for (const key of ["answer", "summary", "content", "output", "text", "message"]) {
        const v = obj[key];
        if (typeof v === "string" && v.trim()) {
            return v.trim();
        }
    }

    // OpenAI chat-completions: { choices: [{ message: { content } }] }.
    const choices = obj.choices;
    if (Array.isArray(choices) && choices.length > 0) {
        const msg = (choices[0] as Record<string, unknown>)?.message as Record<string, unknown> | undefined;
        const content = msg?.content;
        if (typeof content === "string" && content.trim()) {
            return content.trim();
        }
    }

    // Assistants-style: { data: [{ content: [{ text: { value } }] }] }.
    const data = obj.data;
    if (Array.isArray(data) && data.length > 0) {
        const content = (data[0] as Record<string, unknown>)?.content;
        if (Array.isArray(content) && content.length > 0) {
            const text = (content[0] as Record<string, unknown>)?.text as Record<string, unknown> | undefined;
            const value = text?.value;
            if (typeof value === "string" && value.trim()) {
                return value.trim();
            }
        }
    }

    // MCP tools/call: { result: { content: [{ type: "text", text }] } }.
    const result = obj.result;
    if (result && typeof result === "object") {
        const resultObj = result as Record<string, unknown>;
        const content = resultObj.content;
        if (Array.isArray(content)) {
            for (const item of content) {
                if (!item || typeof item !== "object") {
                    continue;
                }
                const entry = item as Record<string, unknown>;
                const text = entry.text;
                if (typeof text === "string" && text.trim()) {
                    return text.trim();
                }
            }
        }
        for (const key of ["answer", "summary", "output", "output_text", "text"]) {
            const v = resultObj[key];
            if (typeof v === "string" && v.trim()) {
                return v.trim();
            }
        }
    }

    // Some response APIs wrap text in output arrays.
    const output = obj.output;
    if (Array.isArray(output)) {
        for (const item of output) {
            if (!item || typeof item !== "object") {
                continue;
            }
            const entry = item as Record<string, unknown>;
            const content = entry.content;
            if (Array.isArray(content)) {
                for (const c of content) {
                    if (!c || typeof c !== "object") {
                        continue;
                    }
                    const text = (c as Record<string, unknown>).text;
                    if (typeof text === "string" && text.trim()) {
                        return text.trim();
                    }
                }
            }
        }
    }

    return "";
}

/** Parse a best-effort confidence score from multiple payload shapes. */
export function extractAgentConfidence(payload: unknown): number | undefined {
    if (!payload || typeof payload !== "object") {
        return undefined;
    }
    const obj = payload as Record<string, unknown>;
    for (const key of ["confidence", "score", "confidenceScore"]) {
        const v = obj[key];
        if (typeof v === "number" && Number.isFinite(v)) {
            return Math.max(0, Math.min(1, v > 1 ? v / 100 : v));
        }
    }

    const result = obj.result;
    if (result && typeof result === "object") {
        const nested = extractAgentConfidence(result);
        if (nested != null) {
            return nested;
        }
    }

    return undefined;
}

/** Parse a short evidence/citation list from tolerant payload shapes. */
export function extractAgentEvidence(payload: unknown): string[] {
    if (!payload || typeof payload !== "object") {
        return [];
    }
    const obj = payload as Record<string, unknown>;
    for (const key of ["evidence", "citations", "reasons", "highlights"]) {
        const v = obj[key];
        if (Array.isArray(v)) {
            const list = v
                .map((x) => {
                    if (typeof x === "string") {
                        return x.trim();
                    }
                    if (x && typeof x === "object") {
                        const text = (x as Record<string, unknown>).text;
                        if (typeof text === "string") {
                            return text.trim();
                        }
                    }
                    return "";
                })
                .filter(Boolean);
            if (list.length > 0) {
                return list.slice(0, 5);
            }
        }
    }

    const result = obj.result;
    if (result && typeof result === "object") {
        const nested = extractAgentEvidence(result);
        if (nested.length > 0) {
            return nested;
        }
    }

    return [];
}

function shouldUseMcpFirst(url: string, mode: string | undefined): boolean {
    if (mode === "mcp") {
        return true;
    }
    if (mode === "legacy") {
        return false;
    }
    return /\/mcp\/?$/i.test(url);
}

function legacyBody(question: string, context: Record<string, unknown>): Record<string, unknown> {
    return { question, context };
}

function mcpBody(question: string, context: Record<string, unknown>): Record<string, unknown> {
    const toolName = import.meta.env.VITE_DATA_AGENT_MCP_TOOL?.trim() || "ask_data_agent";
    return {
        jsonrpc: "2.0",
        id: 1,
        method: "tools/call",
        params: {
            name: toolName,
            arguments: {
                question,
                context,
            },
        },
    };
}

async function postAgent(
    url: string,
    headers: Record<string, string>,
    body: Record<string, unknown>,
    controller: AbortController,
): Promise<unknown> {
    const res = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: controller.signal,
    });
    if (!res.ok) {
        throw new Error(`Data Agent responded ${res.status}`);
    }
    return (await res.json()) as unknown;
}

/**
 * Send a question to the configured Fabric Data Agent. Throws when the endpoint
 * is not configured, the request fails, or no answer can be parsed — callers are
 * expected to fall back to the local engine on any rejection.
 */
export async function queryDataAgent(question: string, context: Record<string, unknown>): Promise<DataAgentAnswer> {
    const url = import.meta.env.VITE_DATA_AGENT_URL;
    if (!url) {
        throw new Error("Data Agent endpoint not configured (VITE_DATA_AGENT_URL).");
    }
    const key = import.meta.env.VITE_DATA_AGENT_KEY;

    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (key) {
        headers.Authorization = `Bearer ${key}`;
    }

    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 20_000);
    try {
        const mode = import.meta.env.VITE_DATA_AGENT_MODE?.trim().toLowerCase();
        const mcpFirst = shouldUseMcpFirst(url, mode);
        const transports: AgentTransport[] = mcpFirst ? ["mcp", "legacy"] : ["legacy", "mcp"];

        let lastError: unknown;
        for (const transport of transports) {
            try {
                const payload = await postAgent(
                    url,
                    headers,
                    transport === "mcp" ? mcpBody(question, context) : legacyBody(question, context),
                    controller,
                );
                const payloadObj = payload as Record<string, unknown> | null;
                const rpcError = payloadObj && typeof payloadObj === "object" ? payloadObj.error : undefined;
                if (rpcError) {
                    throw new Error("Data Agent MCP call failed.");
                }

                const summary = extractAgentAnswer(payload);
                if (!summary) {
                    throw new Error("Data Agent returned no parsable answer.");
                }
                return {
                    summary,
                    queryText:
                        transport === "mcp"
                            ? "Fabric Data Agent MCP · live query over Lakehouse/Eventhouse"
                            : "Fabric Data Agent · live query over Lakehouse/Eventhouse",
                    confidence: extractAgentConfidence(payload),
                    evidence: extractAgentEvidence(payload),
                    transport,
                };
            } catch (err) {
                lastError = err;
            }
        }

        throw lastError instanceof Error ? lastError : new Error("Data Agent request failed.");
    } finally {
        window.clearTimeout(timeout);
    }
}
