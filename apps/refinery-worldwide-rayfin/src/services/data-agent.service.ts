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
}

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

    return "";
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
        const res = await fetch(url, {
            method: "POST",
            headers,
            body: JSON.stringify({ question, context }),
            signal: controller.signal,
        });
        if (!res.ok) {
            throw new Error(`Data Agent responded ${res.status}`);
        }
        const payload = (await res.json()) as unknown;
        const summary = extractAgentAnswer(payload);
        if (!summary) {
            throw new Error("Data Agent returned no parsable answer.");
        }
        return { summary, queryText: "Fabric Data Agent · live query over Lakehouse/Eventhouse" };
    } finally {
        window.clearTimeout(timeout);
    }
}
