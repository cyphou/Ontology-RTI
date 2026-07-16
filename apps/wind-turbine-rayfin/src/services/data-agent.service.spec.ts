//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";
import { buildAgentHeaders, extractAgentAnswer, extractAgentConfidence, extractAgentEvidence, getDataAgentAuthScheme, getDataAgentMode, resolveAgentHeadersFromConfig, resolveAgentTransports } from "@/services/data-agent.service";

describe("extractAgentAnswer", () => {
    it("returns a trimmed plain string", () => {
        expect(extractAgentAnswer("  hello world  ")).toBe("hello world");
    });

    it("reads flat answer-style fields", () => {
        expect(extractAgentAnswer({ answer: "from answer" })).toBe("from answer");
        expect(extractAgentAnswer({ summary: "from summary" })).toBe("from summary");
        expect(extractAgentAnswer({ content: "from content" })).toBe("from content");
    });

    it("reads an OpenAI chat-completions shape", () => {
        const payload = { choices: [{ message: { content: "chat reply" } }] };
        expect(extractAgentAnswer(payload)).toBe("chat reply");
    });

    it("reads an Assistants-style nested content shape", () => {
        const payload = { data: [{ content: [{ text: { value: "assistant reply" } }] }] };
        expect(extractAgentAnswer(payload)).toBe("assistant reply");
    });
    it("reads an MCP tools/call result content shape", () => {
        const payload = { result: { content: [{ type: "text", text: "mcp reply" }] } };
        expect(extractAgentAnswer(payload)).toBe("mcp reply");
    });

    it("returns an empty string when nothing usable is present", () => {
        expect(extractAgentAnswer(null)).toBe("");
        expect(extractAgentAnswer({})).toBe("");
        expect(extractAgentAnswer({ choices: [] })).toBe("");
        expect(extractAgentAnswer(42)).toBe("");
    });
});

describe("extractAgentConfidence", () => {
    it("reads direct normalized confidence", () => {
        expect(extractAgentConfidence({ confidence: 0.82 })).toBe(0.82);
    });

    it("normalizes percentage-like confidence", () => {
        expect(extractAgentConfidence({ confidenceScore: 87 })).toBeCloseTo(0.87, 6);
    });

    it("returns undefined when no numeric confidence exists", () => {
        expect(extractAgentConfidence({})).toBeUndefined();
        expect(extractAgentConfidence({ confidence: "hi" })).toBeUndefined();
    });
    it("reads nested confidence from MCP-like result envelopes", () => {
        expect(extractAgentConfidence({ result: { confidenceScore: 91 } })).toBeCloseTo(0.91, 6);
    });
});

describe("extractAgentEvidence", () => {
    it("reads string evidence arrays", () => {
        expect(extractAgentEvidence({ evidence: ["a", "b"] })).toEqual(["a", "b"]);
    });

    it("reads object evidence arrays with text fields", () => {
        expect(extractAgentEvidence({ citations: [{ text: "row 1" }, { text: "row 2" }] })).toEqual(["row 1", "row 2"]);
    });

    it("returns an empty list when no evidence is present", () => {
        expect(extractAgentEvidence({})).toEqual([]);
        expect(extractAgentEvidence(null)).toEqual([]);
    });
    it("reads nested evidence from MCP-like result envelopes", () => {
        expect(extractAgentEvidence({ result: { citations: [{ text: "row 1" }] } })).toEqual(["row 1"]);
    });
});

describe("data agent mode and transport", () => {
    it("normalizes mode values", () => {
        expect(getDataAgentMode(undefined)).toBe("auto");
        expect(getDataAgentMode("mcp")).toBe("mcp");
        expect(getDataAgentMode("legacy")).toBe("legacy");
        expect(getDataAgentMode("invalid")).toBe("auto");
    });

    it("normalizes auth scheme values", () => {
        expect(getDataAgentAuthScheme(undefined)).toBe("bearer");
        expect(getDataAgentAuthScheme("api-key")).toBe("api-key");
        expect(getDataAgentAuthScheme("none")).toBe("none");
        expect(getDataAgentAuthScheme("weird")).toBe("bearer");
    });

    it("prefers mcp transport for mcp mode", () => {
        expect(resolveAgentTransports("https://example.com/agent", "mcp")).toEqual(["mcp", "legacy"]);
    });

    it("prefers legacy transport for legacy mode", () => {
        expect(resolveAgentTransports("https://example.com/agent", "legacy")).toEqual(["legacy", "mcp"]);
    });

    it("infers mcp preference from MCP-like endpoint paths", () => {
        expect(resolveAgentTransports("https://example.com/v1/mcp", undefined)).toEqual(["mcp", "legacy"]);
    });
});

describe("buildAgentHeaders", () => {
    it("returns only content-type when no token is configured", () => {
        const headers = buildAgentHeaders();
        expect(headers).toEqual({ "Content-Type": "application/json" });
    });

    it("uses bearer auth by default", () => {
        const headers = resolveAgentHeadersFromConfig({ token: "abc" });
        expect(headers.Authorization).toBe("Bearer abc");
    });

    it("supports api-key header mode", () => {
        const headers = resolveAgentHeadersFromConfig({ token: "abc", authScheme: "api-key", apiKeyHeader: "x-fabric-key" });
        expect(headers["x-fabric-key"]).toBe("abc");
        expect(headers.Authorization).toBeUndefined();
    });

    it("supports no-auth mode", () => {
        const headers = resolveAgentHeadersFromConfig({ token: "abc", authScheme: "none" });
        expect(headers).toEqual({ "Content-Type": "application/json" });
    });
});
