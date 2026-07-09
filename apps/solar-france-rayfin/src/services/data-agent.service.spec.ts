//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";
import { extractAgentAnswer, extractAgentConfidence, extractAgentEvidence } from "@/services/data-agent.service";

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
});
