//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";
import { extractAgentAnswer } from "@/services/data-agent.service";

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
