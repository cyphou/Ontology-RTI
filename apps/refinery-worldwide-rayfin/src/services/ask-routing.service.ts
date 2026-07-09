//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

export type AskIntent = "ops-fastpath" | "analytics" | "hybrid";

export function normalizeAskQuestion(question: string): string {
    return question.trim().toLowerCase().replace(/\s+/g, " ");
}

// Lightweight intent router so deterministic operational Q&A avoids remote
// round-trips while analytical prompts prefer the live Data Agent.
export function classifyAskIntent(question: string): AskIntent {
    const q = normalizeAskQuestion(question);
    if (!q) {
        return "ops-fastpath";
    }

    const analyticsPatterns = [
        /\b(trend|over time|history|historical)\b/,
        /(last\s+\d+|\bhour\b|\bday\b|\bweek\b|\bmonth\b)/,
        /\b(compare|correlation|distribution|average|median|percentile)\b/,
        /(by site|per site|by region|per region)/,
    ];
    if (analyticsPatterns.some((p) => p.test(q))) {
        return "analytics";
    }

    const opsPatterns = [
        /\b(alarm|warning|critical|fault|trip)\b/,
        /\b(hottest|temperature|temp|module|unit|inverter|utilization|feed|output|throughput)\b/,
        /(how many|\bcount\b|\btotal\b)/,
        /\b(highest|lowest|top|worst|best)\b/,
    ];
    if (opsPatterns.some((p) => p.test(q))) {
        return "ops-fastpath";
    }

    return "hybrid";
}
