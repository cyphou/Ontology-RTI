//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";
import { encodeBand, decodeBand, INF_BAND } from "@/services/ontology-data.service";

describe("threshold band encoding", () => {
    it("passes finite band values through unchanged", () => {
        expect(encodeBand(67)).toBe(67);
        expect(decodeBand(79)).toBe(79);
    });

    it("encodes Infinity to the JSON-safe sentinel and decodes it back", () => {
        expect(encodeBand(Infinity)).toBe(INF_BAND);
        expect(decodeBand(INF_BAND)).toBe(Infinity);
    });

    it("round-trips an informational (unbanded) value", () => {
        expect(decodeBand(encodeBand(Infinity))).toBe(Infinity);
    });

    it("round-trips a finite value", () => {
        expect(decodeBand(encodeBand(22))).toBe(22);
    });
});
