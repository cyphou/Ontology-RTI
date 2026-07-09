//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, expect, it } from "vitest";
import { HISTORY_WINDOWS, historyPointLimit } from "@/services/history-window.service";

describe("history windows", () => {
    it("exposes the supported windows in order", () => {
        expect(HISTORY_WINDOWS).toEqual(["1h", "6h", "24h"]);
    });

    it("maps each window to a larger history point budget", () => {
        expect(historyPointLimit("1h")).toBe(24);
        expect(historyPointLimit("6h")).toBe(72);
        expect(historyPointLimit("24h")).toBe(120);
    });
});
