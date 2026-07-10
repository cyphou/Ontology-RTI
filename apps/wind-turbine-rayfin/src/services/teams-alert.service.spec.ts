//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, expect, it } from "vitest";
import { buildAlarmCard } from "@/services/teams-alert.service";

describe("buildAlarmCard", () => {
    it("builds a MessageCard carrying the alarm identity and facts", () => {
        const card = buildAlarmCard({
            id: "SITE-TX-WT-01",
            siteName: "Panhandle Ridge",
            status: "alarm",
            powerKw: 1500,
            detail: "temp 90 C, vib 9 mm/s",
        }) as { "@type": string; summary: string; sections: { facts: { name: string; value: string }[] }[] };

        expect(card["@type"]).toBe("MessageCard");
        expect(card.summary).toContain("SITE-TX-WT-01");
        expect(card.sections[0].facts).toEqual(
            expect.arrayContaining([
                { name: "Status", value: "alarm" },
                { name: "Detail", value: "temp 90 C, vib 9 mm/s" },
            ]),
        );
    });
});
