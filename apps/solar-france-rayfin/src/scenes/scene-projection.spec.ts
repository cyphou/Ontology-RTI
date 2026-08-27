//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";

import { projectLonToX, projectLatToZ } from "@/App";

// Regression guard: SolarFleetScene imports these projection helpers from App to pin
// each site to its real lon/lat on the map. They were previously declared without
// `export`, which the --noCheck build did not catch and surfaced as a runtime
// "projectLonToX is not defined" that crashed the fleet scene.
describe("fleet scene projection helpers", () => {
    it("App re-exports projectLonToX/projectLatToZ used by SolarFleetScene", () => {
        expect(typeof projectLonToX).toBe("function");
        expect(typeof projectLatToZ).toBe("function");
    });

    it("projects longitude monotonically (east of west)", () => {
        expect(projectLonToX(180)).toBeGreaterThan(projectLonToX(-180));
    });

    it("projects latitude so north maps ahead of south", () => {
        expect(projectLatToZ(90)).toBeLessThan(projectLatToZ(-90));
    });
});
