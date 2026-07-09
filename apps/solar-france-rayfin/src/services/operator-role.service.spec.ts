//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, expect, it } from "vitest";
import { canManageDispatch, normalizeOperatorRole } from "@/services/operator-role.service";

describe("normalizeOperatorRole", () => {
    it("defaults unknown values to operator", () => {
        expect(normalizeOperatorRole(undefined)).toBe("operator");
        expect(normalizeOperatorRole(null)).toBe("operator");
        expect(normalizeOperatorRole("anything")).toBe("operator");
    });

    it("accepts viewer explicitly", () => {
        expect(normalizeOperatorRole("viewer")).toBe("viewer");
    });
});

describe("canManageDispatch", () => {
    it("allows operators and blocks viewers", () => {
        expect(canManageDispatch("operator")).toBe(true);
        expect(canManageDispatch("viewer")).toBe(false);
    });
});
