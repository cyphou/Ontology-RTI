//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";
import {
    encodeBand,
    decodeBand,
    INF_BAND,
    saveMaintenanceOrder,
    recentMaintenanceOrders,
    listUnitDevices,
    ensureUnitDevices,
    createUnitDevice,
    updateUnitDevice,
    deleteUnitDevice,
    type MaintenanceOrderRecord,
    type UnitDeviceRecord,
} from "@/services/ontology-data.service";

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

describe("maintenance order + unit device writeback (fallback-safe)", () => {
    const order: MaintenanceOrderRecord = {
        turbineId: "CDU-01",
        siteId: "REF-ROT",
        component: "Feed pump",
        priority: "P1",
        status: "open",
        curtailPct: 20,
        downtimeTicks: 4,
        projectedDeltaKwt: -1200,
        assignee: "Shift lead",
        note: "Predicted cavitation",
        createdAt: new Date().toISOString(),
    };

    const device: UnitDeviceRecord = {
        deviceKey: "cdu-feed-pump",
        component: "Column",
        label: "Feed pump",
        property: "vibrationMmS",
        unit: "mm/s",
        note: "Primary feed pump",
        anchorX: 0, anchorY: 1, anchorZ: 0,
        lookAtX: 0, lookAtY: 0, lookAtZ: 0,
        offsetX: 0, offsetY: 0, offsetZ: 2,
        zoom: 1,
        sortOrder: 1,
    };

    it("exposes the maintenance order writeback functions", () => {
        expect(typeof saveMaintenanceOrder).toBe("function");
        expect(typeof recentMaintenanceOrders).toBe("function");
    });

    it("exposes the unit device CRUD functions", () => {
        expect(typeof listUnitDevices).toBe("function");
        expect(typeof ensureUnitDevices).toBe("function");
        expect(typeof createUnitDevice).toBe("function");
        expect(typeof updateUnitDevice).toBe("function");
        expect(typeof deleteUnitDevice).toBe("function");
    });

    it("rejects (rather than throwing synchronously) when no backend is configured", async () => {
        await expect(saveMaintenanceOrder(order)).rejects.toBeInstanceOf(Error);
        await expect(recentMaintenanceOrders()).rejects.toBeInstanceOf(Error);
        await expect(listUnitDevices()).rejects.toBeInstanceOf(Error);
        await expect(createUnitDevice(device)).rejects.toBeInstanceOf(Error);
        await expect(updateUnitDevice({ deviceKey: device.deviceKey }, { note: "x" })).rejects.toBeInstanceOf(Error);
        await expect(deleteUnitDevice({ deviceKey: device.deviceKey })).rejects.toBeInstanceOf(Error);
    });
});
