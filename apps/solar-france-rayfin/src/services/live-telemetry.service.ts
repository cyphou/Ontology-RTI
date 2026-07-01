//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Live telemetry service — pulls REAL PV array readings from the deployed
// SolarFarm semantic model (Direct Lake over the Lakehouse) instead of the
// synthetic buildFarm() generator.
//
// Activation is fully opt-in and fallback-safe:
//   - Set VITE_LIVE_TELEMETRY_MODEL to the semantic-model connection alias
//     registered in fabric.yaml / fabric.generated.ts (via `npx fabric-app-data`).
//   - When unset, isLiveTelemetryConfigured() is false and the app keeps using
//     the built-in synthetic feed, so nothing breaks until the model is wired.
//
// Schema is grounded in ontologies/SolarFarm/SemanticModel:
//   sensortelemetry(Timestamp, ArrayId, SensorId, SensorType, Value, Unit, Quality)
//   dimsolararray(ArrayId, ArrayName, PlantId, RatedCapacityKW, TiltDegrees, Orientation, Status)
//   dimsolarplant(PlantId, PlantName, Region, Latitude, Longitude, CapacityMWc, ArrayCount, ...)

import type { QueryTable } from "@microsoft/fabric-app-data";
import { getFabricClient } from "@/lib/fabric-client";

/** A solar plant (site) row projected from dimsolarplant. */
export interface LivePlant {
    plantId: string;
    plantName: string;
    latitude: number;
    longitude: number;
    capacityMwc: number;
}

/** A PV array → plant mapping row projected from dimsolararray. */
export interface LiveArray {
    arrayId: string;
    plantId: string;
}

/** Latest pivoted signal values for a single PV array. */
export interface LiveMetrics {
    arrayId: string;
    powerKw: number;
    irradianceWm2: number;
    moduleTempC: number;
    inverterLoadPct: number;
}

/** Everything needed to assemble a live view of the solar fleet. */
export interface LiveTelemetrySnapshot {
    plants: LivePlant[];
    arrays: LiveArray[];
    metrics: LiveMetrics[];
}

/** A single latest sensor reading (one SensorType for one PV array). */
export interface TelemetryReading {
    arrayId: string;
    sensorType: string;
    value: number;
}

// Connection alias resolved from fabric.generated.ts (managed by `npx fabric-app-data`).
const LIVE_MODEL = import.meta.env.VITE_LIVE_TELEMETRY_MODEL?.trim();

// Maps the ontology telemetry SensorType to an app signal. The names line up with
// SIGNAL_METADATA.ontologyProperty (AcPowerKW, IrradianceWM2, ModuleTempC,
// InverterLoadPct) so live values feed the same status bands used for classification.
const SIGNAL_BY_SENSOR_TYPE: Record<string, keyof Omit<LiveMetrics, "arrayId">> = {
    AcPower: "powerKw",
    Irradiance: "irradianceWm2",
    ModuleTemp: "moduleTempC",
    InverterLoad: "inverterLoadPct",
};

export const DAX_PLANTS =
    'EVALUATE SELECTCOLUMNS(dimsolarplant, ' +
    '"PlantId", dimsolarplant[PlantId], ' +
    '"PlantName", dimsolarplant[PlantName], ' +
    '"Latitude", dimsolarplant[Latitude], ' +
    '"Longitude", dimsolarplant[Longitude], ' +
    '"CapacityMWc", dimsolarplant[CapacityMWc])';

export const DAX_ARRAYS =
    'EVALUATE SELECTCOLUMNS(dimsolararray, ' +
    '"ArrayId", dimsolararray[ArrayId], ' +
    '"PlantId", dimsolararray[PlantId])';

// Latest Value per (ArrayId, SensorType). Timestamp is an ISO string, so MAX()
// gives the most recent reading; the inner CALCULATE isolates that row's value.
export const DAX_LATEST_TELEMETRY =
    'EVALUATE SELECTCOLUMNS(ADDCOLUMNS(' +
    'SUMMARIZE(sensortelemetry, sensortelemetry[ArrayId], sensortelemetry[SensorType]), ' +
    '"LatestValue", VAR ts = CALCULATE(MAX(sensortelemetry[Timestamp])) ' +
    'RETURN CALCULATE(MAX(sensortelemetry[Value]), sensortelemetry[Timestamp] = ts)), ' +
    '"ArrayId", sensortelemetry[ArrayId], ' +
    '"SensorType", sensortelemetry[SensorType], ' +
    '"Value", [LatestValue])';

/** True when a live semantic-model connection alias is configured. */
export function isLiveTelemetryConfigured(): boolean {
    return Boolean(LIVE_MODEL);
}

// Normalise a DAX result column name: strip surrounding brackets and lowercase so
// "PlantId", "[PlantId]" and "dimsolarplant[PlantId]" all match.
function normalizeKey(name: string): string {
    const bracket = name.lastIndexOf("[");
    const core = bracket >= 0 ? name.slice(bracket + 1).replace(/\]$/, "") : name;
    return core.trim().toLowerCase();
}

/**
 * Convert an SDK QueryTable ({ columns: [{name}], rows: [[...]] }) into an array
 * of records keyed by normalised column name. Tolerant of column ordering.
 */
export function tableToRecords(table: QueryTable): Record<string, unknown>[] {
    const keys = table.columns.map((c) => normalizeKey(c.name));
    return table.rows.map((row) => {
        const record: Record<string, unknown> = {};
        keys.forEach((key, i) => {
            record[key] = (row as unknown[])[i];
        });
        return record;
    });
}

function toNumber(value: unknown): number {
    const n = typeof value === "number" ? value : Number(value);
    return Number.isFinite(n) ? n : 0;
}

function toText(value: unknown): string {
    return value == null ? "" : String(value);
}

export function recordsToPlants(records: Record<string, unknown>[]): LivePlant[] {
    return records
        .map((r) => ({
            plantId: toText(r["plantid"]),
            plantName: toText(r["plantname"]),
            latitude: toNumber(r["latitude"]),
            longitude: toNumber(r["longitude"]),
            capacityMwc: toNumber(r["capacitymwc"]),
        }))
        .filter((f) => f.plantId !== "");
}

export function recordsToArrays(records: Record<string, unknown>[]): LiveArray[] {
    return records
        .map((r) => ({
            arrayId: toText(r["arrayid"]),
            plantId: toText(r["plantid"]),
        }))
        .filter((t) => t.arrayId !== "");
}

export function recordsToReadings(records: Record<string, unknown>[]): TelemetryReading[] {
    return records
        .map((r) => ({
            arrayId: toText(r["arrayid"]),
            sensorType: toText(r["sensortype"]),
            value: toNumber(r["value"]),
        }))
        .filter((r) => r.arrayId !== "");
}

/**
 * Pivot flat latest-reading rows into one LiveMetrics per PV array, mapping each
 * telemetry SensorType onto its app signal (power / irradiance / module temp / inverter).
 */
export function pivotReadings(readings: TelemetryReading[]): LiveMetrics[] {
    const byArray = new Map<string, LiveMetrics>();
    for (const reading of readings) {
        const signal = SIGNAL_BY_SENSOR_TYPE[reading.sensorType];
        if (!signal) {
            continue;
        }
        let metrics = byArray.get(reading.arrayId);
        if (!metrics) {
            metrics = { arrayId: reading.arrayId, powerKw: 0, irradianceWm2: 0, moduleTempC: 0, inverterLoadPct: 0 };
            byArray.set(reading.arrayId, metrics);
        }
        metrics[signal] = reading.value;
    }
    return [...byArray.values()];
}

// Run one DAX query, returning parsed records or null on any failure (the SDK
// signals errors via result.status rather than throwing, but we guard both).
async function runQuery(dax: string): Promise<Record<string, unknown>[] | null> {
    if (!LIVE_MODEL) {
        return null;
    }
    try {
        const result = await getFabricClient().semanticModel(LIVE_MODEL).query(dax);
        if (result.status !== "success") {
            return null;
        }
        return tableToRecords(result.table);
    } catch {
        return null;
    }
}

/**
 * Fetch a full live snapshot (plants + arrays + latest metrics) from the
 * semantic model. Returns null when unconfigured or when any query fails, so
 * callers can transparently fall back to the synthetic feed.
 */
export async function fetchLiveTelemetry(): Promise<LiveTelemetrySnapshot | null> {
    if (!isLiveTelemetryConfigured()) {
        return null;
    }
    const [plantRecords, arrayRecords, telemetryRecords] = await Promise.all([
        runQuery(DAX_PLANTS),
        runQuery(DAX_ARRAYS),
        runQuery(DAX_LATEST_TELEMETRY),
    ]);
    if (!plantRecords || !arrayRecords || !telemetryRecords) {
        return null;
    }
    return {
        plants: recordsToPlants(plantRecords),
        arrays: recordsToArrays(arrayRecords),
        metrics: pivotReadings(recordsToReadings(telemetryRecords)),
    };
}
