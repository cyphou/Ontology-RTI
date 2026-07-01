//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Live telemetry service — pulls REAL turbine readings from the deployed
// WindTurbine semantic model (Direct Lake over the Eventhouse-backed Lakehouse)
// instead of the synthetic buildFarm() generator.
//
// Activation is fully opt-in and fallback-safe:
//   - Set VITE_LIVE_TELEMETRY_MODEL to the semantic-model connection alias
//     registered in fabric.yaml / fabric.generated.ts (via `npx fabric-app-data`).
//   - When unset, isLiveTelemetryConfigured() is false and the app keeps using
//     the built-in synthetic feed, so nothing breaks until the model is wired.
//
// Schema is grounded in ontologies/WindTurbine/SemanticModel:
//   sensortelemetry(Timestamp, TurbineId, SensorId, SensorType, Value, Unit, Quality)
//   dimturbine(TurbineId, TurbineName, WindFarmId, Model, Manufacturer, RatedCapacityKW)
//   dimwindfarm(WindFarmId, WindFarmName, Location, Latitude, Longitude, InstalledCapacityMW)

import type { QueryTable } from "@microsoft/fabric-app-data";
import { getFabricClient } from "@/lib/fabric-client";

/** A wind farm (site) row projected from dimwindfarm. */
export interface LiveFarm {
    farmId: string;
    farmName: string;
    latitude: number;
    longitude: number;
    capacityMw: number;
}

/** A turbine → farm mapping row projected from dimturbine. */
export interface LiveTurbine {
    turbineId: string;
    farmId: string;
}

/** Latest pivoted signal values for a single turbine. */
export interface LiveMetrics {
    turbineId: string;
    powerKw: number;
    windMs: number;
    nacelleTempC: number;
    vibrationMmS: number;
}

/** Everything needed to assemble a live view of the farm. */
export interface LiveTelemetrySnapshot {
    farms: LiveFarm[];
    turbines: LiveTurbine[];
    metrics: LiveMetrics[];
}

/** A single latest sensor reading (one SensorType for one turbine). */
export interface TelemetryReading {
    turbineId: string;
    sensorType: string;
    value: number;
}

// Connection alias resolved from fabric.generated.ts (managed by `npx fabric-app-data`).
const LIVE_MODEL = import.meta.env.VITE_LIVE_TELEMETRY_MODEL?.trim();

// Maps the ontology sensor DEVICE type to an app signal, mirroring the pivot in
// ontologies/WindTurbine/Deploy-KqlTables.ps1 so live values line up with the
// SIGNAL_METADATA bands used for status classification.
const SIGNAL_BY_SENSOR_TYPE: Record<string, keyof Omit<LiveMetrics, "turbineId">> = {
    Anemometer: "windMs",
    CurrentSensor: "powerKw",
    Accelerometer: "vibrationMmS",
    Temperature: "nacelleTempC",
    Thermometer: "nacelleTempC",
};

// CurrentSensor readings are converted to kW with the same factor the KQL loader uses.
const CURRENT_TO_KW = 0.69;

export const DAX_FARMS =
    'EVALUATE SELECTCOLUMNS(dimwindfarm, ' +
    '"WindFarmId", dimwindfarm[WindFarmId], ' +
    '"WindFarmName", dimwindfarm[WindFarmName], ' +
    '"Latitude", dimwindfarm[Latitude], ' +
    '"Longitude", dimwindfarm[Longitude], ' +
    '"InstalledCapacityMW", dimwindfarm[InstalledCapacityMW])';

export const DAX_TURBINES =
    'EVALUATE SELECTCOLUMNS(dimturbine, ' +
    '"TurbineId", dimturbine[TurbineId], ' +
    '"WindFarmId", dimturbine[WindFarmId])';

// Latest Value per (TurbineId, SensorType). Timestamp is an ISO string, so MAX()
// gives the most recent reading; the inner CALCULATE isolates that row's value.
export const DAX_LATEST_TELEMETRY =
    'EVALUATE SELECTCOLUMNS(ADDCOLUMNS(' +
    'SUMMARIZE(sensortelemetry, sensortelemetry[TurbineId], sensortelemetry[SensorType]), ' +
    '"LatestValue", VAR ts = CALCULATE(MAX(sensortelemetry[Timestamp])) ' +
    'RETURN CALCULATE(MAX(sensortelemetry[Value]), sensortelemetry[Timestamp] = ts)), ' +
    '"TurbineId", sensortelemetry[TurbineId], ' +
    '"SensorType", sensortelemetry[SensorType], ' +
    '"Value", [LatestValue])';

// Default number of historical points to pull for a sparkline / forecast window.
export const DEFAULT_HISTORY_LIMIT = 40;

// Escape a value for safe inclusion in a DAX string literal (double any quotes).
// TurbineIds come from our own data, but this keeps the query well-formed and
// closes off any string-injection edge for defence in depth.
export function escapeDaxString(value: string): string {
    return value.replace(/"/g, '""');
}

/**
 * DAX for the last `limit` power readings (CurrentSensor) of one turbine, ordered
 * oldest→newest. TOPN takes the most recent by Timestamp (ISO strings sort
 * chronologically), then ORDER BY re-sorts them ascending for a left-to-right
 * sparkline. Raw Value stays in amps here; kW conversion happens in JS via
 * recordsToHistory so it matches the CURRENT_TO_KW factor used everywhere else.
 */
export function daxPowerHistory(turbineId: string, limit: number = DEFAULT_HISTORY_LIMIT): string {
    const id = escapeDaxString(turbineId);
    return (
        `EVALUATE TOPN(${limit}, FILTER(SELECTCOLUMNS(sensortelemetry, ` +
        '"Timestamp", sensortelemetry[Timestamp], ' +
        '"Value", sensortelemetry[Value], ' +
        '"TurbineId", sensortelemetry[TurbineId], ' +
        '"SensorType", sensortelemetry[SensorType]), ' +
        `[TurbineId] = "${id}" && [SensorType] = "CurrentSensor"), ` +
        '[Timestamp], DESC) ORDER BY [Timestamp] ASC'
    );
}

/** True when a live semantic-model connection alias is configured. */
export function isLiveTelemetryConfigured(): boolean {
    return Boolean(LIVE_MODEL);
}

// Normalise a DAX result column name: strip surrounding brackets and lowercase so
// "TurbineId", "[TurbineId]" and "sensortelemetry[TurbineId]" all match.
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

export function recordsToFarms(records: Record<string, unknown>[]): LiveFarm[] {
    return records
        .map((r) => ({
            farmId: toText(r["windfarmid"]),
            farmName: toText(r["windfarmname"]),
            latitude: toNumber(r["latitude"]),
            longitude: toNumber(r["longitude"]),
            capacityMw: toNumber(r["installedcapacitymw"]),
        }))
        .filter((f) => f.farmId !== "");
}

export function recordsToTurbines(records: Record<string, unknown>[]): LiveTurbine[] {
    return records
        .map((r) => ({
            turbineId: toText(r["turbineid"]),
            farmId: toText(r["windfarmid"]),
        }))
        .filter((t) => t.turbineId !== "");
}

export function recordsToReadings(records: Record<string, unknown>[]): TelemetryReading[] {
    return records
        .map((r) => ({
            turbineId: toText(r["turbineid"]),
            sensorType: toText(r["sensortype"]),
            value: toNumber(r["value"]),
        }))
        .filter((r) => r.turbineId !== "");
}

/**
 * Pivot flat latest-reading rows into one LiveMetrics per turbine, mapping each
 * sensor device type onto its app signal (wind / power / temp / vibration).
 */
export function pivotReadings(readings: TelemetryReading[]): LiveMetrics[] {
    const byTurbine = new Map<string, LiveMetrics>();
    for (const reading of readings) {
        const signal = SIGNAL_BY_SENSOR_TYPE[reading.sensorType];
        if (!signal) {
            continue;
        }
        let metrics = byTurbine.get(reading.turbineId);
        if (!metrics) {
            metrics = { turbineId: reading.turbineId, powerKw: 0, windMs: 0, nacelleTempC: 0, vibrationMmS: 0 };
            byTurbine.set(reading.turbineId, metrics);
        }
        const value = signal === "powerKw" ? Math.round(reading.value * CURRENT_TO_KW * 10) / 10 : reading.value;
        metrics[signal] = value;
    }
    return [...byTurbine.values()];
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
 * Fetch a full live snapshot (farms + turbines + latest metrics) from the
 * semantic model. Returns null when unconfigured or when any query fails, so
 * callers can transparently fall back to the synthetic feed.
 */
export async function fetchLiveTelemetry(): Promise<LiveTelemetrySnapshot | null> {
    if (!isLiveTelemetryConfigured()) {
        return null;
    }
    const [farmRecords, turbineRecords, telemetryRecords] = await Promise.all([
        runQuery(DAX_FARMS),
        runQuery(DAX_TURBINES),
        runQuery(DAX_LATEST_TELEMETRY),
    ]);
    if (!farmRecords || !turbineRecords || !telemetryRecords) {
        return null;
    }
    return {
        farms: recordsToFarms(farmRecords),
        turbines: recordsToTurbines(turbineRecords),
        metrics: pivotReadings(recordsToReadings(telemetryRecords)),
    };
}

/**
 * Project raw history rows ({ timestamp, value }) into a chronologically ordered
 * array of power values in kW. Rows are sorted oldest→newest defensively (the
 * DAX already orders ASC) and CurrentSensor amps are converted with the same
 * CURRENT_TO_KW factor the KQL loader and latest-value pivot use, so the seeded
 * sparkline history lines up with the live "current power" reading.
 */
export function recordsToHistory(records: Record<string, unknown>[]): number[] {
    return records
        .map((r) => ({ timestamp: toText(r["timestamp"]), value: toNumber(r["value"]) }))
        .filter((p) => p.timestamp !== "")
        .sort((a, b) => (a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0))
        .map((p) => Math.round(p.value * CURRENT_TO_KW * 10) / 10);
}

/**
 * Fetch persisted power history (kW) for one turbine from the semantic model so
 * sparklines and forecasts reflect real Eventhouse-backed readings instead of a
 * cold in-browser accumulation. Returns null when unconfigured or on any query
 * failure, letting callers fall back to the live/synthetic accumulation.
 */
export async function fetchPowerHistory(
    turbineId: string,
    limit: number = DEFAULT_HISTORY_LIMIT,
): Promise<number[] | null> {
    if (!isLiveTelemetryConfigured() || turbineId === "") {
        return null;
    }
    const records = await runQuery(daxPowerHistory(turbineId, limit));
    if (!records) {
        return null;
    }
    return recordsToHistory(records);
}
