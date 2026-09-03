//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Live telemetry service — pulls REAL process-unit readings from the deployed
// OilGasRefinery semantic model (Direct Lake over the Lakehouse) instead of the
// synthetic buildFarm() generator.
//
// Activation is fully opt-in and fallback-safe:
//   - Set VITE_LIVE_TELEMETRY_MODEL to the semantic-model connection alias
//     registered in fabric.yaml / fabric.generated.ts (via `npx fabric-app-data`).
//   - When unset, isLiveTelemetryConfigured() is false and the app keeps using
//     the built-in synthetic feed, so nothing breaks until the model is wired.
//
// Schema is grounded in ontologies/OilGasRefinery/SemanticModel:
//   sensortelemetry(Timestamp, ProcessUnitId, SensorId, SensorType, Value, Unit, Quality)
//   dimprocessunit(ProcessUnitId, ProcessUnitName, ProcessUnitType, RefineryId, CapacityBPD, ...)
//   dimrefinery(RefineryId, RefineryName, Country, State, City, Latitude, Longitude, CapacityBPD, ...)

import type { QueryTable } from "@microsoft/fabric-app-data";
import { getFabricClient } from "@/lib/fabric-client";

/** A refinery (site) row projected from dimrefinery. */
export interface LiveRefinery {
    refineryId: string;
    refineryName: string;
    latitude: number;
    longitude: number;
    capacityKbd: number;
}

/** A process unit → refinery mapping row projected from dimprocessunit. */
export interface LiveUnit {
    unitId: string;
    refineryId: string;
    unitName?: string;
}

/** Latest pivoted signal values for a single process unit. */
export interface LiveMetrics {
    unitId: string;
    powerKw: number;
    irradianceWm2: number;
    moduleTempC: number;
    inverterLoadPct: number;
    latestAt?: string;
}

/** Everything needed to assemble a live view of the refinery fleet. */
export interface LiveTelemetrySnapshot {
    refineries: LiveRefinery[];
    units: LiveUnit[];
    metrics: LiveMetrics[];
}

/** Safety alarm projected from the protected semantic-model route. */
export interface LiveSafetyAlarm {
    alarmId: string;
    sensorId: string;
    severity: string;
    timestamp: string;
    alarmType: string;
    description: string;
    alarmValue: number;
    thresholdValue: number;
    acknowledgedAt?: string;
}

export interface LiveSensorLookup {
    sensorId: string;
    equipmentId: string;
    sensorName: string;
}

export interface LiveEquipmentLookup {
    equipmentId: string;
    equipmentName: string;
    equipmentType: string;
    processUnitId: string;
    criticality: string;
}

export interface EnrichedLiveIncident extends LiveSafetyAlarm {
    equipmentId?: string;
    equipmentName?: string;
    equipmentType?: string;
    processUnitId?: string;
    processUnitName?: string;
    refineryId?: string;
    refineryName?: string;
}

/** A single latest sensor reading (one SensorType for one process unit). */
export interface TelemetryReading {
    unitId: string;
    sensorType: string;
    value: number;
    timestamp?: string;
}

// Connection alias resolved from fabric.generated.ts (managed by `npx fabric-app-data`).
const LIVE_MODEL = import.meta.env.VITE_LIVE_TELEMETRY_MODEL?.trim();

// Maps the ontology telemetry SensorType to an app signal. The refinery app repurposes
// the shared PlantTelemetry fields: powerKw = throughput (kbd), irradianceWm2 = feed rate
// (kbd), moduleTempC = unit temperature (°C), inverterLoadPct = utilization (%). The names
// line up with SIGNAL_METADATA.ontologyProperty so live values feed the same status bands.
const SIGNAL_BY_SENSOR_TYPE: Record<string, keyof Omit<LiveMetrics, "unitId">> = {
    Throughput: "powerKw",
    FeedRate: "irradianceWm2",
    UnitTemp: "moduleTempC",
    Utilization: "inverterLoadPct",
};

export const DAX_REFINERIES =
    'EVALUATE SELECTCOLUMNS(dimrefinery, ' +
    '"RefineryId", dimrefinery[RefineryId], ' +
    '"RefineryName", dimrefinery[RefineryName], ' +
    '"Latitude", dimrefinery[Latitude], ' +
    '"Longitude", dimrefinery[Longitude], ' +
    '"CapacityBPD", dimrefinery[CapacityBPD])';

export const DAX_UNITS =
    'EVALUATE SELECTCOLUMNS(dimprocessunit, ' +
    '"ProcessUnitId", dimprocessunit[ProcessUnitId], ' +
    '"RefineryId", dimprocessunit[RefineryId], ' +
    '"ProcessUnitName", dimprocessunit[ProcessUnitName])';

export const DAX_SAFETY_ALARMS =
    'EVALUATE SELECTCOLUMNS(factsafetyalarm, ' +
    '"AlarmId", factsafetyalarm[AlarmId], "SensorId", factsafetyalarm[SensorId], ' +
    '"Severity", factsafetyalarm[Severity], "AlarmTimestamp", factsafetyalarm[AlarmTimestamp], ' +
    '"AlarmType", factsafetyalarm[AlarmType], "Description", factsafetyalarm[Description], ' +
    '"AlarmValue", factsafetyalarm[AlarmValue], "ThresholdValue", factsafetyalarm[ThresholdValue], ' +
    '"AcknowledgedTimestamp", factsafetyalarm[AcknowledgedTimestamp])';

export const DAX_SENSOR_LOOKUP =
    'EVALUATE SELECTCOLUMNS(dimsensor, "SensorId", dimsensor[SensorId], "EquipmentId", dimsensor[EquipmentId], "SensorName", dimsensor[SensorName])';

export const DAX_EQUIPMENT_LOOKUP =
    'EVALUATE SELECTCOLUMNS(dimequipment, "EquipmentId", dimequipment[EquipmentId], "EquipmentName", dimequipment[EquipmentName], "EquipmentType", dimequipment[EquipmentType], "ProcessUnitId", dimequipment[ProcessUnitId], "CriticalityLevel", dimequipment[CriticalityLevel])';

// Latest Value per (ProcessUnitId, SensorType). Timestamp is an ISO string, so MAX()
// gives the most recent reading; the inner CALCULATE isolates that row's value.
export const DAX_LATEST_TELEMETRY =
    'EVALUATE SELECTCOLUMNS(ADDCOLUMNS(' +
    'SUMMARIZE(sensortelemetry, sensortelemetry[ProcessUnitId], sensortelemetry[SensorType]), ' +
    '"LatestValue", VAR ts = CALCULATE(MAX(sensortelemetry[Timestamp])) ' +
    'RETURN CALCULATE(MAX(sensortelemetry[Value]), sensortelemetry[Timestamp] = ts)), ' +
    '"ProcessUnitId", sensortelemetry[ProcessUnitId], ' +
    '"SensorType", sensortelemetry[SensorType], ' +
    '"Value", [LatestValue], ' +
    '"Timestamp", CALCULATE(MAX(sensortelemetry[Timestamp])))';

// Default number of historical points to pull for a sparkline / forecast window.
export const DEFAULT_HISTORY_LIMIT = 40;
// Default number of anomaly-score points used by the trend forecast watch.
export const DEFAULT_ANOMALY_LIMIT = 12;

// Escape a value for safe inclusion in a DAX string literal (double any quotes).
// ProcessUnitIds come from our own data, but this keeps the query well-formed
// and closes off any string-injection edge for defence in depth.
export function escapeDaxString(value: string): string {
    return value.replace(/"/g, '""');
}

/**
 * DAX for the last `limit` throughput readings of one process unit, ordered
 * oldest→newest. TOPN takes the most recent by Timestamp (ISO strings sort
 * chronologically), then ORDER BY re-sorts them ascending for a left-to-right
 * sparkline. Throughput (kbd) maps to the shared powerKw field with no conversion.
 */
export function daxPowerHistory(unitId: string, limit: number = DEFAULT_HISTORY_LIMIT): string {
    const id = escapeDaxString(unitId);
    return (
        `EVALUATE TOPN(${limit}, FILTER(SELECTCOLUMNS(sensortelemetry, ` +
        '"Timestamp", sensortelemetry[Timestamp], ' +
        '"Value", sensortelemetry[Value], ' +
        '"ProcessUnitId", sensortelemetry[ProcessUnitId], ' +
        '"SensorType", sensortelemetry[SensorType]), ' +
        `[ProcessUnitId] = "${id}" && [SensorType] = "Throughput"), ` +
        '[Timestamp], DESC) ORDER BY [Timestamp] ASC'
    );
}

/**
 * DAX for the last `limit` anomaly signal snapshots of one process unit,
 * ordered oldest→newest. Each row includes UnitTemp and Utilization values for
 * the same timestamp so the app can derive the same anomaly score it uses live.
 */
export function daxAnomalyScores(unitId: string, limit: number = DEFAULT_ANOMALY_LIMIT): string {
    const id = escapeDaxString(unitId);
    return (
        `EVALUATE TOPN(${limit}, ADDCOLUMNS(` +
        `SUMMARIZE(FILTER(sensortelemetry, sensortelemetry[ProcessUnitId] = "${id}" && ` +
        '(sensortelemetry[SensorType] = "UnitTemp" || sensortelemetry[SensorType] = "Utilization")), ' +
        'sensortelemetry[Timestamp]), ' +
        '"UnitTemp", CALCULATE(MAX(sensortelemetry[Value]), sensortelemetry[SensorType] = "UnitTemp"), ' +
        '"Utilization", CALCULATE(MAX(sensortelemetry[Value]), sensortelemetry[SensorType] = "Utilization")), ' +
        '[Timestamp], DESC) ORDER BY [Timestamp] ASC'
    );
}

/** True when a live semantic-model connection alias is configured. */
export function isLiveTelemetryConfigured(): boolean {
    return Boolean(LIVE_MODEL);
}

// Normalise a DAX result column name: strip surrounding brackets and lowercase so
// "RefineryId", "[RefineryId]" and "dimrefinery[RefineryId]" all match.
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

function toOptionalNumber(value: unknown): number | null {
    if (value == null || value === "") {
        return null;
    }
    const n = typeof value === "number" ? value : Number(value);
    return Number.isFinite(n) ? n : null;
}

function toText(value: unknown): string {
    return value == null ? "" : String(value);
}

export function recordsToRefineries(records: Record<string, unknown>[]): LiveRefinery[] {
    return records
        .map((r) => ({
            refineryId: toText(r["refineryid"]),
            refineryName: toText(r["refineryname"]),
            latitude: toNumber(r["latitude"]),
            longitude: toNumber(r["longitude"]),
            // dimrefinery.CapacityBPD is barrels/day; the app renders capacity in kbd.
            capacityKbd: Math.round(toNumber(r["capacitybpd"]) / 1000),
        }))
        .filter((f) => f.refineryId !== "");
}

export function recordsToUnits(records: Record<string, unknown>[]): LiveUnit[] {
    return records
        .map((r) => ({
            unitId: toText(r["processunitid"]),
            refineryId: toText(r["refineryid"]),
            unitName: toText(r["processunitname"]) || undefined,
        }))
        .filter((t) => t.unitId !== "");
}

export function recordsToSensorLookup(records: Record<string, unknown>[]): LiveSensorLookup[] {
    return records.map((r) => ({ sensorId: toText(r["sensorid"]), equipmentId: toText(r["equipmentid"]), sensorName: toText(r["sensorname"]) }))
        .filter((sensor) => sensor.sensorId !== "" && sensor.equipmentId !== "");
}

export function recordsToEquipmentLookup(records: Record<string, unknown>[]): LiveEquipmentLookup[] {
    return records.map((r) => ({
        equipmentId: toText(r["equipmentid"]), equipmentName: toText(r["equipmentname"]), equipmentType: toText(r["equipmenttype"]),
        processUnitId: toText(r["processunitid"]), criticality: toText(r["criticalitylevel"]),
    })).filter((asset) => asset.equipmentId !== "" && asset.processUnitId !== "");
}

export function recordsToSafetyAlarms(records: Record<string, unknown>[]): LiveSafetyAlarm[] {
    return records.map((r) => ({
        alarmId: toText(r["alarmid"]),
        sensorId: toText(r["sensorid"]),
        severity: toText(r["severity"]),
        timestamp: toText(r["alarmtimestamp"]),
        alarmType: toText(r["alarmtype"]),
        description: toText(r["description"]),
        alarmValue: toNumber(r["alarmvalue"]),
        thresholdValue: toNumber(r["thresholdvalue"]),
        acknowledgedAt: toText(r["acknowledgedtimestamp"]) || undefined,
    })).filter((alarm) => alarm.alarmId !== "" && alarm.sensorId !== "");
}

export function enrichSafetyAlarms(
    alarms: LiveSafetyAlarm[],
    sensors: LiveSensorLookup[],
    equipment: LiveEquipmentLookup[],
    units: Array<LiveUnit & { unitName?: string }>,
    refineries: LiveRefinery[],
): EnrichedLiveIncident[] {
    const sensorById = new Map(sensors.map((sensor) => [sensor.sensorId, sensor]));
    const equipmentById = new Map(equipment.map((asset) => [asset.equipmentId, asset]));
    const unitById = new Map(units.map((unit) => [unit.unitId, unit]));
    const refineryById = new Map(refineries.map((refinery) => [refinery.refineryId, refinery]));
    return alarms.map((alarm) => {
        const sensor = sensorById.get(alarm.sensorId);
        const asset = sensor ? equipmentById.get(sensor.equipmentId) : undefined;
        const unit = asset ? unitById.get(asset.processUnitId) : undefined;
        const refinery = unit ? refineryById.get(unit.refineryId) : undefined;
        return {
            ...alarm,
            equipmentId: asset?.equipmentId,
            equipmentName: asset?.equipmentName,
            equipmentType: asset?.equipmentType,
            processUnitId: asset?.processUnitId,
            processUnitName: unit?.unitName,
            refineryId: unit?.refineryId,
            refineryName: refinery?.refineryName,
        };
    });
}

export function recordsToReadings(records: Record<string, unknown>[]): TelemetryReading[] {
    return records
        .map((r) => ({
            unitId: toText(r["processunitid"]),
            sensorType: toText(r["sensortype"]),
            value: toNumber(r["value"]),
            timestamp: toText(r["timestamp"]) || undefined,
        }))
        .filter((r) => r.unitId !== "");
}

/**
 * Pivot flat latest-reading rows into one LiveMetrics per process unit, mapping each
 * telemetry SensorType onto its app signal (throughput / feed / unit temp / utilization).
 */
export function pivotReadings(readings: TelemetryReading[]): LiveMetrics[] {
    const byUnit = new Map<string, LiveMetrics>();
    for (const reading of readings) {
        const signal = SIGNAL_BY_SENSOR_TYPE[reading.sensorType];
        if (!signal) {
            continue;
        }
        let metrics = byUnit.get(reading.unitId);
        if (!metrics) {
            metrics = { unitId: reading.unitId, powerKw: 0, irradianceWm2: 0, moduleTempC: 0, inverterLoadPct: 0 };
            byUnit.set(reading.unitId, metrics);
        }
        metrics[signal] = reading.value;
        if (reading.timestamp && (!metrics.latestAt || reading.timestamp > metrics.latestAt)) {
            metrics.latestAt = reading.timestamp;
        }
    }
    return [...byUnit.values()];
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
 * Fetch a full live snapshot (refineries + units + latest metrics) from the
 * semantic model. Returns null when unconfigured or when any query fails, so
 * callers can transparently fall back to the synthetic feed.
 */
export async function fetchLiveTelemetry(): Promise<LiveTelemetrySnapshot | null> {
    if (!isLiveTelemetryConfigured()) {
        return null;
    }
    const [refineryRecords, unitRecords, telemetryRecords] = await Promise.all([
        runQuery(DAX_REFINERIES),
        runQuery(DAX_UNITS),
        runQuery(DAX_LATEST_TELEMETRY),
    ]);
    if (!refineryRecords || !unitRecords || !telemetryRecords) {
        return null;
    }
    return {
        refineries: recordsToRefineries(refineryRecords),
        units: recordsToUnits(unitRecords),
        metrics: pivotReadings(recordsToReadings(telemetryRecords)),
    };
}

/**
 * Fetch safety alarms and enrich them through the Direct Lake model's Sensor,
 * Equipment, Process Unit, and Refinery dimensions. This is the browser-safe
 * counterpart to the Eventhouse EnrichedEquipmentAlert() function: it uses the
 * Fabric host proxy and never exposes KQL credentials to client code.
 */
export async function fetchLiveIncidents(): Promise<EnrichedLiveIncident[] | null> {
    if (!isLiveTelemetryConfigured()) {
        return null;
    }
    const [alarmRecords, sensorRecords, equipmentRecords, unitRecords, refineryRecords] = await Promise.all([
        runQuery(DAX_SAFETY_ALARMS),
        runQuery(DAX_SENSOR_LOOKUP),
        runQuery(DAX_EQUIPMENT_LOOKUP),
        runQuery(DAX_UNITS),
        runQuery(DAX_REFINERIES),
    ]);
    if (!alarmRecords || !sensorRecords || !equipmentRecords || !unitRecords || !refineryRecords) {
        return null;
    }
    return enrichSafetyAlarms(
        recordsToSafetyAlarms(alarmRecords),
        recordsToSensorLookup(sensorRecords),
        recordsToEquipmentLookup(equipmentRecords),
        recordsToUnits(unitRecords),
        recordsToRefineries(refineryRecords),
    );
}

/**
 * Project raw history rows ({ timestamp, value }) into a chronologically ordered
 * array of throughput values (kbd). Rows are sorted oldest→newest defensively
 * (the DAX already orders ASC) so the seeded sparkline history lines up with the
 * live "current throughput" reading.
 */
export function recordsToHistory(records: Record<string, unknown>[]): number[] {
    return records
        .map((r) => ({ timestamp: toText(r["timestamp"]), value: toNumber(r["value"]) }))
        .filter((p) => p.timestamp !== "")
        .sort((a, b) => (a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0))
        .map((p) => Math.round(p.value * 10) / 10);
}

/**
 * Convert timestamped unit-temp/utilization signal rows into anomaly scores
 * (0..1) using the same thresholds as App.anomalyScore for refinery.
 */
export function recordsToAnomalyScores(records: Record<string, unknown>[]): number[] {
    return records
        .map((r) => {
            const timestamp = toText(r["timestamp"]);
            const unitTemp = toOptionalNumber(r["unittemp"]);
            const utilization = toOptionalNumber(r["utilization"]);
            return { timestamp, unitTemp, utilization };
        })
        .filter((p) => p.timestamp !== "" && (p.unitTemp != null || p.utilization != null))
        .sort((a, b) => (a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0))
        .map((p) => {
            const tempScore = p.unitTemp == null ? 0 : (p.unitTemp - 360) / 70;
            const loadScore = p.utilization == null ? 0 : (p.utilization - 80) / 18;
            const score = Math.max(0, tempScore, loadScore);
            return +Math.min(1, score).toFixed(3);
        });
}

/**
 * Fetch persisted throughput history (kbd) for one process unit from the semantic
 * model so sparklines and forecasts reflect real Lakehouse-backed readings instead
 * of a cold in-browser accumulation. Returns null when unconfigured or on any
 * query failure, letting callers fall back to the live/synthetic accumulation.
 */
export async function fetchPowerHistory(
    unitId: string,
    limit: number = DEFAULT_HISTORY_LIMIT,
): Promise<number[] | null> {
    if (!isLiveTelemetryConfigured() || unitId === "") {
        return null;
    }
    const records = await runQuery(daxPowerHistory(unitId, limit));
    if (!records) {
        return null;
    }
    return recordsToHistory(records);
}

/**
 * Fetch persisted anomaly-score history for one process unit from the semantic
 * model. Returns null when unconfigured or on any query failure, allowing
 * callers to keep the existing in-memory rolling-window behavior.
 */
export async function fetchAnomalyScores(
    unitId: string,
    limit: number = DEFAULT_ANOMALY_LIMIT,
): Promise<number[] | null> {
    if (!isLiveTelemetryConfigured() || unitId === "") {
        return null;
    }
    const records = await runQuery(daxAnomalyScores(unitId, limit));
    if (!records) {
        return null;
    }
    return recordsToAnomalyScores(records);
}
