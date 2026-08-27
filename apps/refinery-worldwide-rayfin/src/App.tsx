//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { Suspense, useCallback, useEffect, useMemo, useRef, useState, type ReactNode, type PointerEvent as ReactPointerEvent, type WheelEvent as ReactWheelEvent } from "react";
import { lazyRetry } from "@/lib/lazy-retry";
import * as THREE from "three";
import {
    answerQuestion,
    askOntology,
    createUnitDevice,
    deleteUnitDevice,
    ensureOntologySites,
    ensureSignalThresholds,
    ensureUnitDevices,
    listSignalThresholds,
    listUnitDevices,
    recentDispatchNotes,
    recentMaintenanceOrders,
    saveDispatchNote,
    saveMaintenanceOrder,
    updateUnitDevice,
    type DispatchNoteRecord,
    type MaintenanceOrderRecord,
    type SignalThresholdRecord,
    type UnitDeviceRecord,
} from "@/services/ontology-data.service";
import { isDataAgentConfigured, queryDataAgent, testDataAgentConnection, type DataAgentConnectionResult } from "@/services/data-agent.service";
import {
    fetchLiveTelemetry,
    fetchAnomalyScores,
    fetchPowerHistory,
    isLiveTelemetryConfigured,
    type LiveTelemetrySnapshot,
} from "@/services/live-telemetry.service";
import { classifyAskIntent, normalizeAskQuestion } from "@/services/ask-routing.service";
import { canManageDispatch, normalizeOperatorRole, type OperatorRole } from "@/services/operator-role.service";
import { HISTORY_WINDOWS, historyPointLimit, type HistoryWindow } from "@/services/history-window.service";
import { compareScenarios, forecastVsRealised, buildScenarioPrompt, summarizeComparison, computeInsights, answerScenarioQuestion, type ScenarioSpec } from "@/services/scenario-lab.service";
import { classifyImport, summarizeActuals, type ImportedActualsResult } from "@/services/scenario-import.service";
import {
    buildMissionChallenge,
    buildMissionReport,
    buildEscalationTimeline,
    pushMissionRun,
    summarizeMissionRun,
    summarizeResponderAvailability,
    slaUrgency,
    narrateStep,
    REFINERY_DEMO_MANIFEST,
    type DemoScriptStepId,
    type EscalationStage,
    type MissionReport,
    type MissionReportEvent,
} from "@/services/demo-experience.service";
import { SceneErrorBoundary } from "@/components/SceneErrorBoundary";
import { isTeamsAlertConfigured, postTeamsAlert } from "@/services/teams-alert.service";

type PlantStatus = "healthy" | "warning" | "alarm";

type PlantTelemetry = {
    id: string;
    siteId: string;
    siteName: string;
    latitude: number;
    longitude: number;
    x: number;
    z: number;
    powerKw: number;
    irradianceWm2: number;
    moduleTempC: number;
    inverterLoadPct: number;
    status: PlantStatus;
};

type SolarPlantSite = {
    id: string;
    name: string;
    region: string;
    lat: number;
    lon: number;
    arrayCount: number;
    capacityMw: number;
};

type AskResult = {
    source: "fabriciq" | "ontology" | "local";
    summary: string;
    generatedAt: string;
    queryText?: string;
    confidence?: number;
    evidence?: string[];
    transport?: "legacy" | "mcp";
    fallbackReason?: string;
    cacheHit?: boolean;
};

type AskHistoryEntry = {
    at: string;
    question: string;
    source: AskResult["source"];
    transport?: AskResult["transport"];
    latencyMs: number;
    cacheHit: boolean;
    fallbackReason?: string;
};

const ASK_CACHE_TTL_MS = 30_000;
const ASK_HISTORY_MAX = 8;

export const STATUS_COLORS: Record<PlantStatus, string> = {
    healthy: "#58d68d",
    warning: "#ffd166",
    alarm: "#ef476f",
};

export const SITE_COLORS = ["#6ee7ff", "#9bffb0", "#fbc2eb", "#ffd166", "#b4d4ff", "#a1c4fd"];

// Major worldwide crude-oil refineries, positioned by their real coordinates across
// the globe. capacityMw carries the crude distillation capacity in thousand barrels
// per day (kbd); arrayCount is the number of process-unit blocks rendered for the site.
const SITES: SolarPlantSite[] = [
    { id: "JAMNAGAR", name: "Jamnagar", region: "India", lat: 22.34, lon: 70.02, arrayCount: 8, capacityMw: 1240 },
    { id: "PARAGUANA", name: "Paraguan\u00e1", region: "Venezuela", lat: 11.72, lon: -70.20, arrayCount: 7, capacityMw: 940 },
    { id: "ULSAN", name: "Ulsan", region: "South Korea", lat: 35.51, lon: 129.38, arrayCount: 7, capacityMw: 840 },
    { id: "RUWAIS", name: "Ruwais", region: "United Arab Emirates", lat: 24.11, lon: 52.73, arrayCount: 6, capacityMw: 817 },
    { id: "PORTARTHUR", name: "Port Arthur", region: "United States", lat: 29.88, lon: -93.94, arrayCount: 6, capacityMw: 600 },
    { id: "RASTANURA", name: "Ras Tanura", region: "Saudi Arabia", lat: 26.65, lon: 50.16, arrayCount: 6, capacityMw: 550 },
    { id: "JURONG", name: "Jurong Island", region: "Singapore", lat: 1.26, lon: 103.70, arrayCount: 5, capacityMw: 500 },
    { id: "PERNIS", name: "Pernis", region: "Netherlands", lat: 51.88, lon: 4.39, arrayCount: 5, capacityMw: 404 },
];

export function seededRand(seed: number) {
    const s = Math.sin(seed) * 10000;
    return s - Math.floor(s);
}

// Worldwide equirectangular projection onto the 92 x 52 world plane: longitude
// -180..180 maps across X, latitude 90..-90 across Z, so every refinery lands on its
// real position on the world map.
export function projectLonToX(lon: number) {
    return ((lon + 180) / 360) * 92 - 46;
}

export function projectLatToZ(lat: number) {
    // Equirectangular is a 2:1 projection (360° lon × 180° lat). With the map
    // plane 92 units wide, the depth must be 46 (92 / 2) so continents keep their
    // true proportions instead of being stretched vertically.
    return ((90 - lat) / 180) * 46 - 23;
}

// Stylized continent + major-island outlines in [lon, lat]. Detailed enough that each
// landmass reads as a recognizable world map behind the refineries, while staying light
// to draw onto the equirectangular canvas texture.
const WORLD: number[][][] = [
    // North America
    [
        [-165, 60], [-160, 66], [-156, 71], [-130, 70], [-110, 69], [-95, 70],
        [-82, 73], [-78, 63], [-64, 60], [-56, 52], [-67, 45], [-70, 41], [-74, 40],
        [-76, 35], [-81, 31], [-80, 25], [-83, 25], [-90, 29], [-97, 26], [-97, 20],
        [-91, 18], [-88, 21], [-94, 16], [-105, 20], [-110, 23], [-114, 30], [-117, 33],
        [-122, 37], [-124, 42], [-124, 48], [-131, 53], [-140, 59], [-150, 59],
        [-160, 58], [-165, 60],
    ],
    // Greenland
    [
        [-46, 60], [-30, 60], [-20, 70], [-22, 76], [-32, 83], [-50, 82], [-58, 76],
        [-54, 68], [-50, 60], [-46, 60],
    ],
    // South America
    [
        [-78, 8], [-72, 11], [-62, 10], [-50, 5], [-50, 0], [-44, -2], [-35, -6],
        [-35, -12], [-39, -18], [-48, -25], [-54, -34], [-58, -39], [-65, -42],
        [-69, -45], [-74, -50], [-72, -52], [-70, -45], [-71, -37], [-73, -30],
        [-71, -18], [-77, -12], [-81, -5], [-79, 2], [-78, 8],
    ],
    // Europe
    [
        [-9, 43], [-9, 38], [-6, 36], [0, 39], [3, 42], [8, 44], [12, 44], [16, 41],
        [18, 40], [24, 40], [27, 41], [33, 45], [40, 46], [38, 54], [30, 60], [24, 66],
        [18, 69], [12, 66], [10, 60], [5, 61], [8, 57], [5, 53], [0, 51], [-2, 49],
        [-5, 48], [-9, 43],
    ],
    // British Isles
    [
        [-6, 50], [-3, 51], [1, 52], [-1, 55], [-3, 58], [-6, 58], [-8, 55],
        [-10, 52], [-6, 50],
    ],
    // Iceland
    [
        [-24, 65], [-19, 66], [-14, 65], [-18, 64], [-22, 64], [-24, 65],
    ],
    // Africa
    [
        [-16, 15], [-16, 21], [-10, 30], [0, 34], [10, 34], [11, 37], [20, 33],
        [25, 32], [32, 31], [35, 27], [37, 22], [43, 12], [51, 12], [45, 5], [42, -2],
        [40, -10], [35, -18], [32, -26], [26, -34], [20, -35], [18, -30], [15, -22],
        [12, -16], [9, -5], [5, 4], [-4, 5], [-8, 10], [-12, 13], [-16, 15],
    ],
    // Madagascar
    [
        [43, -12], [46, -15], [50, -18], [48, -23], [45, -25], [44, -20], [43, -16], [43, -12],
    ],
    // Asia
    [
        [28, 40], [36, 37], [44, 39], [50, 38], [58, 38], [66, 37], [70, 30], [72, 21],
        [76, 9], [78, 8], [82, 17], [88, 21], [90, 16], [95, 16], [98, 10], [104, 9],
        [106, 11], [109, 15], [108, 21], [112, 22], [118, 24], [121, 31], [122, 38],
        [128, 42], [135, 46], [143, 50], [150, 59], [160, 62], [170, 66], [178, 68],
        [160, 72], [140, 74], [110, 76], [90, 74], [70, 70], [60, 68], [55, 58],
        [50, 50], [44, 46], [36, 44], [28, 40],
    ],
    // Arabian Peninsula
    [
        [35, 30], [40, 31], [48, 30], [57, 25], [56, 20], [52, 16], [45, 13], [43, 17],
        [40, 21], [36, 25], [35, 30],
    ],
    // Japan
    [
        [130, 31], [133, 34], [138, 36], [141, 40], [140, 43], [143, 44], [141, 38],
        [138, 35], [135, 33], [131, 31], [130, 31],
    ],
    // Sumatra
    [
        [95, 5], [100, 2], [104, -2], [106, -6], [101, -4], [97, 1], [95, 5],
    ],
    // Borneo
    [
        [109, 2], [114, 4], [118, 5], [119, 0], [116, -4], [110, -3], [109, 2],
    ],
    // New Guinea
    [
        [131, -1], [138, -3], [144, -4], [150, -7], [146, -8], [140, -8], [134, -6], [131, -1],
    ],
    // Australia
    [
        [114, -22], [122, -18], [130, -12], [137, -12], [142, -11], [145, -15],
        [147, -20], [153, -26], [150, -33], [145, -38], [140, -38], [134, -35],
        [129, -32], [123, -34], [115, -34], [114, -28], [114, -22],
    ],
    // New Zealand
    [
        [173, -35], [176, -38], [178, -41], [174, -42], [171, -44], [167, -46],
        [170, -42], [172, -38], [173, -35],
    ],
];

type SignalKey = "power" | "irradiance" | "moduleTemp" | "inverterLoad";

interface SignalMetadata {
    key: SignalKey;
    label: string;
    ontologyProperty: string;
    unit: string;
    warn: number;
    alarm: number;
    governsHealth: boolean;
    get: (t: PlantTelemetry) => number;
}

// Single source of truth for signal bands, mirrored from the RefineryUnit ontology Sensor
// metadata (Unit, MinThreshold/MaxThreshold) and the Data Agent threshold guidance
// (e.g. unit temperature: normal <380, warning 380–430, critical >430 °C). Health is
// governed only by condition signals (unit temp, utilization); throughput and feed rate
// are informational context, so they never drive a unit's alarm status.
const SIGNAL_METADATA: Record<SignalKey, SignalMetadata> = {
    power: { key: "power", label: "Throughput", ontologyProperty: "ThroughputKBD", unit: "kbd", warn: Infinity, alarm: Infinity, governsHealth: false, get: (t) => t.powerKw },
    irradiance: { key: "irradiance", label: "Feed rate", ontologyProperty: "FeedRateKBD", unit: "kbd", warn: Infinity, alarm: Infinity, governsHealth: false, get: (t) => t.irradianceWm2 },
    moduleTemp: { key: "moduleTemp", label: "Unit temp", ontologyProperty: "UnitTempC", unit: "\u00b0C", warn: 380, alarm: 430, governsHealth: true, get: (t) => t.moduleTempC },
    inverterLoad: { key: "inverterLoad", label: "Utilization", ontologyProperty: "UtilizationPct", unit: "%", warn: 90, alarm: 98, governsHealth: true, get: (t) => t.inverterLoadPct },
};

const SIGNAL_ORDER: SignalKey[] = ["power", "irradiance", "moduleTemp", "inverterLoad"];
const STATUS_RANK: Record<PlantStatus, number> = { healthy: 0, warning: 1, alarm: 2 };

// Runtime warn/alarm overrides sourced from the ontology backend (SensorThreshold
// store) at startup. Empty by default so the pure classifiers fall back to the
// static SIGNAL_METADATA bands — keeping default behaviour and unit tests
// deterministic. Applying overrides is fallback-safe: an unreachable backend
// simply leaves the compiled-in defaults in place.
const THRESHOLD_OVERRIDES = new Map<SignalKey, { warn: number; alarm: number }>();

function isSignalKey(key: string): key is SignalKey {
    return key === "power" || key === "irradiance" || key === "moduleTemp" || key === "inverterLoad";
}

// Resolve the active band for a signal: ontology override when present, else the
// compiled-in default.
function activeBands(key: SignalKey): { warn: number; alarm: number } {
    return THRESHOLD_OVERRIDES.get(key) ?? SIGNAL_METADATA[key];
}

// Adopt ontology-published threshold bands as runtime overrides. Only rows with a
// known signalKey and a coherent band (non-negative, warn <= alarm; Infinity
// allowed for informational signals) are accepted; malformed rows are ignored so
// a bad backend row can never destabilise classification. Returns the count applied.
export function applyThresholdOverrides(rows: SignalThresholdRecord[]): number {
    let applied = 0;
    for (const row of rows) {
        if (!isSignalKey(row.signalKey)) continue;
        if (!(row.warn >= 0) || !(row.alarm >= 0) || row.warn > row.alarm) continue;
        THRESHOLD_OVERRIDES.set(row.signalKey, { warn: row.warn, alarm: row.alarm });
        applied += 1;
    }
    return applied;
}

// Drop every runtime override, restoring the compiled-in default bands.
export function clearThresholdOverrides(): void {
    THRESHOLD_OVERRIDES.clear();
}

// Classify a single signal reading against its active threshold band.
export function signalState(key: SignalKey, value: number): PlantStatus {
    const m = activeBands(key);
    if (value >= m.alarm) return "alarm";
    if (value >= m.warn) return "warning";
    return "healthy";
}

export function signalColor(key: SignalKey, value: number): string {
    return STATUS_COLORS[signalState(key, value)];
}

// Overall plant status = worst band across the health-governing signals only.
export function derivePlantStatus(moduleTempC: number, inverterLoadPct: number): PlantStatus {
    let worst: PlantStatus = "healthy";
    for (const key of SIGNAL_ORDER) {
        if (!SIGNAL_METADATA[key].governsHealth) {
            continue;
        }
        const value = key === "moduleTemp" ? moduleTempC : inverterLoadPct;
        const s = signalState(key, value);
        if (STATUS_RANK[s] > STATUS_RANK[worst]) {
            worst = s;
        }
    }
    return worst;
}

// Flatten the signal metadata into ontology-store rows (the single source the app
// publishes to the Fabric backend and renders in the thresholds reference card).
export function thresholdRows(): SignalThresholdRecord[] {
    return SIGNAL_ORDER.map((key) => {
        const m = SIGNAL_METADATA[key];
        const bands = activeBands(key);
        return {
            signalKey: m.key,
            ontologyProperty: m.ontologyProperty,
            unit: m.unit,
            warn: bands.warn,
            alarm: bands.alarm,
            governsHealth: m.governsHealth,
        };
    });
}

// Human-readable band summary for a threshold row (informational when unbanded).
export function formatBand(row: { warn: number; alarm: number; unit: string }): string {
    if (!Number.isFinite(row.alarm)) {
        return "informational";
    }
    return `warn \u2265 ${row.warn}${row.unit} \u00b7 alarm \u2265 ${row.alarm}${row.unit}`;
}

export interface SiteSummary {
    id: string;
    name: string;
    unitCount: number;
    totalKbd: number;
    ratedKbd: number;
    utilization: number;
    alarms: number;
    warnings: number;
    healthy: number;
    avgFeedKbd: number;
    avgUnitTempC: number;
    avgUtilizationPct: number;
}

// Pure per-site aggregation for the site drill-down view: rolls each refinery's
// process units into scoped KPIs (throughput, utilization, health counts, averages).
// Sites with no units report zeros so the UI can render them safely.
export function summarizeSites(
    turbines: { siteId: string; status: PlantStatus; powerKw: number; irradianceWm2: number; moduleTempC: number; inverterLoadPct: number }[],
    sites: { id: string; name: string; capacityMw: number }[],
): SiteSummary[] {
    return sites.map((site) => {
        const local = turbines.filter((t) => t.siteId === site.id);
        const n = local.length;
        const totalKbd = local.reduce((sum, t) => sum + t.powerKw, 0);
        const alarms = local.filter((t) => t.status === "alarm").length;
        const warnings = local.filter((t) => t.status === "warning").length;
        const ratedKbd = site.capacityMw * n;
        const avg = (sel: (t: (typeof local)[number]) => number) => (n > 0 ? local.reduce((sum, t) => sum + sel(t), 0) / n : 0);
        return {
            id: site.id,
            name: site.name,
            unitCount: n,
            totalKbd,
            ratedKbd,
            utilization: ratedKbd > 0 ? (totalKbd / ratedKbd) * 100 : 0,
            alarms,
            warnings,
            healthy: n - alarms - warnings,
            avgFeedKbd: +avg((t) => t.irradianceWm2).toFixed(0),
            avgUnitTempC: +avg((t) => t.moduleTempC).toFixed(1),
            avgUtilizationPct: +avg((t) => t.inverterLoadPct).toFixed(0),
        };
    });
}

// Anchor points (in twin-scene world space) for the part value callouts.
export const TWIN_PARTS: { key: string; caption: string; pos: [number, number, number] }[] = [
    { key: "array", caption: "Column", pos: [0, 4.0, -1.4] },
    { key: "inverter", caption: "Tank", pos: [-3.6, 1.5, 0] },
    { key: "tracker", caption: "Flare", pos: [2.8, 0.9, 1.4] },
    { key: "output", caption: "Throughput", pos: [0, 0.7, 2.8] },
];

// ---------------------------------------------------------------------------
// Twin device graph: an editable component → device tree backing the digital
// twin admin. Refinery adaptation of the wind twin-device model; the process
// asset (column / tank / flare / throughput) fans out into child devices whose
// layout + metadata persist to the ontology `UnitDevice` store (fallback-safe).
// ---------------------------------------------------------------------------

export type RefineryPartKey = "array" | "inverter" | "tracker" | "output";

export type RefineryDeviceNode = {
    key: string;
    component: RefineryPartKey;
    label: string;
    property: string;
    unit: string;
    note: string;
    anchor: [number, number, number];
    lookAt: [number, number, number];
    offset: [number, number, number];
    zoom: number;
    value: (t: PlantTelemetry) => string;
    status: (t: PlantTelemetry) => PlantStatus;
};

type RefineryDeviceDraft = {
    label: string;
    property: string;
    unit: string;
    note: string;
    zoom: string;
    sortOrder: string;
    anchor: string;
    lookAt: string;
    offset: string;
};

export const UNIT_COMPONENT_DEVICES: Record<RefineryPartKey, RefineryDeviceNode[]> = {
    array: [
        {
            key: "array.feed-pump", component: "array", label: "Feed pump", property: "FeedPumpVibrationMmS", unit: "mm/s",
            note: "Charge pump feeding the distillation column.",
            anchor: [0, 4.0, -1.4], lookAt: [0, 4.0, -1.4], offset: [6.5, 1.6, 4.4], zoom: 0.55,
            value: (t) => `${(t.inverterLoadPct * 0.06).toFixed(1)}`, status: (t) => signalState("inverterLoad", t.inverterLoadPct),
        },
        {
            key: "array.reboiler", component: "array", label: "Reboiler", property: "ReboilerTempC", unit: "°C",
            note: "Bottoms reboiler driving column separation.",
            anchor: [0.6, 3.4, -1.0], lookAt: [0.6, 3.4, -1.0], offset: [6.0, 1.8, 5.3], zoom: 0.6,
            value: (t) => `${t.moduleTempC.toFixed(1)}`, status: (t) => signalState("moduleTemp", t.moduleTempC),
        },
    ],
    inverter: [
        {
            key: "inverter.level", component: "inverter", label: "Tank level", property: "TankLevelPct", unit: "%",
            note: "Intermediate storage tank level.",
            anchor: [-3.6, 1.5, 0], lookAt: [-3.6, 1.5, 0], offset: [5.0, 1.8, 5.7], zoom: 0.62,
            value: (t) => `${Math.min(100, Math.round(t.inverterLoadPct))}`, status: (t) => signalState("inverterLoad", t.inverterLoadPct),
        },
        {
            key: "inverter.transfer-pump", component: "inverter", label: "Transfer pump", property: "TransferPumpLoadPct", unit: "%",
            note: "Rundown pump moving product to storage.",
            anchor: [-3.0, 1.2, 0.4], lookAt: [-3.0, 1.2, 0.4], offset: [4.7, 2.1, 6.0], zoom: 0.66,
            value: (t) => `${Math.min(100, Math.round((t.powerKw / 260) * 100))}`, status: (t) => signalState("power", t.powerKw),
        },
    ],
    tracker: [
        {
            key: "tracker.pilot", component: "tracker", label: "Flare pilot", property: "FlarePilotTempC", unit: "°C",
            note: "Flare pilot flame temperature.",
            anchor: [2.8, 0.9, 1.4], lookAt: [2.8, 0.9, 1.4], offset: [6.1, 1.7, 6.5], zoom: 0.7,
            value: (t) => `${(t.moduleTempC + 12).toFixed(0)}`, status: (t) => signalState("moduleTemp", t.moduleTempC),
        },
        {
            key: "tracker.ko-drum", component: "tracker", label: "Knock-out drum", property: "KODrumLevelPct", unit: "%",
            note: "Flare knock-out drum liquid level.",
            anchor: [2.4, 0.7, 1.0], lookAt: [2.4, 0.7, 1.0], offset: [6.4, 1.4, 6.8], zoom: 0.74,
            value: (t) => `${Math.max(0, 100 - Math.round(t.inverterLoadPct))}`, status: (t) => signalState("inverterLoad", t.inverterLoadPct),
        },
    ],
    output: [
        {
            key: "output.metering", component: "output", label: "Custody metering", property: "MeteringKBD", unit: "kbd",
            note: "Fiscal throughput metering skid.",
            anchor: [0, 0.7, 2.8], lookAt: [0, 0.7, 2.8], offset: [6.9, 2.1, 7.2], zoom: 0.8,
            value: (t) => `${t.powerKw.toLocaleString()}`, status: (t) => signalState("power", t.powerKw),
        },
        {
            key: "output.control-valve", component: "output", label: "Control valve", property: "ValvePositionPct", unit: "%",
            note: "Product rundown control valve position.",
            anchor: [0.4, 0.6, 2.4], lookAt: [0.4, 0.6, 2.4], offset: [7.3, 2.5, 7.8], zoom: 0.86,
            value: (t) => `${Math.min(100, Math.round(t.inverterLoadPct))}`, status: (t) => signalState("inverterLoad", t.inverterLoadPct),
        },
    ],
};

// Flatten the component→device graph into ordered ontology rows for idempotent seeding.
export function unitDeviceSeeds(graph: Record<RefineryPartKey, RefineryDeviceNode[]>): UnitDeviceRecord[] {
    let order = 0;
    return (Object.keys(graph) as RefineryPartKey[]).flatMap((component) =>
        graph[component].map((d) => ({
            deviceKey: d.key,
            component: d.component,
            label: d.label,
            property: d.property,
            unit: d.unit,
            note: d.note,
            anchorX: d.anchor[0], anchorY: d.anchor[1], anchorZ: d.anchor[2],
            lookAtX: d.lookAt[0], lookAtY: d.lookAt[1], lookAtZ: d.lookAt[2],
            offsetX: d.offset[0], offsetY: d.offset[1], offsetZ: d.offset[2],
            zoom: d.zoom,
            sortOrder: order++,
        })),
    );
}

// Merge persisted ontology rows over the bundled fallback graph: rows drive the
// editable metadata/layout; components with no rows fall back to the bundle so the
// twin always has a complete device tree.
export function mergeUnitDeviceGraph(
    rows: UnitDeviceRecord[],
    fallback: Record<RefineryPartKey, RefineryDeviceNode[]>,
): Record<RefineryPartKey, RefineryDeviceNode[]> {
    const fallbackByKey = new Map<string, RefineryDeviceNode>(
        (Object.keys(fallback) as RefineryPartKey[]).flatMap((component) =>
            fallback[component].map((d) => [d.key, d] as const),
        ),
    );
    const next: Record<RefineryPartKey, RefineryDeviceNode[]> = { array: [], inverter: [], tracker: [], output: [] };
    rows
        .slice()
        .sort((a, b) => a.sortOrder - b.sortOrder)
        .forEach((r) => {
            const component = r.component as RefineryPartKey;
            if (!(component in next)) {
                return;
            }
            const fallbackNode = fallbackByKey.get(r.deviceKey);
            if (!fallbackNode) {
                return;
            }
            next[component].push({
                ...fallbackNode,
                key: r.deviceKey,
                component,
                label: r.label,
                property: r.property,
                unit: r.unit,
                note: r.note,
                anchor: [r.anchorX, r.anchorY, r.anchorZ],
                lookAt: [r.lookAtX, r.lookAtY, r.lookAtZ],
                offset: [r.offsetX, r.offsetY, r.offsetZ],
                zoom: r.zoom,
            });
        });
    (Object.keys(next) as RefineryPartKey[]).forEach((component) => {
        if (next[component].length === 0) {
            next[component] = fallback[component];
        }
    });
    return next;
}

export function formatVec(v: [number, number, number]): string {
    return v.map((n) => Number(n.toFixed(3))).join(", ");
}

function parseVec(input: string, fallback: [number, number, number]): [number, number, number] {
    const parts = input.split(",").map((s) => Number(s.trim())).filter((n) => Number.isFinite(n));
    if (parts.length !== 3) {
        return fallback;
    }
    return [parts[0], parts[1], parts[2]];
}

function draftFromUnitDevice(device: RefineryDeviceNode, sortOrder: number): RefineryDeviceDraft {
    return {
        label: device.label,
        property: device.property,
        unit: device.unit,
        note: device.note,
        zoom: String(device.zoom),
        sortOrder: String(sortOrder),
        anchor: formatVec(device.anchor),
        lookAt: formatVec(device.lookAt),
        offset: formatVec(device.offset),
    };
}

export function normalizeUnitDeviceDraft(
    draft: RefineryDeviceDraft,
    base: { zoom: number; sortOrder: number; anchor: [number, number, number]; lookAt: [number, number, number]; offset: [number, number, number] },
): { label: string; property: string; unit: string; note: string; zoom: number; sortOrder: number; anchor: [number, number, number]; lookAt: [number, number, number]; offset: [number, number, number] } {
    const zoom = Number(draft.zoom);
    const sortOrder = Number(draft.sortOrder);
    return {
        label: draft.label.trim() || "Device",
        property: draft.property.trim() || "Property",
        unit: draft.unit.trim() || "",
        note: draft.note.trim() || "No note provided.",
        zoom: Number.isFinite(zoom) ? Math.max(0.16, Math.min(2, zoom)) : base.zoom,
        sortOrder: Number.isFinite(sortOrder) ? Math.max(0, Math.round(sortOrder)) : base.sortOrder,
        anchor: parseVec(draft.anchor, base.anchor),
        lookAt: parseVec(draft.lookAt, base.lookAt),
        offset: parseVec(draft.offset, base.offset),
    };
}

export function createMapTexture(sites: SolarPlantSite[]) {
    const canvas = document.createElement("canvas");
    canvas.width = 2048;
    canvas.height = 1024;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
        return new THREE.CanvasTexture(canvas);
    }

    const w = canvas.width;
    const h = canvas.height;
    // Map texture spans the 92 x 52 world plane; project lon/lat with the worldwide
    // equirectangular projection so each refinery lands on its real position.
    const toPx = (lon: number, lat: number): [number, number] => {
        const u = (projectLonToX(lon) + 46) / 92;
        const v = (projectLatToZ(lat) + 23) / 46;
        return [u * w, v * h];
    };

    // Transparent overlay: the lit ocean plane shows through; we only paint land.
    ctx.strokeStyle = "rgba(150, 205, 255, 0.10)";
    ctx.lineWidth = 1;
    for (let lon = -150; lon <= 150; lon += 30) {
        const [x] = toPx(lon, 80);
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
        ctx.stroke();
    }
    for (let lat = -60; lat <= 80; lat += 30) {
        const [, y] = toPx(0, lat);
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(w, y);
        ctx.stroke();
    }

    const tracePoly = (poly: number[][]) => {
        ctx.beginPath();
        const [sx, sy] = toPx(poly[0][0], poly[0][1]);
        ctx.moveTo(sx, sy);
        for (let i = 1; i < poly.length; i += 1) {
            const [px, py] = toPx(poly[i][0], poly[i][1]);
            ctx.lineTo(px, py);
        }
        ctx.closePath();
    };

    ctx.lineJoin = "round";
    ctx.lineCap = "round";

    // Pass 1 - soft shallow-water halo hugging every coastline.
    ctx.save();
    ctx.shadowColor = "rgba(96, 214, 192, 0.7)";
    ctx.shadowBlur = 16;
    ctx.fillStyle = "rgba(58, 120, 86, 0.9)";
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.fill();
    });
    ctx.restore();

    // Pass 2 - landmass with a north-lit relief gradient.
    const landGrad = ctx.createLinearGradient(0, 0, 0, h);
    landGrad.addColorStop(0, "#6cae7f");
    landGrad.addColorStop(0.45, "#4f9268");
    landGrad.addColorStop(0.75, "#3f8159");
    landGrad.addColorStop(1, "#356f4d");
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.fillStyle = landGrad;
        ctx.fill();
    });

    // Pass 3 - inner coast shading for depth (clipped to each landmass).
    WORLD.forEach((poly) => {
        ctx.save();
        tracePoly(poly);
        ctx.clip();
        tracePoly(poly);
        ctx.strokeStyle = "rgba(10, 40, 30, 0.5)";
        ctx.lineWidth = 7;
        ctx.stroke();
        ctx.restore();
    });

    // Pass 4 - crisp luminous coastline.
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.strokeStyle = "rgba(208, 255, 232, 0.9)";
        ctx.lineWidth = 2;
        ctx.stroke();
    });

    // Refinery site pins: glow ring + colored disc + bright core + label.
    ctx.font = "600 22px 'Segoe UI', system-ui, sans-serif";
    sites.forEach((site, idx) => {
        const [x, y] = toPx(site.lon, site.lat);
        const color = SITE_COLORS[idx % SITE_COLORS.length];

        ctx.strokeStyle = color;
        ctx.globalAlpha = 0.7;
        ctx.lineWidth = 2.5;
        ctx.beginPath();
        ctx.arc(x, y, 22, 0, Math.PI * 2);
        ctx.stroke();
        ctx.globalAlpha = 1;

        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(x, y, 9, 0, Math.PI * 2);
        ctx.fill();

        ctx.fillStyle = "#ffffff";
        ctx.beginPath();
        ctx.arc(x, y, 3.4, 0, Math.PI * 2);
        ctx.fill();

        ctx.save();
        ctx.shadowColor = "rgba(0, 0, 0, 0.85)";
        ctx.shadowBlur = 6;
        ctx.fillStyle = "#eefcff";
        ctx.fillText(site.name, x + 16, y - 12);
        ctx.restore();
    });

    const texture = new THREE.CanvasTexture(canvas);
    texture.anisotropy = 4;
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.needsUpdate = true;
    return texture;
}

// Vertical sky gradient backdrop (deep space -> teal horizon) with faint stars.
export function createSkyTexture() {
    const canvas = document.createElement("canvas");
    canvas.width = 32;
    canvas.height = 512;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
        return new THREE.CanvasTexture(canvas);
    }

    const grad = ctx.createLinearGradient(0, 0, 0, canvas.height);
    grad.addColorStop(0, "#02060f");
    grad.addColorStop(0.42, "#06182b");
    grad.addColorStop(0.74, "#0b3149");
    grad.addColorStop(1, "#14536f");
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    for (let i = 0; i < 70; i += 1) {
        const x = seededRand(i * 2 + 1) * canvas.width;
        const y = seededRand(i * 2 + 2) * canvas.height * 0.45;
        ctx.globalAlpha = 0.18 + seededRand(i + 5) * 0.5;
        ctx.fillStyle = "#cfe4ff";
        ctx.fillRect(x, y, 1, 1);
    }
    ctx.globalAlpha = 1;

    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.needsUpdate = true;
    return texture;
}

// Lit-ocean texture: depth gradient + soft sun glint + drifting ripple bands.
export function createOceanTexture() {
    const canvas = document.createElement("canvas");
    canvas.width = 1024;
    canvas.height = 1024;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
        return new THREE.CanvasTexture(canvas);
    }

    const w = canvas.width;
    const h = canvas.height;

    const base = ctx.createLinearGradient(0, 0, 0, h);
    base.addColorStop(0, "#1b5478");
    base.addColorStop(0.5, "#123f60");
    base.addColorStop(1, "#0b2a44");
    ctx.fillStyle = base;
    ctx.fillRect(0, 0, w, h);

    const glint = ctx.createRadialGradient(w * 0.66, h * 0.32, 20, w * 0.66, h * 0.32, w * 0.52);
    glint.addColorStop(0, "rgba(165, 226, 255, 0.34)");
    glint.addColorStop(0.4, "rgba(120, 190, 230, 0.12)");
    glint.addColorStop(1, "rgba(120, 190, 230, 0)");
    ctx.fillStyle = glint;
    ctx.fillRect(0, 0, w, h);

    ctx.strokeStyle = "rgba(170, 220, 255, 0.05)";
    ctx.lineWidth = 2;
    for (let i = 0; i < 130; i += 1) {
        const y = seededRand(i + 1) * h;
        const amp = 6 + seededRand(i + 2) * 14;
        ctx.beginPath();
        for (let x = 0; x <= w; x += 32) {
            const yy = y + Math.sin((x / w) * Math.PI * 6 + i) * amp;
            if (x === 0) {
                ctx.moveTo(x, yy);
            } else {
                ctx.lineTo(x, yy);
            }
        }
        ctx.stroke();
    }

    const texture = new THREE.CanvasTexture(canvas);
    texture.wrapS = THREE.RepeatWrapping;
    texture.wrapT = THREE.RepeatWrapping;
    texture.repeat.set(2, 1.4);
    texture.anisotropy = 4;
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.needsUpdate = true;
    return texture;
}

function buildFarm(seedOffset: number): PlantTelemetry[] {
    const rows: PlantTelemetry[] = [];

    SITES.forEach((site, siteIdx) => {
        const cx = projectLonToX(site.lon);
        const cz = projectLatToZ(site.lat);
        const cols = Math.max(1, Math.ceil(Math.sqrt(site.arrayCount)));
        const totalRows = Math.ceil(site.arrayCount / cols);
        const ratedPerUnitKbd = site.capacityMw / Math.max(site.arrayCount, 1);

        for (let i = 0; i < site.arrayCount; i += 1) {
            const idx = siteIdx * 100 + i;
            const col = i % cols;
            const row = Math.floor(i / cols);
            const x = cx + (col - (cols - 1) / 2) * 1.5;
            const z = cz + (row - (totalRows - 1) / 2) * 1.5;

            const id = `${site.id}-U-${String(i + 1).padStart(2, "0")}`;
            // inverterLoadPct carries unit utilization %; irradianceWm2 carries feed rate
            // (kbd); powerKw carries product throughput (kbd); moduleTempC carries unit °C.
            const inverterLoadPct = Math.min(100, 58 + seededRand(idx + 15 + seedOffset) * 44);
            const irradianceWm2 = ratedPerUnitKbd * (inverterLoadPct / 100);
            const yieldFactor = 0.82 + seededRand(idx + 7 + seedOffset) * 0.14;
            const powerKw = Math.round(irradianceWm2 * yieldFactor);
            const moduleTempC = 320 + (inverterLoadPct / 100) * 130 + seededRand(idx + 11 + seedOffset) * 40;

            const status: PlantStatus = derivePlantStatus(moduleTempC, inverterLoadPct);

            rows.push({
                id,
                siteId: site.id,
                siteName: site.name,
                latitude: site.lat + (row - (totalRows - 1) / 2) * 0.012,
                longitude: site.lon + (col - (cols - 1) / 2) * 0.014,
                x,
                z,
                powerKw,
                irradianceWm2: +irradianceWm2.toFixed(0),
                moduleTempC: +moduleTempC.toFixed(1),
                inverterLoadPct: +inverterLoadPct.toFixed(0),
                status,
            });
        }
    });

    return rows;
}

// Live refresh cadence for the real semantic-model feed (kept close to the
// synthetic seed cadence so the two paths feel consistent to the operator).
const LIVE_TELEMETRY_REFRESH_MS = 5000;

// Assemble live semantic-model rows into the same PlantTelemetry shape the scene
// renders, reusing the ontology refinery coordinates and the shared status bands so
// real data flows through the identical rendering path as buildFarm().
function assembleLiveView(snapshot: LiveTelemetrySnapshot): { turbines: PlantTelemetry[]; sites: SolarPlantSite[] } {
    const metricsById = new Map(snapshot.metrics.map((m) => [m.unitId, m]));
    const refineryById = new Map(snapshot.refineries.map((r) => [r.refineryId, r]));
    const unitsByRefinery = new Map<string, string[]>();
    for (const u of snapshot.units) {
        const list = unitsByRefinery.get(u.refineryId) ?? [];
        list.push(u.unitId);
        unitsByRefinery.set(u.refineryId, list);
    }

    const plants: PlantTelemetry[] = [];
    for (const refinery of snapshot.refineries) {
        const ids = unitsByRefinery.get(refinery.refineryId) ?? [];
        const cx = projectLonToX(refinery.longitude);
        const cz = projectLatToZ(refinery.latitude);
        const cols = Math.max(1, Math.ceil(Math.sqrt(ids.length)));
        const totalRows = Math.max(1, Math.ceil(ids.length / cols));
        ids.forEach((unitId, i) => {
            const col = i % cols;
            const row = Math.floor(i / cols);
            const x = cx + (col - (cols - 1) / 2) * 1.5;
            const z = cz + (row - (totalRows - 1) / 2) * 1.5;
            const m = metricsById.get(unitId);
            const moduleTempC = +(m?.moduleTempC ?? 0).toFixed(1);
            const inverterLoadPct = +(m?.inverterLoadPct ?? 0).toFixed(0);
            plants.push({
                id: unitId,
                siteId: refinery.refineryId,
                siteName: refinery.refineryName,
                latitude: refinery.latitude + (row - (totalRows - 1) / 2) * 0.012,
                longitude: refinery.longitude + (col - (cols - 1) / 2) * 0.014,
                x,
                z,
                powerKw: Math.round(m?.powerKw ?? 0),
                irradianceWm2: +(m?.irradianceWm2 ?? 0).toFixed(0),
                moduleTempC,
                inverterLoadPct,
                status: derivePlantStatus(moduleTempC, inverterLoadPct),
            });
        });
    }

    const sites: SolarPlantSite[] = snapshot.refineries.map((refinery) => ({
        id: refinery.refineryId,
        name: refinery.refineryName,
        region: "",
        lat: refinery.latitude,
        lon: refinery.longitude,
        arrayCount: (unitsByRefinery.get(refinery.refineryId) ?? []).length,
        capacityMw: refinery.capacityKbd,
    }));

    // Ignore units whose refinery has no coordinates so they cannot collapse to origin.
    return {
        turbines: plants.filter((p) => refineryById.has(p.siteId)),
        sites,
    };
}

// React hook: when a live semantic-model connection is configured, poll it and
// return real units + sites; otherwise return null so callers fall back to the
// synthetic feed. Any query failure also yields null (fail-safe).
function useLiveTelemetry(): { turbines: PlantTelemetry[]; sites: SolarPlantSite[] } | null {
    const [snapshot, setSnapshot] = useState<{ turbines: PlantTelemetry[]; sites: SolarPlantSite[] } | null>(null);

    useEffect(() => {
        if (!isLiveTelemetryConfigured()) {
            return;
        }
        let cancelled = false;
        const load = async () => {
            const data = await fetchLiveTelemetry();
            if (cancelled || !data || data.units.length === 0) {
                return;
            }
            const view = assembleLiveView(data);
            if (!cancelled && view.turbines.length > 0) {
                setSnapshot(view);
            }
        };
        void load();
        const id = window.setInterval(load, LIVE_TELEMETRY_REFRESH_MS);
        return () => {
            cancelled = true;
            window.clearInterval(id);
        };
    }, []);

    return snapshot;
}

async function askFabricIQ(question: string, context: Record<string, unknown>): Promise<AskResult> {
    const telemetry = (context.telemetry as PlantTelemetry[] | undefined) ?? [];
    const intent = classifyAskIntent(question);
    let dataAgentFailed = false;

    // Fast operational intents are answered deterministically to minimize latency,
    // avoid unnecessary remote calls, and keep triage behavior stable.
    if (intent === "ops-fastpath") {
        return {
            source: "local",
            summary: answerQuestion(question, telemetry),
            generatedAt: new Date().toISOString(),
            queryText: "Deterministic local rule engine",
            confidence: 0.98,
            fallbackReason: "ops-fastpath",
            evidence: [`telemetry rows: ${telemetry.length}`, "routing: ops-fastpath"],
        };
    }

    // Route analytics-heavy prompts to the live Data Agent whenever configured.
    if (isDataAgentConfigured()) {
        try {
            const agent = await queryDataAgent(question, context);
            return {
                source: "fabriciq",
                summary: agent.summary,
                generatedAt: new Date().toISOString(),
                queryText: agent.queryText,
                confidence: agent.confidence,
                evidence: agent.evidence,
                transport: agent.transport,
            };
        } catch {
            dataAgentFailed = true;
        }
    }

    try {
        const { summary, queryText } = await askOntology(question, context);
        return {
            source: "ontology",
            summary,
            generatedAt: new Date().toISOString(),
            queryText,
            confidence: 0.85,
            fallbackReason: dataAgentFailed ? "data-agent-failed" : undefined,
            evidence: dataAgentFailed ? ["routing: ontology-grounded", "fallback: data-agent failed"] : ["routing: ontology-grounded"],
        };
    } catch {
        return {
            source: "local",
            summary: `${answerQuestion(question, telemetry)} (offline — ontology backend unreachable)`,
            generatedAt: new Date().toISOString(),
            queryText: "Deterministic local fallback",
            confidence: 0.72,
            fallbackReason: dataAgentFailed ? "data-agent-and-ontology-failed" : "ontology-backend-unreachable",
            evidence: dataAgentFailed
                ? ["data-agent failed", "ontology backend unreachable", "routing: local fallback"]
                : ["ontology backend unreachable", "routing: local fallback"],
        };
    }
}

// Human-readable provenance for an answer's backing engine.
export function sourceLabel(source: AskResult["source"]): string {
    switch (source) {
        case "fabriciq":
            return "Fabric Data Agent (live)";
        case "ontology":
            return "Ontology-grounded engine";
        default:
            return "Local engine (offline)";
    }
}

const LazySolarFleetScene = lazyRetry(() => import("@/scenes/GlobeFleetScene"));
const LazyPlantTwinScene = lazyRetry(() => import("@/scenes/PlantTwinScene"));

type StatusFilter = PlantStatus | "all";

function Sparkline({ values, color = "#6ee7ff", forecast }: { values: number[]; color?: string; forecast?: { value: number; lo: number; hi: number } }) {
    if (values.length < 2) {
        return <div className="flex h-8 items-center text-xs text-slate-500">Collecting trend…</div>;
    }
    const domain = forecast ? [...values, forecast.value, forecast.lo, forecast.hi] : values;
    const min = Math.min(...domain);
    const max = Math.max(...domain);
    const span = max - min || 1;
    const yOf = (v: number) => 28 - ((v - min) / span) * 26;
    const xMax = forecast ? 78 : 100;
    const points = values
        .map((v, i) => `${(i / (values.length - 1)) * xMax},${yOf(v)}`)
        .join(" ");
    const lastY = yOf(values[values.length - 1]);
    const fx = 100;
    return (
        <svg viewBox="0 0 100 28" preserveAspectRatio="none" className="h-8 w-full">
            {forecast && (
                <>
                    <rect x={xMax} y={Math.min(yOf(forecast.hi), yOf(forecast.lo))} width={fx - xMax} height={Math.max(0.5, Math.abs(yOf(forecast.lo) - yOf(forecast.hi)))} fill={color} opacity={0.16} />
                    <line x1={xMax} y1={lastY} x2={fx} y2={yOf(forecast.value)} stroke={color} strokeWidth={1.2} strokeDasharray="3 2" vectorEffect="non-scaling-stroke" />
                    <circle cx={fx} cy={yOf(forecast.value)} r={1.6} fill={color} />
                </>
            )}
            <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
        </svg>
    );
}

export function forecastDetail(history: number[], horizon: number): { value: number; lo: number; hi: number; confidence: number } {
    const last = history[history.length - 1] ?? 0;
    const n = history.length;
    if (n < 3) {
        const v = Math.round(last);
        return { value: v, lo: v, hi: v, confidence: 0 };
    }
    const meanX = (n - 1) / 2;
    const meanY = history.reduce((a, b) => a + b, 0) / n;
    let num = 0;
    let den = 0;
    for (let i = 0; i < n; i += 1) {
        num += (i - meanX) * (history[i] - meanY);
        den += (i - meanX) ** 2;
    }
    const slope = den === 0 ? 0 : num / den;
    const intercept = meanY - slope * meanX;
    let ssRes = 0;
    let ssTot = 0;
    for (let i = 0; i < n; i += 1) {
        const pred = intercept + slope * i;
        ssRes += (history[i] - pred) ** 2;
        ssTot += (history[i] - meanY) ** 2;
    }
    const rmse = Math.sqrt(ssRes / n);
    const r2 = ssTot === 0 ? 1 : Math.max(0, 1 - ssRes / ssTot);
    const value = Math.max(0, Math.round(intercept + slope * (n - 1 + horizon)));
    const band = Math.round(rmse * (1 + horizon / 6) * 1.96);
    return { value, lo: Math.max(0, value - band), hi: value + band, confidence: Math.round(r2 * 100) };
}

// Predictive anomaly score (0-1) from unit temperature and utilization approaching limits.
export function anomalyScore(t: PlantTelemetry): number {
    const tempScore = (t.moduleTempC - 360) / 70;
    const loadScore = (t.inverterLoadPct - 80) / 18;
    return Math.min(1, Math.max(0, tempScore, loadScore));
}

export interface EscalationForecast {
    direction: "rising" | "stable" | "falling";
    slopePerTick: number;
    etaToAlarmTicks: number | null;
}

// Trend-based escalation forecast over a rolling window of anomaly scores (0..1).
// Fits a least-squares slope; a rising trend projects ticks-to-alarm (score -> 1).
// Fewer than 3 samples is treated as stable so early frames never mislead.
export function forecastEscalation(scores: number[]): EscalationForecast {
    const n = scores.length;
    if (n < 3) {
        return { direction: "stable", slopePerTick: 0, etaToAlarmTicks: null };
    }
    const meanX = (n - 1) / 2;
    const meanY = scores.reduce((a, b) => a + b, 0) / n;
    let num = 0;
    let den = 0;
    for (let i = 0; i < n; i += 1) {
        num += (i - meanX) * (scores[i] - meanY);
        den += (i - meanX) ** 2;
    }
    const slope = den === 0 ? 0 : num / den;
    const current = scores[n - 1];
    const direction = slope > 0.01 ? "rising" : slope < -0.01 ? "falling" : "stable";
    const etaToAlarmTicks = slope > 0.001 && current < 1 ? Math.ceil((1 - current) / slope) : null;
    return { direction, slopePerTick: +slope.toFixed(3), etaToAlarmTicks };
}

export interface ScenarioInput {
    baselineKw: number;
    curtailmentPct: number;
    downtimeTicks: number;
    horizonTicks: number;
}

export interface ScenarioResult {
    projectedKw: number;
    runningTicks: number;
    energyBaselineKwt: number;
    energyScenarioKwt: number;
    energyDeltaKwt: number;
}

// Pure what-if model: apply a curtailment (% throughput reduction while running)
// and a maintenance downtime (ticks offline) to a baseline throughput over a
// horizon, reporting projected throughput plus the volume delta (throughput x ticks).
export function simulateScenario(input: ScenarioInput): ScenarioResult {
    const baseline = Math.max(0, input.baselineKw);
    const curtail = Math.min(100, Math.max(0, input.curtailmentPct));
    const horizon = Math.max(0, Math.round(input.horizonTicks));
    const downtime = Math.min(horizon, Math.max(0, Math.round(input.downtimeTicks)));
    const projectedKw = Math.round(baseline * (1 - curtail / 100));
    const runningTicks = horizon - downtime;
    const energyBaselineKwt = baseline * horizon;
    const energyScenarioKwt = projectedKw * runningTicks;
    return {
        projectedKw,
        runningTicks,
        energyBaselineKwt,
        energyScenarioKwt,
        energyDeltaKwt: energyScenarioKwt - energyBaselineKwt,
    };
}

// Ids of units that transitioned into "alarm" since the previous status snapshot.
export function newlyAlarmed(prev: Record<string, PlantStatus>, turbines: { id: string; status: PlantStatus }[]): string[] {
    return turbines.filter((t) => t.status === "alarm" && prev[t.id] !== "alarm").map((t) => t.id);
}

// ---------------------------------------------------------------------------
// Mission-ops: work-order dispatch, responder ranking, escalation & SLA.
// Refinery adaptation of the wind mission-ops model — the two diagnosable
// refinery assets are the Pump (mechanical / high utilization) and the Heat
// Exchanger (thermal / high unit temperature).
// ---------------------------------------------------------------------------

export type WorkOrderPriority = "P1" | "P2" | "P3";
export type WorkOrderComponent = "Pump" | "Heat Exchanger";

type MockWorkIQResponder = {
    id: string;
    name: string;
    role: string;
    skills: WorkOrderComponent[];
    confidenceBySkill: Record<WorkOrderComponent, number>;
    siteCoverage: string[];
    shift: "day" | "swing" | "night";
    onCall: boolean;
    etaMin: number;
    photo: string;
};

type RankedResponder = MockWorkIQResponder & {
    score: number;
    reason: string;
    skillConfidence: number;
    currentLoad: number;
};

type MockRefineryEvidence = {
    id: string;
    label: string;
    component: WorkOrderComponent;
    image: string;
    capturedAt: string;
};

type DemoScriptStep = {
    id: DemoScriptStepId;
    label: string;
    detail: string;
    action: () => void | Promise<void>;
};

function svgAvatar(name: string, seed: number, role: string, shift: "day" | "swing" | "night", onCall: boolean): string {
    const profile = role.toLowerCase();
    const isField = /field|maintainer|reliability|rotating/.test(profile);
    const isOps = /operations|lead|process/.test(profile);
    const skins = [
        { base: "#f2c9a0", shade: "#d9a877", blush: "#e19a83" },
        { base: "#e2ac7e", shade: "#c2894f", blush: "#d1876a" },
        { base: "#c68a5c", shade: "#a3673c", blush: "#b56a4a" },
        { base: "#8d5a3c", shade: "#6d4026", blush: "#7c4632" },
        { base: "#f7d9bd", shade: "#e0b78d", blush: "#eaa98f" },
    ];
    const skin = skins[seed % skins.length];
    const hair = ["#241812", "#432a19", "#6b4423", "#8a6a3a", "#12100f", "#8a8f98"][seed % 6];
    const eyeColor = ["#5b3a29", "#3b2a1a", "#2e5d5a", "#3f5f86", "#3a3a3a"][seed % 5];
    const jacket = isField ? "#0f766e" : isOps ? "#1d4ed8" : ["#7c3aed", "#9a3412", "#0f172a"][seed % 3];
    const accent = onCall ? "#34d399" : ["#60a5fa", "#f59e0b", "#f472b6", "#a3e635"][seed % 4];
    const bg = shift === "night" ? "#0b1120" : shift === "swing" ? "#14203a" : "#16273f";
    const bg2 = shift === "night" ? "#060a14" : shift === "swing" ? "#0b1424" : "#0d1a2c";
    const helmet = isField
        ? `<path d='M46 50c1-19 15-30 34-30s33 11 34 30c-6-4-14-6-24-7l-2-9c-8-2-16-2-24 0l-1 9c-8 1-13 3-17 7Z' fill='#f6b73c'/><rect x='42' y='49' width='76' height='7' rx='3.5' fill='#e08a1e'/>`
        : isOps
        ? `<path d='M50 66c0-18 13-30 30-30s30 12 30 30' fill='none' stroke='#0f172a' stroke-width='6' stroke-linecap='round'/><rect x='44' y='64' width='12' height='20' rx='6' fill='#111827'/><rect x='104' y='64' width='12' height='20' rx='6' fill='#111827'/>`
        : "";
    const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 160 160'>
<defs>
    <linearGradient id='bgv${seed}' x1='0' y1='0' x2='0' y2='1'><stop offset='0%' stop-color='${bg}'/><stop offset='100%' stop-color='${bg2}'/></linearGradient>
    <radialGradient id='glow${seed}' cx='0.35' cy='0.28' r='0.7'><stop offset='0%' stop-color='${accent}' stop-opacity='0.28'/><stop offset='100%' stop-color='${accent}' stop-opacity='0'/></radialGradient>
    <linearGradient id='skin${seed}' x1='0.2' y1='0' x2='0.85' y2='1'><stop offset='0%' stop-color='${skin.base}'/><stop offset='100%' stop-color='${skin.shade}'/></linearGradient>
</defs>
<rect width='160' height='160' rx='18' fill='url(#bgv${seed})'/>
<rect width='160' height='160' rx='18' fill='url(#glow${seed})'/>
<path d='M34 152c3-24 20-38 46-38s43 14 46 38Z' fill='${jacket}'/>
<path d='M60 120c5 12 35 12 40 0l6 10-8 8H62l-8-8Z' fill='#f5c542'/>
<path d='M68 108h24v14c0 7-24 7-24 0Z' fill='${skin.shade}'/>
<path d='M48 78c0-24 14-42 32-42s32 18 32 42c0 12-3 22-7 30 2-14 1-30-6-38-6 8-32 8-38 0-7 8-8 24-6 38-4-8-7-18-7-30Z' fill='${hair}'/>
<ellipse cx='49' cy='84' rx='7' ry='10' fill='url(#skin${seed})'/><ellipse cx='111' cy='84' rx='7' ry='10' fill='url(#skin${seed})'/>
<ellipse cx='80' cy='80' rx='31' ry='35' fill='url(#skin${seed})'/>
<ellipse cx='66' cy='92' rx='6' ry='4' fill='${skin.blush}' opacity='0.45'/><ellipse cx='94' cy='92' rx='6' ry='4' fill='${skin.blush}' opacity='0.45'/>
<path d='M62 70c5-4 12-4 16-1' stroke='${hair}' stroke-width='3' fill='none' stroke-linecap='round'/><path d='M82 69c4-3 11-3 16 1' stroke='${hair}' stroke-width='3' fill='none' stroke-linecap='round'/>
<ellipse cx='70' cy='79' rx='8.5' ry='5.5' fill='#ffffff'/><ellipse cx='90' cy='79' rx='8.5' ry='5.5' fill='#ffffff'/>
<circle cx='71' cy='79' r='3.6' fill='${eyeColor}'/><circle cx='71' cy='79' r='1.7' fill='#111'/><circle cx='89' cy='79' r='3.6' fill='${eyeColor}'/><circle cx='89' cy='79' r='1.7' fill='#111'/>
<path d='M80 80c-1 6-3 10-5 12 2 2 6 2 9 0' fill='none' stroke='${skin.shade}' stroke-width='2.4' stroke-linecap='round' stroke-linejoin='round'/>
<path d='M70 100c6 5 14 5 20 0' stroke='#8a4a3a' stroke-width='3' fill='none' stroke-linecap='round'/>
${helmet}
<circle cx='128' cy='30' r='7' fill='${onCall ? "#34d399" : "rgba(255,255,255,0.22)"}' stroke='#0b1120' stroke-width='2'/>
</svg>`;
    return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}

function svgMockFieldCapture(label: string, seed: number, component: WorkOrderComponent): string {
    const isPump = component === "Pump";
    const subtitle = isPump ? "seal / vibration" : "tube / thermal";
    const palette = isPump
        ? { skyTop: "#cdeef0", skyBot: "#8fd0c9", sun: "#f4ffe9", hillFar: "#8fc99b", hillMid: "#5ea67f", hillNear: "#356b57", part: "#eafaf1", partLine: "#2c5a4a", cloud: "#f4fffb" }
        : { skyTop: "#ffd9a8", skyBot: "#f7a072", sun: "#fff3d6", hillFar: "#e08a5c", hillMid: "#c96f4a", hillNear: "#8f4a3a", part: "#fbe8c9", partLine: "#7a3b28", cloud: "#fff1e0" };
    const part = isPump
        ? `<g><ellipse cx='150' cy='98' rx='34' ry='14' fill='${palette.partLine}' opacity='0.25'/><circle cx='150' cy='76' r='22' fill='${palette.part}' stroke='${palette.partLine}' stroke-width='2.5'/><circle cx='150' cy='76' r='7' fill='${palette.partLine}'/><path d='M150 50v10M150 92v10M124 76h10M166 76h10' stroke='${palette.partLine}' stroke-width='2.6' stroke-linecap='round'/></g>`
        : `<g><ellipse cx='150' cy='96' rx='34' ry='16' fill='${palette.partLine}' opacity='0.25'/><rect x='120' y='60' width='60' height='32' rx='10' fill='${palette.part}' stroke='${palette.partLine}' stroke-width='2.5'/><path d='M126 68h48M126 76h48M126 84h48' stroke='${palette.partLine}' stroke-width='2.4' stroke-linecap='round'/></g>`;
    const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 220 132'>
<defs>
    <linearGradient id='sky${seed}' x1='0' y1='0' x2='0' y2='1'><stop offset='0%' stop-color='${palette.skyTop}'/><stop offset='100%' stop-color='${palette.skyBot}'/></linearGradient>
    <clipPath id='card${seed}'><rect width='220' height='132' rx='12'/></clipPath>
</defs>
<g clip-path='url(#card${seed})'>
    <rect width='220' height='132' fill='url(#sky${seed})'/>
    <circle cx='62' cy='42' r='20' fill='${palette.sun}' opacity='0.9'/>
    <ellipse cx='150' cy='30' rx='26' ry='11' fill='${palette.cloud}' opacity='0.85'/>
    <path d='M0 96c40-22 74-16 110 0s70 10 110-8v44H0Z' fill='${palette.hillFar}'/>
    <path d='M0 108c46-16 82-6 120 6s64 6 100-6v24H0Z' fill='${palette.hillMid}'/>
    <path d='M0 120c50-10 92 4 130 8s56-2 90-8v12H0Z' fill='${palette.hillNear}'/>
    ${part}
    <rect x='0' y='108' width='220' height='24' fill='${palette.hillNear}' opacity='0.55'/>
    <text x='12' y='124' font-size='10' fill='#ffffff' opacity='0.95' font-family='Segoe UI, Arial, sans-serif'>${label}</text>
    <text x='208' y='124' text-anchor='end' font-size='9' fill='#ffffff' opacity='0.8' font-family='Segoe UI, Arial, sans-serif'>${subtitle}</text>
</g>
</svg>`;
    return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}

const MOCK_WORKIQ_RESPONDERS: MockWorkIQResponder[] = [
    { id: "wk-001", name: "Aline Laurent", role: "Rotating Equipment Engineer", skills: ["Pump"], confidenceBySkill: { Pump: 0.92, "Heat Exchanger": 0.58 }, siteCoverage: ["PORTARTHUR", "PERNIS"], shift: "day", onCall: true, etaMin: 28, photo: svgAvatar("Aline Laurent", 1, "Rotating Equipment Engineer", "day", true) },
    { id: "wk-002", name: "Marc Delorme", role: "Heat Exchanger Specialist", skills: ["Heat Exchanger"], confidenceBySkill: { Pump: 0.42, "Heat Exchanger": 0.95 }, siteCoverage: ["RUWAIS", "PERNIS", "RASTANURA"], shift: "night", onCall: true, etaMin: 35, photo: svgAvatar("Marc Delorme", 2, "Heat Exchanger Specialist", "night", true) },
    { id: "wk-003", name: "Nora Haddad", role: "Process Reliability Lead", skills: ["Pump", "Heat Exchanger"], confidenceBySkill: { Pump: 0.83, "Heat Exchanger": 0.8 }, siteCoverage: ["JAMNAGAR", "RASTANURA", "JURONG"], shift: "swing", onCall: true, etaMin: 18, photo: svgAvatar("Nora Haddad", 3, "Process Reliability Lead", "swing", true) },
    { id: "wk-004", name: "Victor Klein", role: "Mechanical Maintainer", skills: ["Pump"], confidenceBySkill: { Pump: 0.86, "Heat Exchanger": 0.44 }, siteCoverage: ["RUWAIS", "PORTARTHUR"], shift: "day", onCall: false, etaMin: 41, photo: svgAvatar("Victor Klein", 4, "Mechanical Maintainer", "day", false) },
    { id: "wk-005", name: "Sofia Ribeiro", role: "Fired Heater Engineer", skills: ["Heat Exchanger"], confidenceBySkill: { Pump: 0.52, "Heat Exchanger": 0.9 }, siteCoverage: ["PARAGUANA", "PORTARTHUR", "JAMNAGAR"], shift: "night", onCall: true, etaMin: 32, photo: svgAvatar("Sofia Ribeiro", 5, "Fired Heater Engineer", "night", true) },
];

const MOCK_REFINERY_EVIDENCE: MockRefineryEvidence[] = [
    { id: "rf-ev-pump-01", label: "Pump seal leak trace", component: "Pump", image: svgMockFieldCapture("Pump seal leak trace", 1, "Pump"), capturedAt: "2026-07-14T07:21:00Z" },
    { id: "rf-ev-pump-02", label: "Vibration spectrum anomaly", component: "Pump", image: svgMockFieldCapture("Vibration spectrum anomaly", 2, "Pump"), capturedAt: "2026-07-14T07:24:00Z" },
    { id: "rf-ev-hx-01", label: "Exchanger thermal fouling", component: "Heat Exchanger", image: svgMockFieldCapture("Exchanger thermal fouling", 3, "Heat Exchanger"), capturedAt: "2026-07-14T07:26:00Z" },
    { id: "rf-ev-hx-02", label: "Tube bundle scaling", component: "Heat Exchanger", image: svgMockFieldCapture("Tube bundle scaling", 4, "Heat Exchanger"), capturedAt: "2026-07-14T07:29:00Z" },
];

export function currentShiftByHour(hour: number): "day" | "swing" | "night" {
    if (hour >= 7 && hour < 15) {
        return "day";
    }
    if (hour >= 15 && hour < 23) {
        return "swing";
    }
    return "night";
}

function rankWorkIQResponders(
    responders: MockWorkIQResponder[],
    siteId: string,
    component: WorkOrderComponent,
    priority: WorkOrderPriority,
    opts?: {
        shift?: "day" | "swing" | "night" | "all";
        onCallOnly?: boolean;
        loadByResponder?: Record<string, number>;
    },
): RankedResponder[] {
    const priorityBoost = priority === "P1" ? 20 : priority === "P2" ? 10 : 0;
    return responders
        .filter((r) => {
            if (opts?.onCallOnly && !r.onCall) {
                return false;
            }
            if (opts?.shift && opts.shift !== "all" && r.shift !== opts.shift) {
                return false;
            }
            return true;
        })
        .map((r) => {
            const skillMatch = r.skills.includes(component);
            const siteMatch = r.siteCoverage.includes(siteId);
            const speedScore = Math.max(0, 40 - r.etaMin);
            const skillConfidence = Math.round((r.confidenceBySkill[component] ?? 0.4) * 100);
            const load = opts?.loadByResponder?.[r.id] ?? 0;
            const loadPenalty = Math.min(24, load * 5);
            const score = (skillMatch ? 38 : 10) + (siteMatch ? 22 : 8) + speedScore + Math.round(skillConfidence * 0.16) + priorityBoost - loadPenalty;
            const reason = [
                skillMatch ? `${component} certified (${skillConfidence}%)` : `cross-trained on ${component} (${skillConfidence}%)`,
                siteMatch ? "site familiar" : "regional backup",
                load > 0 ? `${load} active task${load > 1 ? "s" : ""}` : "immediately available",
                `ETA ${r.etaMin} min`,
            ].join(" \u00b7 ");
            return { ...r, score, reason, skillConfidence, currentLoad: load };
        })
        .sort((a, b) => b.score - a.score) as RankedResponder[];
}

function prioritySlaMinutes(priority: WorkOrderPriority): number {
    if (priority === "P1") {
        return 30;
    }
    if (priority === "P2") {
        return 90;
    }
    return 240;
}

function elapsedMinutesSince(iso: string): number | null {
    const t = new Date(iso).getTime();
    if (!Number.isFinite(t)) {
        return null;
    }
    return Math.max(0, Math.round((Date.now() - t) / 60000));
}

// Work-order priority from current severity (anomaly score 0..1) and imminence
// (ticks-to-alarm from the escalation forecast; null when not trending up).
// P1 = act now, P2 = schedule soon, P3 = monitor.
export function derivePriority(anomalyScoreValue: number, etaToAlarmTicks: number | null): WorkOrderPriority {
    if (anomalyScoreValue >= 0.75 || (etaToAlarmTicks != null && etaToAlarmTicks <= 3)) {
        return "P1";
    }
    if (anomalyScoreValue >= 0.5 || (etaToAlarmTicks != null && etaToAlarmTicks <= 8)) {
        return "P2";
    }
    return "P3";
}

// Suspected refinery asset from the dominant abnormal signal, each normalised
// against its own warn->alarm band: high utilization points at the rotating
// equipment (Pump), high unit temperature at the Heat Exchanger.
export function recommendComponent(moduleTempC: number, inverterLoadPct: number): WorkOrderComponent {
    const tempSeverity = (moduleTempC - SIGNAL_METADATA.moduleTemp.warn) / Math.max(1, SIGNAL_METADATA.moduleTemp.alarm - SIGNAL_METADATA.moduleTemp.warn);
    const loadSeverity = (inverterLoadPct - SIGNAL_METADATA.inverterLoad.warn) / Math.max(1, SIGNAL_METADATA.inverterLoad.alarm - SIGNAL_METADATA.inverterLoad.warn);
    return loadSeverity >= tempSeverity ? "Pump" : "Heat Exchanger";
}

// Hysteresis wrapper around recommendComponent: keeps the previously-shown
// component when the two signal severities are within `margin` of each other, so
// the probable cause stops flip-flopping on every live refresh near the boundary.
export function recommendComponentStable(
    moduleTempC: number,
    inverterLoadPct: number,
    previous: WorkOrderComponent | null | undefined,
    margin = 0.18,
): WorkOrderComponent {
    const tempSeverity = (moduleTempC - SIGNAL_METADATA.moduleTemp.warn) / Math.max(1, SIGNAL_METADATA.moduleTemp.alarm - SIGNAL_METADATA.moduleTemp.warn);
    const loadSeverity = (inverterLoadPct - SIGNAL_METADATA.inverterLoad.warn) / Math.max(1, SIGNAL_METADATA.inverterLoad.alarm - SIGNAL_METADATA.inverterLoad.warn);
    const raw: WorkOrderComponent = loadSeverity >= tempSeverity ? "Pump" : "Heat Exchanger";
    if (!previous) {
        return raw;
    }
    return Math.abs(loadSeverity - tempSeverity) < margin ? previous : raw;
}

type ViewKey = "map" | "twin" | "alerts" | "graph" | "analytics" | "scenario" | "ask";

const NAV: { key: ViewKey; label: string; icon: string }[] = [
    { key: "map", label: "Map", icon: "🗺" },
    { key: "twin", label: "Digital Twin", icon: "🌀" },
    { key: "alerts", label: "Alerts", icon: "🚨" },
    { key: "graph", label: "Graph", icon: "🕸" },
    { key: "analytics", label: "Analytics", icon: "📊" },
    { key: "scenario", label: "Scenario Lab", icon: "🧪" },
    { key: "ask", label: "Ask IQ", icon: "💬" },
];

// Parse the URL hash (#/view/turbineId) into a shareable route.
export function parseHash(): { view?: ViewKey; selectedId?: string } {
    if (typeof window === "undefined") {
        return {};
    }
    const raw = window.location.hash.replace(/^#\/?/, "");
    const [v, id] = raw.split("/");
    const view = NAV.some((n) => n.key === v) ? (v as ViewKey) : undefined;
    return { view, selectedId: id ? decodeURIComponent(id) : undefined };
}

const SUGGESTED = [
    "Which refinery has the highest throughput now?",
    "Any units in alarm?",
    "Which unit has the highest utilization?",
    "What is the hottest unit temperature?",
    "Peak feed rate right now?",
    "How many process units total?",
];

function NavRail({ view, onChange, badges }: { view: ViewKey; onChange: (v: ViewKey) => void; badges?: Partial<Record<ViewKey, number>> }) {
    return (
        <nav aria-label="Primary views" className="flex w-full flex-row gap-1 overflow-x-auto border-b border-slate-800/60 bg-[#06101fcc] px-2 py-2 md:w-44 md:flex-col md:overflow-visible md:border-b-0 md:border-r md:px-0 md:py-3">
            {NAV.map((n) => {
                const badge = badges?.[n.key] ?? 0;
                return (
                    <button
                        key={n.key}
                        type="button"
                        onClick={() => onChange(n.key)}
                        aria-current={view === n.key ? "page" : undefined}
                        aria-label={badge > 0 ? `${n.label}, ${badge} active` : n.label}
                        className={`mx-2 flex items-center gap-3 rounded-lg px-3 py-2 text-left text-sm transition-colors ${view === n.key ? "bg-cyan-600/90 text-white" : "text-slate-300 hover:bg-slate-800/60"}`}
                    >
                        <span aria-hidden="true" className="text-lg leading-none">{n.icon}</span>
                        <span className="hidden md:inline">{n.label}</span>
                        {badge > 0 && (
                            <span aria-hidden="true" className="ml-auto rounded-full bg-red-600 px-1.5 text-[10px] font-semibold leading-4 text-white">{badge}</span>
                        )}
                    </button>
                );
            })}
        </nav>
    );
}

function KpiPill({ label, value, color }: { label: string; value: string; color?: string }) {
    return (
        <div className="rounded-lg border border-slate-700/60 bg-[#0a1830aa] px-3 py-1.5">
            <p className="text-[10px] uppercase tracking-wide text-slate-400">{label}</p>
            <p className="text-sm font-semibold" style={color ? { color } : undefined}>{value}</p>
        </div>
    );
}

function MetricCard({ label, value, sub, accent = "text-cyan-200" }: { label: string; value: string; sub?: string; accent?: string }) {
    return (
        <div className="rounded-lg border border-slate-700/60 bg-[#07162dbf] p-3 shadow-[0_10px_30px_rgba(1,8,20,0.5)]">
            <p className="text-xs uppercase tracking-wide text-slate-400">{label}</p>
            <p className={`mt-1 text-2xl font-semibold ${accent}`}>{value}</p>
            {sub && <p className="text-xs text-slate-400">{sub}</p>}
        </div>
    );
}

function Panel({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
    return (
        <div className="rounded-lg border border-slate-700/60 bg-[#07162dbf] p-3 shadow-[0_10px_30px_rgba(1,8,20,0.5)]">
            <div className="mb-2 flex items-center justify-between">
                <p className="text-xs uppercase tracking-wide text-slate-400">{title}</p>
                {action}
            </div>
            {children}
        </div>
    );
}

function BarList({ items }: { items: { label: string; value: number; display: string; color: string }[] }) {
    if (items.length === 0) {
        return <p className="text-xs text-slate-500">No data for the current filter.</p>;
    }
    const max = Math.max(1, ...items.map((i) => i.value));
    return (
        <ul className="space-y-2">
            {items.map((i) => (
                <li key={i.label}>
                    <div className="flex justify-between text-xs">
                        <span className="text-slate-300">{i.label}</span>
                        <span className="text-slate-100">{i.display}</span>
                    </div>
                    <div className="mt-1 h-2 rounded bg-slate-800">
                        <div className="h-2 rounded" style={{ width: `${(i.value / max) * 100}%`, background: i.color }} />
                    </div>
                </li>
            ))}
        </ul>
    );
}

export interface DonutSegment {
    label: string;
    color: string;
    value: number;
    fraction: number;
    offset: number;
}

// Pure helper: turn weighted items into cumulative donut arc segments (fractions sum to 1).
export function donutSegments(items: { label: string; value: number; color: string }[]): DonutSegment[] {
    const total = items.reduce((sum, i) => sum + Math.max(0, i.value), 0);
    if (total <= 0) {
        return [];
    }
    let offset = 0;
    return items.map((i) => {
        const fraction = Math.max(0, i.value) / total;
        const seg: DonutSegment = { label: i.label, color: i.color, value: i.value, fraction, offset };
        offset += fraction;
        return seg;
    });
}

function DonutChart({ items, unit }: { items: { label: string; value: number; display: string; color: string }[]; unit: string }) {
    const segs = donutSegments(items);
    if (segs.length === 0) {
        return <p className="text-xs text-slate-500">No data for the current filter.</p>;
    }
    const total = items.reduce((sum, i) => sum + Math.max(0, i.value), 0);
    const size = 132;
    const stroke = 18;
    const r = (size - stroke) / 2;
    const circ = 2 * Math.PI * r;
    return (
        <div className="flex items-center gap-4">
            <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="shrink-0">
                <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#0f1e36" strokeWidth={stroke} />
                <g transform={`rotate(-90 ${size / 2} ${size / 2})`}>
                    {segs.map((s) => (
                        <circle
                            key={s.label}
                            cx={size / 2}
                            cy={size / 2}
                            r={r}
                            fill="none"
                            stroke={s.color}
                            strokeWidth={stroke}
                            strokeDasharray={`${s.fraction * circ} ${circ - s.fraction * circ}`}
                            strokeDashoffset={-s.offset * circ}
                        >
                            <title>{`${s.label}: ${(s.fraction * 100).toFixed(0)}%`}</title>
                        </circle>
                    ))}
                </g>
                <text x="50%" y="47%" textAnchor="middle" className="fill-slate-100" style={{ fontSize: 17, fontWeight: 600 }}>
                    {total.toFixed(1)}
                </text>
                <text x="50%" y="60%" textAnchor="middle" className="fill-slate-400" style={{ fontSize: 9 }}>
                    {unit}
                </text>
            </svg>
            <ul className="flex-1 space-y-1 text-xs">
                {segs.map((s) => (
                    <li key={s.label} className="flex items-center justify-between gap-2">
                        <span className="flex items-center gap-1.5 truncate text-slate-300">
                            <span className="h-2 w-2 shrink-0 rounded-sm" style={{ background: s.color }} />
                            <span className="truncate">{s.label}</span>
                        </span>
                        <span className="shrink-0 text-slate-100">{(s.fraction * 100).toFixed(0)}%</span>
                    </li>
                ))}
            </ul>
        </div>
    );
}

function ScatterPlot({
    points,
    xLabel,
    yLabel,
    xMax,
    yMax,
}: {
    points: { x: number; y: number; color: string; label: string }[];
    xLabel: string;
    yLabel: string;
    xMax: number;
    yMax: number;
}) {
    const W = 520;
    const H = 210;
    const padL = 42;
    const padR = 12;
    const padT = 10;
    const padB = 30;
    const iw = W - padL - padR;
    const ih = H - padT - padB;
    const sx = (x: number) => padL + (Math.min(x, xMax) / xMax) * iw;
    const sy = (y: number) => padT + ih - (Math.min(y, yMax) / yMax) * ih;
    const ticks = 4;
    if (points.length === 0) {
        return <p className="text-xs text-slate-500">No data for the current filter.</p>;
    }
    return (
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full">
            {Array.from({ length: ticks + 1 }).map((_, i) => {
                const v = (yMax / ticks) * i;
                const y = sy(v);
                return (
                    <g key={`y${i}`}>
                        <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="#16233d" strokeWidth={1} />
                        <text x={padL - 6} y={y + 3} textAnchor="end" className="fill-slate-500" style={{ fontSize: 8 }}>
                            {Math.round(v)}
                        </text>
                    </g>
                );
            })}
            {Array.from({ length: ticks + 1 }).map((_, i) => {
                const v = (xMax / ticks) * i;
                const x = sx(v);
                return (
                    <text key={`x${i}`} x={x} y={H - padB + 13} textAnchor="middle" className="fill-slate-500" style={{ fontSize: 8 }}>
                        {Math.round(v)}
                    </text>
                );
            })}
            {points.map((p, i) => (
                <circle key={i} cx={sx(p.x)} cy={sy(p.y)} r={4} fill={p.color} fillOpacity={0.82} stroke="#0b1220" strokeWidth={0.5}>
                    <title>{p.label}</title>
                </circle>
            ))}
            <text x={padL + iw / 2} y={H - 4} textAnchor="middle" className="fill-slate-400" style={{ fontSize: 9 }}>
                {xLabel}
            </text>
            <text x={12} y={padT + ih / 2} textAnchor="middle" transform={`rotate(-90 12 ${padT + ih / 2})`} className="fill-slate-400" style={{ fontSize: 9 }}>
                {yLabel}
            </text>
        </svg>
    );
}

function HealthBar({ healthy, warning, alarm }: { healthy: number; warning: number; alarm: number }) {
    const total = Math.max(1, healthy + warning + alarm);
    return (
        <div className="flex h-3 overflow-hidden rounded">
            <div style={{ width: `${(healthy / total) * 100}%`, background: STATUS_COLORS.healthy }} />
            <div style={{ width: `${(warning / total) * 100}%`, background: STATUS_COLORS.warning }} />
            <div style={{ width: `${(alarm / total) * 100}%`, background: STATUS_COLORS.alarm }} />
        </div>
    );
}

function Meter({ label, value, unit, max, warn, alarm, property }: { label: string; value: number; unit: string; max: number; warn: number; alarm: number; property?: string }) {
    const pct = Math.max(0, Math.min(100, (value / max) * 100));
    const color = value >= alarm ? STATUS_COLORS.alarm : value >= warn ? STATUS_COLORS.warning : STATUS_COLORS.healthy;
    const bandHint = property
        ? `${property} · warn \u2265 ${warn}${unit} · alarm \u2265 ${alarm}${unit}`
        : undefined;
    return (
        <div title={bandHint}>
            <div className="flex justify-between text-xs">
                <span className="text-slate-300">{label}</span>
                <span className="text-slate-100">{value} {unit}</span>
            </div>
            <div className="mt-1 h-2 rounded bg-slate-800">
                <div className="h-2 rounded" style={{ width: `${pct}%`, background: color }} />
            </div>
        </div>
    );
}

const SIGNAL_DEFS: SignalMetadata[] = SIGNAL_ORDER.map((k) => SIGNAL_METADATA[k]);

function RelationshipGraph({ turbines, sites, selectedId, statusFilter, onSelect }: {
    turbines: PlantTelemetry[];
    sites: SolarPlantSite[];
    selectedId: string;
    statusFilter: StatusFilter;
    onSelect: (id: string) => void;
}) {
    const W = 1000;
    const H = 680;
    const cx = W / 2;
    const cy = H / 2;
    const [zoom, setZoom] = useState(1);
    const [pan, setPan] = useState({ x: 0, y: 0 });
    const [hovered, setHovered] = useState<string | null>(null);
    const [hoveredSite, setHoveredSite] = useState<string | null>(null);
    const dragRef = useRef<{ x: number; y: number; px: number; py: number } | null>(null);

    const nSites = Math.max(sites.length, 1);
    const sitePos = sites.map((s, i) => {
        const a = (Math.PI * 2 * i) / nSites - Math.PI / 2;
        return { id: s.id, name: s.name, x: cx + Math.cos(a) * 210, y: cy + Math.sin(a) * 210, a };
    });
    const sitePosById = new Map(sitePos.map((p) => [p.id, p]));
    const matches = (t: PlantTelemetry) => statusFilter === "all" || t.status === statusFilter;
    const hoveredSiteId = hoveredSite ?? (hovered ? turbines.find((t) => t.id === hovered)?.siteId : undefined);

    // Pre-place every turbine so we can anchor the signal drill-down for the selection.
    const placed = sites.flatMap((s) => {
        const sp = sitePosById.get(s.id);
        if (!sp) {
            return [] as { t: PlantTelemetry; sp: typeof sitePos[number]; x: number; y: number; a: number }[];
        }
        const local = turbines.filter((t) => t.siteId === s.id);
        const spread = Math.PI * 0.95;
        return local.map((t, j) => {
            const a = sp.a + (spread * (j - (local.length - 1) / 2)) / Math.max(local.length, 1);
            return { t, sp, x: sp.x + Math.cos(a) * 96, y: sp.y + Math.sin(a) * 96, a };
        });
    });
    const selectedPlaced = placed.find((p) => p.t.id === selectedId);

    const onWheel = (e: ReactWheelEvent<SVGSVGElement>) => {
        e.preventDefault();
        setZoom((z) => Math.min(3, Math.max(0.5, z - e.deltaY * 0.0012)));
    };
    const onPointerDown = (e: ReactPointerEvent<SVGSVGElement>) => {
        dragRef.current = { x: e.clientX, y: e.clientY, px: pan.x, py: pan.y };
    };
    const onPointerMove = (e: ReactPointerEvent<SVGSVGElement>) => {
        const d = dragRef.current;
        if (!d) {
            return;
        }
        setPan({ x: d.px + (e.clientX - d.x), y: d.py + (e.clientY - d.y) });
    };
    const endDrag = () => { dragRef.current = null; };

    return (
        <svg
            viewBox={`0 0 ${W} ${H}`}
            className="h-full w-full cursor-grab touch-none select-none active:cursor-grabbing"
            onWheel={onWheel}
            onPointerDown={onPointerDown}
            onPointerMove={onPointerMove}
            onPointerUp={endDrag}
            onPointerLeave={endDrag}
        >
            <g transform={`translate(${pan.x} ${pan.y}) scale(${zoom})`}>
                {sitePos.map((p) => {
                    const lit = p.id === hoveredSiteId;
                    return (
                        <g key={`fe-${p.id}`}>
                            <line x1={cx} y1={cy} x2={p.x} y2={p.y} stroke={lit ? "#3d6ea5" : "#1f3b5c"} strokeWidth={2} opacity={hoveredSiteId ? (lit ? 1 : 0.18) : 1} />
                            {lit && (
                                <text x={(cx + p.x) / 2} y={(cy + p.y) / 2 - 4} textAnchor="middle" fontSize={10} fill="#7fb2e6" className="italic">operates</text>
                            )}
                        </g>
                    );
                })}
                {placed.map(({ t, sp, x: tx, y: ty }) => {
                    const sel = t.id === selectedId;
                    const on = matches(t);
                    let opacity = on ? 1 : 0.1;
                    if (hovered && hovered !== t.id) {
                        opacity = Math.min(opacity, 0.22);
                    }
                    return (
                        <g
                            key={t.id}
                            onClick={() => { if (on) { onSelect(t.id); } }}
                            onMouseEnter={() => setHovered(t.id)}
                            onMouseLeave={() => setHovered(null)}
                            className={on ? "cursor-pointer" : "cursor-default"}
                            opacity={opacity}
                        >
                            <line x1={sp.x} y1={sp.y} x2={tx} y2={ty} stroke={hovered === t.id ? STATUS_COLORS[t.status] : "#24405f"} strokeWidth={hovered === t.id ? 2 : 1.2} />
                            {hovered === t.id && (
                                <text x={(sp.x + tx) / 2} y={(sp.y + ty) / 2 - 3} textAnchor="middle" fontSize={9} fill="#9fb6cf" className="italic">hosts</text>
                            )}
                            <circle cx={tx} cy={ty} r={sel || hovered === t.id ? 9 : 6} fill={STATUS_COLORS[t.status]} stroke={sel ? "#ffffff" : "#0a1830"} strokeWidth={sel ? 2 : 1} />
                            {sel && <text x={tx} y={ty - 14} textAnchor="middle" fontSize={10} fill="#e2e8f0">{t.id.slice(-5)}</text>}
                        </g>
                    );
                })}
                {selectedPlaced && (() => {
                    const { t, x: sx, y: sy, a } = selectedPlaced;
                    const spread = Math.PI * 0.6;
                    const n = SIGNAL_DEFS.length;
                    return (
                        <g>
                            {SIGNAL_DEFS.map((def, k) => {
                                const sa = a + (spread * (k - (n - 1) / 2)) / n;
                                const gx = sx + Math.cos(sa) * 74;
                                const gy = sy + Math.sin(sa) * 74;
                                const val = def.get(t);
                                const color = STATUS_COLORS[signalState(def.key, val)];
                                const labelAbove = gy < sy;
                                const band = Number.isFinite(def.alarm)
                                    ? `warn \u2265 ${def.warn}${def.unit} \u00b7 alarm \u2265 ${def.alarm}${def.unit}`
                                    : "informational";
                                return (
                                    <g key={def.key}>
                                        <title>{`${def.ontologyProperty} (${def.unit}) \u00b7 ${band}`}</title>
                                        <line x1={sx} y1={sy} x2={gx} y2={gy} stroke="#2c4a6b" strokeWidth={1.2} strokeDasharray="3 3" />
                                        <circle cx={gx} cy={gy} r={5} fill={color} stroke="#0a1830" strokeWidth={1} />
                                        <text x={gx} y={gy + (labelAbove ? -9 : 15)} textAnchor="middle" fontSize={9} fill="#94a3b8">{def.label}</text>
                                        <text x={gx} y={gy + (labelAbove ? -20 : 26)} textAnchor="middle" fontSize={10} fill="#cbd5e1">{Math.round(val)} {def.unit}</text>
                                    </g>
                                );
                            })}
                        </g>
                    );
                })()}
                {sitePos.map((p, i) => (
                    <g
                        key={p.id}
                        onMouseEnter={() => setHoveredSite(p.id)}
                        onMouseLeave={() => setHoveredSite(null)}
                        className="cursor-pointer"
                    >
                        <circle cx={p.x} cy={p.y} r={16} fill="#0b2747" stroke={SITE_COLORS[i % SITE_COLORS.length]} strokeWidth={2.5} />
                        <text x={p.x} y={p.y - 22} textAnchor="middle" fontSize={13} fill="#cbd5e1">{p.name}</text>
                    </g>
                ))}
                <circle cx={cx} cy={cy} r={24} fill="#0e2a4d" stroke="#6ee7ff" strokeWidth={3} />
                <text x={cx} y={cy + 4} textAnchor="middle" fontSize={13} fill="#e2e8f0">Fleet</text>
            </g>
        </svg>
    );
}

function App() {
    const initialRoute = parseHash();
    const [seed, setSeed] = useState(0);
    const [selectedId, setSelectedId] = useState(initialRoute.selectedId ?? "CESTAS-PV-01");
    const [view, setView] = useState<ViewKey>(initialRoute.view ?? "map");
    // Digital Twin now hosts the merged Sites + Operations content via a sub-tab.
    const [twinTab, setTwinTab] = useState<"overview" | "ops">("overview");
    const [detailOpen, setDetailOpen] = useState(false);

    const [live, setLive] = useState(true);
    const [refreshMs, setRefreshMs] = useState(2500);
    const [siteFilter, setSiteFilter] = useState("all");
    const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
    const [search, setSearch] = useState("");

    const [question, setQuestion] = useState("Which site has the highest output now?");
    const [askLoading, setAskLoading] = useState(false);
    const [askError, setAskError] = useState<string | null>(null);
    const [askResult, setAskResult] = useState<AskResult | null>(null);
    const [askHistory, setAskHistory] = useState<AskHistoryEntry[]>([]);
    const [agentCheckLoading, setAgentCheckLoading] = useState(false);
    const [agentCheckResult, setAgentCheckResult] = useState<DataAgentConnectionResult | null>(null);
    const [writebackMessage, setWritebackMessage] = useState<string | null>(null);
    const [ackLog, setAckLog] = useState<Record<string, { at: string; by: string }>>(() => JSON.parse(localStorage.getItem("refinery-ack-log") ?? "{}"));
    const [ackMessage, setAckMessage] = useState<string | null>(null);
    const [operatorRole, setOperatorRole] = useState<OperatorRole>(() => normalizeOperatorRole(localStorage.getItem("refinery-operator-role")));
    const [showAcked, setShowAcked] = useState(false);
    const [graphFilter, setGraphFilter] = useState<StatusFilter>("all");
    const [graphNonce, setGraphNonce] = useState(0);
    const [autoLogCount, setAutoLogCount] = useState(0);
    const prevStatusRef = useRef<Record<string, PlantStatus>>({});
    const autoLoggedRef = useRef<Set<string>>(new Set());
    const autoInitRef = useRef(false);
    const anomalyHistRef = useRef<Map<string, number[]>>(new Map());
    const askCacheRef = useRef<Map<string, { at: number; result: AskResult }>>(new Map());

    const [powerHistory, setPowerHistory] = useState<number[]>([]);
    const historyKeyRef = useRef(selectedId);

    const [notes, setNotes] = useState<DispatchNoteRecord[]>([]);
    const [notesLoading, setNotesLoading] = useState(false);
    const [thresholdsPublished, setThresholdsPublished] = useState<number | null>(null);
    const [thresholdNonce, setThresholdNonce] = useState(0);

    const [forecastHorizon, setForecastHorizon] = useState(5);
    const [simCurtail, setSimCurtail] = useState(0);
    const [simDowntime, setSimDowntime] = useState(0);
    const [simHorizon, setSimHorizon] = useState(12);

    // Scenario Lab: multiple editable what-if plans compared side by side.
    const [scenarios, setScenarios] = useState<ScenarioSpec[]>([
        { id: "asis", label: "Run as-is", curtailmentPct: 0, downtimeTicks: 0, horizonTicks: 12 },
        { id: "trim", label: "Trim throughput 15%", curtailmentPct: 15, downtimeTicks: 0, horizonTicks: 12 },
        { id: "maint", label: "Maintenance window", curtailmentPct: 0, downtimeTicks: 4, horizonTicks: 12 },
    ]);
    const [scenarioNarrative, setScenarioNarrative] = useState<string | null>(null);
    const [scenarioNarrativeSource, setScenarioNarrativeSource] = useState<"fabriciq" | "local" | null>(null);
    const [scenarioNarrativeLoading, setScenarioNarrativeLoading] = useState(false);
    const [scenarioQuestion, setScenarioQuestion] = useState("");
    const [importedActuals, setImportedActuals] = useState<ImportedActualsResult | null>(null);
    const [importStatus, setImportStatus] = useState<string | null>(null);
    const scenarioFileRef = useRef<HTMLInputElement | null>(null);
    const [historyWindow, setHistoryWindow] = useState<HistoryWindow>("6h");
    const [wbAction, setWbAction] = useState("Acknowledge");
    const [wbSetpoint, setWbSetpoint] = useState("");
    const [wbNote, setWbNote] = useState("");

    // Mission-ops: work-order dispatch, responder ranking, escalation & SLA.
    const [maintenanceOrders, setMaintenanceOrders] = useState<MaintenanceOrderRecord[]>([]);
    const [woAssignee, setWoAssignee] = useState("");
    const [woMessage, setWoMessage] = useState<string | null>(null);
    const [selectedEvidenceId, setSelectedEvidenceId] = useState(MOCK_REFINERY_EVIDENCE[0]?.id ?? "");
    const [responderShiftFilter, setResponderShiftFilter] = useState<"all" | "day" | "swing" | "night">(() => currentShiftByHour(new Date().getHours()));
    const [onCallOnly, setOnCallOnly] = useState(true);
    const [responderLoad, setResponderLoad] = useState<Record<string, number>>(() => JSON.parse(localStorage.getItem("refinery-responder-load") ?? "{}"));
    const [escalationStage, setEscalationStage] = useState<EscalationStage>("none");
    const [demoRunCount, setDemoRunCount] = useState(0);
    const [techPopupResponderId, setTechPopupResponderId] = useState<string | null>(null);
    const [techPopupOpen, setTechPopupOpen] = useState(false);
    const componentStickyRef = useRef<Record<string, WorkOrderComponent>>({});

    // Guided demo experience.
    const [autoPlayRunning, setAutoPlayRunning] = useState(false);
    const [demoScriptStep, setDemoScriptStep] = useState<DemoScriptStepId | "idle">("idle");
    const [demoRunLog, setDemoRunLog] = useState<MissionReportEvent[]>([]);
    const [runHistory, setRunHistory] = useState<MissionReport[]>(() => {
        try {
            return JSON.parse(localStorage.getItem("refinery-run-history") ?? "[]") as MissionReport[];
        } catch {
            return [];
        }
    });
    const [demoPanelOpen, setDemoPanelOpen] = useState(false);
    const [demoStepIndex, setDemoStepIndex] = useState(0);
    const [demoIntroOpen, setDemoIntroOpen] = useState(false);
    const [reportModal, setReportModal] = useState<MissionReport | null>(null);
    const [demoFocusPart, setDemoFocusPart] = useState<string | null>(null);
    const runAskRef = useRef<(override?: string) => Promise<void>>(async () => {});
    const missionReportRef = useRef<() => void>(() => {});

    // Twin device graph admin.
    const [focusedTwinPart, setFocusedTwinPart] = useState<RefineryPartKey | null>(null);
    const [focusedTwinDevice, setFocusedTwinDevice] = useState<string | null>(null);
    const [twinDeviceGraph, setTwinDeviceGraph] = useState<Record<RefineryPartKey, RefineryDeviceNode[]>>(UNIT_COMPONENT_DEVICES);
    const [twinDeviceRows, setTwinDeviceRows] = useState<UnitDeviceRecord[]>([]);
    const [deviceDraft, setDeviceDraft] = useState<RefineryDeviceDraft | null>(null);
    const [deviceDraftDirty, setDeviceDraftDirty] = useState(false);
    const [deviceSaveMessage, setDeviceSaveMessage] = useState<string | null>(null);
    const [deviceSaveBusy, setDeviceSaveBusy] = useState(false);

    const canWriteback = canManageDispatch(operatorRole);
    const historyLimit = historyPointLimit(historyWindow);

    useEffect(() => {
        localStorage.setItem("refinery-operator-role", operatorRole);
    }, [operatorRole]);

    useEffect(() => {
        localStorage.setItem("refinery-responder-load", JSON.stringify(responderLoad));
    }, [responderLoad]);

    useEffect(() => {
        if (!live) {
            return;
        }
        const id = window.setInterval(() => setSeed((v) => v + 1), refreshMs);
        return () => window.clearInterval(id);
    }, [live, refreshMs]);

    useEffect(() => {
        ensureOntologySites(
            SITES.map((s) => ({
                siteId: s.id,
                name: s.name,
                region: s.region,
                latitude: s.lat,
                longitude: s.lon,
                capacityMw: s.capacityMw,
            })),
        ).catch(() => {
            /* backend not ready — seeding is best-effort */
        });
    }, []);

    useEffect(() => {
        let cancelled = false;
        ensureSignalThresholds(thresholdRows())
            .then(setThresholdsPublished)
            .catch(() => {
                /* backend not ready — publishing thresholds is best-effort */
            })
            .finally(() => {
                listSignalThresholds()
                    .then((rows) => {
                        if (cancelled) return;
                        if (applyThresholdOverrides(rows) > 0) {
                            setThresholdNonce((v) => v + 1);
                        }
                    })
                    .catch(() => {
                        /* backend not ready — keep static default bands */
                    });
            });
        return () => {
            cancelled = true;
        };
    }, []);

    const syntheticTurbines = useMemo(() => buildFarm(seed), [seed, thresholdNonce]);
    const liveView = useLiveTelemetry();
    const turbines = liveView?.turbines ?? syntheticTurbines;
    const sites = liveView?.sites ?? SITES;
    const selected = turbines.find((t) => t.id === selectedId) ?? turbines[0];

    // Seed + load the twin device graph from the ontology backend (fallback-safe).
    useEffect(() => {
        let cancelled = false;
        const seeds = unitDeviceSeeds(UNIT_COMPONENT_DEVICES);
        void (async () => {
            try {
                await ensureUnitDevices(seeds);
                const rows = await listUnitDevices();
                if (cancelled) {
                    return;
                }
                if (rows.length > 0) {
                    setTwinDeviceRows(rows);
                    setTwinDeviceGraph(mergeUnitDeviceGraph(rows, UNIT_COMPONENT_DEVICES));
                }
            } catch {
                /* backend unavailable — continue using the bundled device graph */
            }
        })();
        return () => {
            cancelled = true;
        };
    }, []);

    // Clear the twin focus when the inspected unit changes.
    useEffect(() => {
        setFocusedTwinPart(null);
        setFocusedTwinDevice(null);
    }, [selectedId]);

    // During the guided demo's twin step, mirror the focused asset into the admin.
    useEffect(() => {
        if (demoFocusPart && (["array", "inverter", "tracker", "output"] as string[]).includes(demoFocusPart)) {
            const part = demoFocusPart as RefineryPartKey;
            setFocusedTwinPart(part);
            setFocusedTwinDevice((prev) => prev ?? (twinDeviceGraph[part] ?? [])[0]?.key ?? null);
        }
    }, [demoFocusPart, twinDeviceGraph]);

    // Sync the editable draft to the focused device (or clear it).
    useEffect(() => {
        setDeviceSaveMessage(null);
        if (!focusedTwinPart || !focusedTwinDevice) {
            setDeviceDraft(null);
            setDeviceDraftDirty(false);
            return;
        }
        const componentDevices = twinDeviceGraph[focusedTwinPart] ?? [];
        const node = componentDevices.find((d) => d.key === focusedTwinDevice);
        if (!node) {
            setDeviceDraft(null);
            setDeviceDraftDirty(false);
            return;
        }
        const row = twinDeviceRows.find((r) => r.deviceKey === focusedTwinDevice);
        const sortOrder = row?.sortOrder ?? componentDevices.findIndex((d) => d.key === focusedTwinDevice);
        setDeviceDraft(draftFromUnitDevice(node, sortOrder));
        setDeviceDraftDirty(false);
    }, [focusedTwinDevice, focusedTwinPart, twinDeviceGraph, twinDeviceRows]);

    const openTurbine = useCallback((id: string) => {
        setSelectedId(id);
        setDetailOpen(true);
    }, []);

    // Merged navigation: Operations lives inside the Digital Twin as a sub-tab.
    const goToOps = useCallback(() => {
        setView("twin");
        setTwinTab("ops");
    }, []);
    // Open the Digital Twin on its Overview (3D twin) sub-tab, optionally selecting a unit.
    const goToTwin = useCallback((id?: string) => {
        if (id) {
            setSelectedId(id);
        }
        setView("twin");
        setTwinTab("overview");
    }, []);

    const matchesFilter = useCallback(
        (t: PlantTelemetry) =>
            (siteFilter === "all" || t.siteId === siteFilter) &&
            (statusFilter === "all" || t.status === statusFilter),
        [siteFilter, statusFilter],
    );

    const visibleTurbines = useMemo(() => turbines.filter(matchesFilter), [turbines, matchesFilter]);
    const dimmedIds = useMemo(
        () => new Set(turbines.filter((t) => !matchesFilter(t)).map((t) => t.id)),
        [turbines, matchesFilter],
    );
    const topPerformers = useMemo(
        () => [...visibleTurbines].sort((a, b) => b.powerKw - a.powerKw).slice(0, 3),
        [visibleTurbines],
    );

    const activeAlerts = useMemo(
        () => turbines.filter((t) => t.status !== "healthy").sort((a, b) => anomalyScore(b) - anomalyScore(a)),
        [turbines],
    );
    const anomalyWatch = useMemo(
        () => turbines.map((t) => ({ t, score: anomalyScore(t), forecast: forecastEscalation(anomalyHistRef.current.get(t.id) ?? []) })).sort((a, b) => b.score - a.score).slice(0, 6),
        [turbines],
    );

    // Maintain a short rolling window of each unit's anomaly score so the watch
    // panel can project a trend (rising/falling) and an ETA-to-alarm from the slope.
    useEffect(() => {
        const hist = anomalyHistRef.current;
        const seen = new Set<string>();
        turbines.forEach((t) => {
            seen.add(t.id);
            const arr = hist.get(t.id) ?? [];
            arr.push(anomalyScore(t));
            if (arr.length > 12) {
                arr.shift();
            }
            hist.set(t.id, arr);
        });
        for (const id of hist.keys()) {
            if (!seen.has(id)) {
                hist.delete(id);
            }
        }
    }, [turbines]);

    // Hydrate anomaly-score history from persisted telemetry when live mode is on
    // so trend direction/ETA starts from recent context instead of a cold window.
    useEffect(() => {
        if (!isLiveTelemetryConfigured()) {
            return;
        }
        let cancelled = false;
        const idsToHydrate = turbines
            .map((t) => t.id)
            .filter((id) => (anomalyHistRef.current.get(id)?.length ?? 0) < 3);
        if (idsToHydrate.length === 0) {
            return;
        }
        void (async () => {
            const seeded = await Promise.all(
                idsToHydrate.map(async (id) => ({ id, scores: await fetchAnomalyScores(id, 12) })),
            );
            if (cancelled) {
                return;
            }
            seeded.forEach(({ id, scores }) => {
                if (!scores || scores.length === 0) {
                    return;
                }
                const current = anomalyHistRef.current.get(id) ?? [];
                if (current.length < 3) {
                    anomalyHistRef.current.set(id, scores.slice(-12));
                }
            });
        })();
        return () => {
            cancelled = true;
        };
    }, [turbines]);

    const unackedAlerts = useMemo(() => activeAlerts.filter((t) => !ackLog[t.id]), [activeAlerts, ackLog]);

    const fc = useMemo(
        () => forecastDetail(powerHistory, forecastHorizon),
        [powerHistory, forecastHorizon],
    );
    const forecast = fc.value;
    const multiForecast = useMemo(
        () => [3, 6, 12].map((h) => ({ h, ...forecastDetail(powerHistory, h) })),
        [powerHistory],
    );

    useEffect(() => {
        setPowerHistory((prev) => {
            const base = historyKeyRef.current === selectedId ? prev : [];
            historyKeyRef.current = selectedId;
            return [...base, selected.powerKw].slice(-historyLimit);
        });
    }, [historyLimit, selected.powerKw, selectedId]);

    // Seed the sparkline / forecast window from persisted Lakehouse-backed
    // history when a live semantic model is configured, so it reflects real
    // readings from the first render instead of a cold in-browser accumulation.
    // The synchronous append effect above resets to [] on selection change; this
    // async fetch lands afterwards and overwrites it, then live ticks append on
    // top. No-ops (returns null) when unconfigured, keeping the synthetic path.
    useEffect(() => {
        if (!isLiveTelemetryConfigured()) {
            return;
        }
        let cancelled = false;
        void (async () => {
            const history = await fetchPowerHistory(selectedId, historyLimit);
            if (cancelled || !history || history.length === 0) {
                return;
            }
            historyKeyRef.current = selectedId;
            setPowerHistory(history.slice(-historyLimit));
        })();
        return () => {
            cancelled = true;
        };
    }, [historyLimit, selectedId]);

    const loadNotes = useCallback(async () => {
        setNotesLoading(true);
        try {
            setNotes(await recentDispatchNotes(8));
        } catch {
            /* backend not ready — best effort */
        } finally {
            setNotesLoading(false);
        }
    }, []);

    // Load recent maintenance work orders (fallback-safe): the ontology backend
    // is preferred, but locally-persisted orders keep the loop working offline.
    const loadOrders = useCallback(async () => {
        try {
            const rows = await recentMaintenanceOrders(20);
            setMaintenanceOrders(rows);
        } catch {
            try {
                const local = JSON.parse(localStorage.getItem("refinery-workorders") ?? "[]") as MaintenanceOrderRecord[];
                setMaintenanceOrders(local);
            } catch {
                setMaintenanceOrders([]);
            }
        }
    }, []);

    const acknowledgeAlert = useCallback(async (t: PlantTelemetry) => {
        if (!canWriteback) {
            setAckMessage("Viewer mode — switch to Operator to acknowledge alarms.");
            return;
        }
        const entry = { at: new Date().toISOString(), by: "operator" };
        setAckLog((prev) => {
            const next = { ...prev, [t.id]: entry };
            localStorage.setItem("refinery-ack-log", JSON.stringify(next));
            return next;
        });
        setAckMessage(`Acknowledged ${t.id}…`);
        try {
            await saveDispatchNote({
                turbineId: t.id,
                siteId: t.siteId,
                status: t.status,
                powerKw: t.powerKw,
                note: `[Acknowledge] alert ack (unit ${t.moduleTempC} C, utilization ${t.inverterLoadPct} %)`,
                author: "operator",
                createdAt: new Date().toISOString(),
            });
            setAckMessage(`Acknowledged ${t.id} · written to ontology.`);
            void loadNotes();
        } catch {
            setAckMessage(`Acknowledged ${t.id} · saved locally (backend unreachable).`);
        }
    }, [canWriteback, loadNotes]);

    useEffect(() => {
        void loadNotes();
    }, [loadNotes]);

    useEffect(() => {
        void loadOrders();
    }, [loadOrders]);

    useEffect(() => {
        window.history.replaceState(null, "", `#/${view}/${encodeURIComponent(selectedId)}`);
    }, [view, selectedId]);

    // Auto-write a DispatchNote the first time a turbine crosses into alarm.
    useEffect(() => {
        const prev = prevStatusRef.current;
        if (!autoInitRef.current) {
            turbines.forEach((t) => {
                prev[t.id] = t.status;
                if (t.status === "alarm") {
                    autoLoggedRef.current.add(t.id);
                }
            });
            autoInitRef.current = true;
            return;
        }
        const newlyAlarming: PlantTelemetry[] = newlyAlarmed(prev, turbines)
            .filter((id) => !autoLoggedRef.current.has(id))
            .map((id) => turbines.find((t) => t.id === id))
            .filter((t): t is PlantTelemetry => Boolean(t));
        newlyAlarming.forEach((t) => autoLoggedRef.current.add(t.id));
        turbines.forEach((t) => {
            if (t.status !== "alarm") {
                autoLoggedRef.current.delete(t.id);
            }
            prev[t.id] = t.status;
        });
        if (newlyAlarming.length === 0) {
            return;
        }
        setAutoLogCount((c) => c + newlyAlarming.length);
        void (async () => {
            for (const t of newlyAlarming) {
                try {
                    await saveDispatchNote({
                        turbineId: t.id,
                        siteId: t.siteId,
                        status: t.status,
                        powerKw: t.powerKw,
                        note: `[Auto] alarm onset (unit ${t.moduleTempC} C, utilization ${t.inverterLoadPct} %)`,
                        author: "system",
                        createdAt: new Date().toISOString(),
                    });
                } catch {
                    /* best effort — backend may be unreachable */
                }
                void postTeamsAlert({
                    id: t.id,
                    siteName: t.siteName,
                    status: t.status,
                    powerKw: t.powerKw,
                    detail: `unit ${t.moduleTempC} C, utilization ${t.inverterLoadPct} %`,
                });
            }
            void loadNotes();
        })();
    }, [turbines, loadNotes]);

    const handleExport = useCallback(() => {
        const header = "id,site,siteId,latitude,longitude,powerKw,irradianceWm2,moduleTempC,inverterLoadPct,status";
        const rows = visibleTurbines.map((t) =>
            [t.id, t.siteName, t.siteId, t.latitude.toFixed(3), t.longitude.toFixed(3), t.powerKw, t.irradianceWm2, t.moduleTempC, t.inverterLoadPct, t.status].join(","),
        );
        const blob = new Blob([[header, ...rows].join("\n")], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `fleet-snapshot-${Date.now()}.csv`;
        a.click();
        URL.revokeObjectURL(url);
    }, [visibleTurbines]);

    const handleSearch = useCallback(() => {
        const q = search.trim().toLowerCase();
        if (!q) {
            return;
        }
        const hit = turbines.find((t) => t.id.toLowerCase().includes(q));
        if (hit) {
            setSelectedId(hit.id);
        }
    }, [search, turbines]);

    const sitesSummary = useMemo(() => {
        return sites.map((site) => {
            const local = turbines.filter((t) => t.siteId === site.id);
            const sitePowerMw = local.reduce((sum, t) => sum + t.powerKw, 0);
            const alarms = local.filter((t) => t.status === "alarm").length;
            const warnings = local.filter((t) => t.status === "warning").length;

            return {
                ...site,
                sitePowerMw,
                alarms,
                warnings,
            };
        });
    }, [turbines, sites]);

    const fleetPower = visibleTurbines.reduce((sum, t) => sum + t.powerKw, 0);
    const alarms = visibleTurbines.filter((t) => t.status === "alarm").length;
    const warnings = visibleTurbines.filter((t) => t.status === "warning").length;
    const healthy = visibleTurbines.length - alarms - warnings;

    const siteSummaries = useMemo(() => summarizeSites(turbines, sites), [turbines, sites]);
    const siteReport = siteSummaries.find((s) => s.id === selected.siteId);
    const siteUnits = useMemo(() => turbines.filter((t) => t.siteId === selected.siteId), [turbines, selected.siteId]);

    const scenario = simulateScenario({ baselineKw: selected.powerKw, curtailmentPct: simCurtail, downtimeTicks: simDowntime, horizonTicks: simHorizon });

    // ---- Scenario Lab derived state --------------------------------------
    const scenarioComparison = useMemo(
        () => compareScenarios(selected.powerKw, scenarios),
        [selected.powerKw, scenarios],
    );
    const scenarioVariance = useMemo(
        () => forecastVsRealised(powerHistory, fc.value),
        [powerHistory, fc.value],
    );
    const scenarioInsights = useMemo(() => computeInsights(scenarioComparison), [scenarioComparison]);
    const importedSummary = useMemo(
        () => (importedActuals ? summarizeActuals(importedActuals.points) : null),
        [importedActuals],
    );
    const importedNote = useMemo(
        () => (importedActuals && importedSummary ? `Imported "${importedActuals.name}": mean ${importedSummary.mean.toLocaleString()} kbd over ${importedSummary.n} points, ${importedSummary.trend} trend (min ${importedSummary.min.toLocaleString()}, max ${importedSummary.max.toLocaleString()}).` : undefined),
        [importedActuals, importedSummary],
    );
    const updateScenario = useCallback((id: string, patch: Partial<ScenarioSpec>) => {
        setScenarios((prev) => prev.map((s) => (s.id === id ? { ...s, ...patch } : s)));
        setScenarioNarrative(null);
    }, []);
    const addScenario = useCallback(() => {
        setScenarios((prev) => prev.length >= 8 ? prev : [...prev, {
            id: `plan-${Date.now().toString(36)}`,
            label: `Plan ${prev.length + 1}`,
            curtailmentPct: 10,
            downtimeTicks: 2,
            horizonTicks: 12,
        }]);
    }, []);
    const removeScenario = useCallback((id: string) => {
        setScenarios((prev) => (prev.length <= 1 ? prev : prev.filter((s) => s.id !== id)));
        setScenarioNarrative(null);
    }, []);
    // Parse an uploaded .xlsx/.csv into scenarios or an actuals series. SheetJS is
    // lazy-loaded so it never weighs down the initial bundle.
    const handleScenarioImport = useCallback(async (file: File) => {
        setImportStatus(`Reading ${file.name}…`);
        try {
            const buffer = await file.arrayBuffer();
            const XLSX = await import("xlsx");
            const workbook = XLSX.read(buffer, { type: "array" });
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const rows = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: null });
            const result = classifyImport(rows, file.name);
            if (result.kind === "scenarios") {
                setScenarios((prev) => [...prev, ...result.scenarios].slice(0, 8));
                setImportStatus(`Imported ${result.scenarios.length} scenario(s) from ${file.name}.`);
            } else if (result.kind === "actuals") {
                setImportedActuals(result);
                setImportStatus(`Imported ${result.points.length} actual reading(s) from ${file.name}.`);
            } else {
                setImportStatus(result.reason);
            }
            setScenarioNarrative(null);
        } catch (err) {
            setImportStatus(`Could not read file: ${err instanceof Error ? err.message : String(err)}`);
        }
    }, []);
    // Export the current comparison, insights, variance, AI summary (and any imported
    // actuals) to a multi-sheet .xlsx. SheetJS is lazy-loaded on demand.
    const handleScenarioExport = useCallback(async () => {
        setImportStatus("Building workbook…");
        try {
            const XLSX = await import("xlsx");
            const wb = XLSX.utils.book_new();
            const scenarioRows = scenarioComparison.scenarios.map((s) => ({
                Plan: s.label,
                "Curtailment %": s.curtailmentPct,
                Downtime: s.downtimeTicks,
                Horizon: s.horizonTicks,
                "Projected kbd": s.projectedKbd,
                "Volume Delta kbd-t": s.volumeDelta,
                "Delta %": s.deltaPct,
                Rank: s.rank,
                Best: s.isBest ? "yes" : "",
            }));
            XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(scenarioRows), "Scenarios");
            const insightsRows = [
                { Metric: "Unit", Value: `${selected.siteName} · ${selected.id}` },
                { Metric: "Baseline kbd", Value: scenarioComparison.baselineKbd },
                { Metric: "Best plan", Value: scenarioInsights.bestLabel ?? "" },
                { Metric: "Worst plan", Value: scenarioInsights.worstLabel ?? "" },
                { Metric: "Spread kbd-t", Value: scenarioInsights.spread },
                { Metric: "Average delta kbd-t", Value: scenarioInsights.avgDelta },
                { Metric: "Dispersion", Value: scenarioInsights.deltaStdDev },
                { Metric: "Riskiest plan", Value: scenarioInsights.riskiestLabel ?? "" },
                { Metric: "Forecast kbd", Value: scenarioVariance.forecast },
                { Metric: "Realised mean kbd", Value: scenarioVariance.realisedMean },
                { Metric: "Forecast accuracy %", Value: scenarioVariance.accuracyPct },
                { Metric: "Forecast bias", Value: scenarioVariance.bias },
                { Metric: "AI summary", Value: scenarioNarrative ?? "(not generated)" },
            ];
            XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(insightsRows), "Insights");
            if (importedActuals) {
                XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(importedActuals.points), "Imported actuals");
            }
            XLSX.writeFile(wb, `scenario-lab-${selected.id}.xlsx`);
            setImportStatus(`Exported scenario-lab-${selected.id}.xlsx`);
        } catch (err) {
            setImportStatus(`Export failed: ${err instanceof Error ? err.message : String(err)}`);
        }
    }, [scenarioComparison, scenarioInsights, scenarioVariance, scenarioNarrative, importedActuals, selected.id, selected.siteName]);
    const runScenarioNarrative = useCallback(async (question?: string) => {
        setScenarioNarrativeLoading(true);
        const prompt = buildScenarioPrompt(scenarioComparison, scenarioVariance, "process unit", { insights: scenarioInsights, importedNote, question });
        const offline = () => (question
            ? answerScenarioQuestion(question, { comparison: scenarioComparison, variance: scenarioVariance, insights: scenarioInsights, importedNote }, "process unit")
            : summarizeComparison(scenarioComparison, scenarioVariance, "process unit") + (importedNote ? ` ${importedNote}` : ""));
        try {
            if (isDataAgentConfigured()) {
                const answer = await queryDataAgent(prompt, {
                    selectedUnitId: selected.id,
                    selectedSite: selected.siteName,
                    scenarios: scenarioComparison.scenarios,
                    variance: scenarioVariance,
                    insights: scenarioInsights,
                    importedActuals: importedActuals?.points,
                });
                setScenarioNarrative(answer.summary);
                setScenarioNarrativeSource("fabriciq");
            } else {
                setScenarioNarrative(offline());
                setScenarioNarrativeSource("local");
            }
        } catch {
            setScenarioNarrative(offline());
            setScenarioNarrativeSource("local");
        } finally {
            setScenarioNarrativeLoading(false);
        }
    }, [scenarioComparison, scenarioVariance, scenarioInsights, importedNote, importedActuals, selected.id, selected.siteName]);

    // ---- Mission-ops derived state ---------------------------------------
    const selectedForecast = forecastEscalation(anomalyHistRef.current.get(selected.id) ?? []);
    const suggestedPriority = derivePriority(anomalyScore(selected), selectedForecast.etaToAlarmTicks);
    const suggestedComponent = recommendComponentStable(selected.moduleTempC, selected.inverterLoadPct, componentStickyRef.current[selected.id]);
    componentStickyRef.current[selected.id] = suggestedComponent;
    const suggestedResponders = useMemo(
        () => rankWorkIQResponders(MOCK_WORKIQ_RESPONDERS, selected.siteId, suggestedComponent, suggestedPriority, {
            shift: responderShiftFilter,
            onCallOnly,
            loadByResponder: responderLoad,
        }),
        [onCallOnly, responderLoad, responderShiftFilter, selected.siteId, suggestedComponent, suggestedPriority],
    );
    const primaryResponder = suggestedResponders[0] ?? null;
    const responderAvailability = useMemo(
        () => summarizeResponderAvailability(suggestedResponders.map((r) => ({ shift: r.shift, onCall: r.onCall, currentLoad: r.currentLoad }))),
        [suggestedResponders],
    );
    const techPopupResponder = useMemo(
        () => suggestedResponders.find((r) => r.id === techPopupResponderId) ?? primaryResponder,
        [primaryResponder, suggestedResponders, techPopupResponderId],
    );
    const techPopupFocusComponent = useMemo(
        () => (techPopupResponder?.skills[0] ?? suggestedComponent) as WorkOrderComponent,
        [suggestedComponent, techPopupResponder],
    );
    const matchingEvidence = useMemo(
        () => MOCK_REFINERY_EVIDENCE.filter((ev) => ev.component === suggestedComponent),
        [suggestedComponent],
    );
    const selectedEvidence = useMemo(
        () => matchingEvidence.find((ev) => ev.id === selectedEvidenceId) ?? matchingEvidence[0] ?? null,
        [matchingEvidence, selectedEvidenceId],
    );
    const techPopupEvidence = useMemo(
        () => MOCK_REFINERY_EVIDENCE.find((ev) => ev.component === techPopupFocusComponent) ?? selectedEvidence,
        [selectedEvidence, techPopupFocusComponent],
    );
    const incidentStory = useMemo(() => {
        const lead = primaryResponder ? `${primaryResponder.name} (${primaryResponder.role})` : "No primary responder";
        const evidence = selectedEvidence ? `${selectedEvidence.label} captured ${new Date(selectedEvidence.capturedAt).toLocaleTimeString()}` : "No evidence selected";
        return `Incident detected on ${selected.id}: probable ${suggestedComponent}. Priority ${suggestedPriority}. Lead technician: ${lead}. Evidence: ${evidence}.`;
    }, [primaryResponder, selected.id, selectedEvidence, suggestedComponent, suggestedPriority]);
    const dispatchQuality = useMemo(() => {
        const checks = [
            { label: "Lead technician", ok: Boolean(primaryResponder) },
            { label: "Evidence attached", ok: Boolean(selectedEvidence) },
            { label: "Priority selected", ok: Boolean(suggestedPriority) },
            { label: "Story content", ok: incidentStory.length >= 80 },
        ];
        const passed = checks.filter((c) => c.ok).length;
        return { checks, score: Math.round((passed / checks.length) * 100) };
    }, [incidentStory, primaryResponder, selectedEvidence, suggestedPriority]);
    const escalationThreshold = suggestedPriority === "P1" ? 76 : suggestedPriority === "P2" ? 68 : 60;
    const needsEscalation = suggestedResponders.length === 0 || (suggestedResponders[0]?.score ?? 0) < escalationThreshold;
    const selectedOpenOrder = useMemo(
        () => maintenanceOrders.find((o) => o.turbineId === selected.id && o.status.toLowerCase() !== "closed" && o.status.toLowerCase() !== "resolved"),
        [maintenanceOrders, selected.id],
    );
    const orderAgeMin = selectedOpenOrder ? elapsedMinutesSince(selectedOpenOrder.createdAt) : null;
    const orderPriority = (selectedOpenOrder?.priority as WorkOrderPriority | undefined) ?? suggestedPriority;
    const orderSlaMin = prioritySlaMinutes(orderPriority);
    const slaRemainingMin = orderAgeMin == null ? orderSlaMin : Math.max(0, orderSlaMin - orderAgeMin);
    const isSlaOverdue = orderAgeMin != null && orderAgeMin > orderSlaMin;
    const slaState = slaUrgency(orderAgeMin ?? 0, orderSlaMin);
    const escalationTimeline = buildEscalationTimeline(escalationStage, isSlaOverdue);
    const canEscalateRegional = escalationStage === "manager" && isSlaOverdue;
    const missionChallenge = useMemo(
        () => buildMissionChallenge({
            dispatchQualityScore: dispatchQuality.score,
            hasLeadTechnician: Boolean(primaryResponder),
            hasEvidence: Boolean(selectedEvidence),
            hasOpenOrder: Boolean(selectedOpenOrder),
            escalationStage,
            isSlaOverdue,
            demoRuns: demoRunCount,
        }),
        [demoRunCount, dispatchQuality.score, escalationStage, isSlaOverdue, primaryResponder, selectedEvidence, selectedOpenOrder],
    );

    useEffect(() => {
        if (matchingEvidence.length === 0) {
            return;
        }
        if (!matchingEvidence.some((ev) => ev.id === selectedEvidenceId)) {
            setSelectedEvidenceId(matchingEvidence[0].id);
        }
    }, [matchingEvidence, selectedEvidenceId]);

    useEffect(() => {
        if (!techPopupOpen || !techPopupEvidence) {
            return;
        }
        if (techPopupEvidence.id !== selectedEvidenceId) {
            setSelectedEvidenceId(techPopupEvidence.id);
        }
    }, [selectedEvidenceId, techPopupEvidence, techPopupOpen]);

    const raiseWorkOrder = useCallback(async (assigneeOverride?: string, escalationNote?: string, demoOverride = false) => {
        setWoMessage(null);
        if (!canWriteback && !demoOverride) {
            setWoMessage("Viewer mode — switch to Operator to raise work orders.");
            return false;
        }
        const deltaKwt = Math.round(scenario.energyDeltaKwt);
        const assignee = (assigneeOverride ?? woAssignee).trim() || "unassigned";
        const evidenceNote = selectedEvidence ? `evidence ${selectedEvidence.label}` : "evidence n/a";
        const noteBase = `${suggestedComponent} \u00b7 ${evidenceNote} \u00b7 plan turndown ${simCurtail}% / downtime ${simDowntime}t \u00b7 projected ${deltaKwt.toLocaleString()} kbd\u00b7t`;
        const note = escalationNote ? `${noteBase} \u00b7 ${escalationNote}` : noteBase;
        const order: MaintenanceOrderRecord = {
            turbineId: selected.id,
            siteId: selected.siteId,
            component: suggestedComponent,
            priority: suggestedPriority,
            status: "Open",
            curtailPct: simCurtail,
            downtimeTicks: simDowntime,
            projectedDeltaKwt: deltaKwt,
            assignee,
            note: note.slice(0, 500),
            createdAt: new Date().toISOString(),
        };
        try {
            const saved = await saveMaintenanceOrder(order);
            const ref = saved.id ? ` (id ${saved.id.slice(0, 8)})` : "";
            setWoMessage(`${suggestedPriority} work order raised for ${selected.id} — ${suggestedComponent} \u00b7 ${assignee}${ref}.`);
            setWoAssignee("");
            void loadOrders();
            return true;
        } catch {
            const fallback = JSON.parse(localStorage.getItem("refinery-workorders") ?? "[]") as MaintenanceOrderRecord[];
            fallback.unshift(order);
            localStorage.setItem("refinery-workorders", JSON.stringify(fallback));
            setMaintenanceOrders(fallback);
            setWoMessage(`Backend unreachable. Work order saved locally (${fallback.length}).`);
            return true;
        }
    }, [canWriteback, loadOrders, scenario.energyDeltaKwt, selected.id, selected.siteId, selectedEvidence, simCurtail, simDowntime, suggestedComponent, suggestedPriority, woAssignee]);

    const handleRaiseWorkOrder = useCallback(async () => {
        if (!canWriteback) {
            setOperatorRole("operator");
            setWoMessage("Demo override enabled: switched to Operator for dispatch.");
            await raiseWorkOrder(undefined, undefined, true);
            return;
        }
        await raiseWorkOrder();
    }, [canWriteback, raiseWorkOrder]);

    const handleDispatchResponder = useCallback(async (responder: { id: string; name: string }) => {
        setWoAssignee(responder.name);
        const demoOverride = !canWriteback;
        if (demoOverride) {
            setOperatorRole("operator");
            setWoMessage("Demo override enabled: switched to Operator for responder dispatch.");
        }
        const ok = await raiseWorkOrder(responder.name, undefined, demoOverride);
        if (ok) {
            setResponderLoad((prev) => ({ ...prev, [responder.id]: (prev[responder.id] ?? 0) + 1 }));
            setEscalationStage("none");
        }
    }, [canWriteback, raiseWorkOrder]);

    const handleEscalateManager = useCallback(async (demoOverride = false) => {
        const managerName = "Ops Duty Manager";
        const internalOverride = demoOverride || !canWriteback;
        if (internalOverride && !canWriteback) {
            setOperatorRole("operator");
            setWoMessage("Demo override enabled: switched to Operator for manager escalation.");
        }
        const ok = await raiseWorkOrder(managerName, `Escalation L1: below match threshold / SLA risk for ${selected.id}`, internalOverride);
        if (ok) {
            setEscalationStage("manager");
            setWoMessage((prev) => `${prev ?? ""} Escalated to ${managerName}.`.trim());
        }
    }, [canWriteback, raiseWorkOrder, selected.id]);

    const handleEscalateRegional = useCallback(async (demoOverride = false) => {
        const regionalLead = "Regional Reliability Lead";
        const internalOverride = demoOverride || !canWriteback;
        if (internalOverride && !canWriteback) {
            setOperatorRole("operator");
            setWoMessage("Demo override enabled: switched to Operator for regional escalation.");
        }
        const ok = await raiseWorkOrder(regionalLead, `Escalation L2: unresolved after manager handoff for ${selected.id}`, internalOverride);
        if (ok) {
            setEscalationStage("regional");
            setWoMessage((prev) => `${prev ?? ""} Escalated to ${regionalLead}.`.trim());
        }
    }, [canWriteback, raiseWorkOrder, selected.id]);

    // Close the loop: confirm resolution of the current open order (distinct from
    // dispatch). If nothing was dispatched yet, there is no loop to close, so we
    // escalate for review instead of silently doing nothing.
    const handleCloseLoop = useCallback(async () => {
        if (!selectedOpenOrder) {
            setWoMessage("Close the loop: no open order to resolve — escalating for review.");
            await handleEscalateManager(!canWriteback);
            return false;
        }
        if (!canWriteback) {
            setOperatorRole("operator");
            setWoMessage("Close the loop demo override: switched to Operator.");
        }
        const resolution: MaintenanceOrderRecord = {
            turbineId: selectedOpenOrder.turbineId,
            siteId: selectedOpenOrder.siteId,
            component: selectedOpenOrder.component,
            priority: selectedOpenOrder.priority,
            status: "Resolved",
            curtailPct: selectedOpenOrder.curtailPct ?? simCurtail,
            downtimeTicks: selectedOpenOrder.downtimeTicks ?? simDowntime,
            projectedDeltaKwt: selectedOpenOrder.projectedDeltaKwt ?? 0,
            assignee: selectedOpenOrder.assignee ?? "unassigned",
            note: `Resolution confirmed — recovery verified for ${selectedOpenOrder.turbineId}.`,
            createdAt: new Date().toISOString(),
        };
        try {
            await saveMaintenanceOrder(resolution);
            setEscalationStage("none");
            setWoMessage(`Loop closed — ${selectedOpenOrder.turbineId} order marked Resolved.`);
            void loadOrders();
            return true;
        } catch {
            setWoMessage("Close the loop: backend unreachable, resolution not persisted.");
            return false;
        }
    }, [canWriteback, handleEscalateManager, loadOrders, selectedOpenOrder, simCurtail, simDowntime]);

    const handleRunDispatchQualityCheck = useCallback(() => {
        const missing = dispatchQuality.checks.filter((c) => !c.ok).map((c) => c.label);
        if (missing.length === 0) {
            setWoMessage(`Dispatch Quality Tool: READY (${dispatchQuality.score}%).`);
            return;
        }
        setWoMessage(`Dispatch Quality Tool: MISSING -> ${missing.join(", ")} (${dispatchQuality.score}%).`);
    }, [dispatchQuality]);

    // ---- Guided demo orchestration ---------------------------------------
    const handlePrimeDemoStory = useCallback(() => {
        const targetCurtail = suggestedPriority === "P1" ? 22 : suggestedPriority === "P2" ? 14 : 8;
        const targetDowntime = suggestedPriority === "P1" ? 6 : suggestedPriority === "P2" ? 4 : 2;
        setSimCurtail(targetCurtail);
        setSimDowntime(targetDowntime);
        if (simHorizon < 12) {
            setSimHorizon(12);
        }
        setResponderShiftFilter("all");
        setOnCallOnly(true);
        setEscalationStage("none");
        if (matchingEvidence[0]) {
            setSelectedEvidenceId(matchingEvidence[0].id);
        }
        const lead = suggestedResponders[0] ?? null;
        if (lead) {
            setWoAssignee(lead.name);
            setTechPopupResponderId(lead.id);
            setTechPopupOpen(true);
        }
        setWoMessage(`Demo story primed for ${selected.id}: ${suggestedComponent}, ${suggestedPriority}, ${lead ? lead.name : "no lead"}.`);
    }, [matchingEvidence, selected.id, simHorizon, suggestedComponent, suggestedPriority, suggestedResponders]);

    const handleAutoHealNow = useCallback(async () => {
        const lead = suggestedResponders[0] ?? null;
        if (!lead) {
            setWoMessage("AutoHeal: no responder available in current roster.");
            return false;
        }
        if (!canWriteback) {
            setOperatorRole("operator");
            setWoMessage("AutoHeal demo override: switched to Operator.");
        }
        if ((lead.score ?? 0) < escalationThreshold) {
            await handleEscalateManager(!canWriteback);
            return true;
        }
        await handleDispatchResponder(lead);
        return true;
    }, [canWriteback, escalationThreshold, handleDispatchResponder, handleEscalateManager, suggestedResponders]);

    const handleCallFieldSupport = useCallback(async () => {
        setWoMessage(`Field support contacted — on-site crew dispatched to ${selected.id} (${selected.siteName}).`);
        return handleCloseLoop();
    }, [handleCloseLoop, selected.id, selected.siteName]);

    // Visibly "click" a few graph nodes during the graph step: highlight the current
    // unit, then two peers (same site when possible, else other units), then restore
    // the original selection so downstream steps act on the right unit.
    const cycleGraphSelection = useCallback(async (dwellMs: number) => {
        const delay = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));
        const originalId = selected.id;
        const others = turbines.filter((t) => t.id !== originalId);
        const sameSite = others.filter((t) => t.siteId === selected.siteId);
        const peers = (sameSite.length >= 2 ? sameSite : others).slice(0, 2);
        const cycleIds = [originalId, ...peers.map((t) => t.id), originalId];
        const stepMs = Math.max(1200, Math.floor(dwellMs / cycleIds.length));
        for (let i = 0; i < cycleIds.length; i += 1) {
            const id = cycleIds[i];
            setSelectedId(id);
            const t = turbines.find((x) => x.id === id);
            if (t) {
                setWoMessage(i === 0
                    ? `Graph: drilled into incident node ${id} (${t.status}) — assets & sensors expanded.`
                    : `Graph: tracing related node ${id} — same refinery ${t.siteName}.`);
            }
            await delay(stepMs);
        }
        setSelectedId(originalId);
        setWoMessage(`Graph: back on incident node ${originalId} — reviewing its asset tree.`);
    }, [selected.id, selected.siteId, turbines]);

    const handleAutoRunDemo = useCallback(async () => {
        if (autoPlayRunning) {
            return;
        }
        setAutoPlayRunning(true);
        setDemoIntroOpen(false);
        setDemoPanelOpen(false);
        const dwellMs = 10000;
        const preActionMs = 5000;
        const delay = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));
        const logStep = (step: string, detail: string) =>
            setDemoRunLog((log) => [...log, { step, at: new Date().toISOString(), detail }]);
        try {
            // 1 — Frame the incident on the fleet map (reset any active filters).
            setDemoScriptStep("story");
            setDemoStepIndex(0);
            setDemoRunLog([{ step: "story", at: new Date().toISOString(), detail: "Framed the incident" }]);
            setSiteFilter("all");
            setStatusFilter("all");
            setGraphFilter("all");
            setView("map");
            handlePrimeDemoStory();
            setTechPopupOpen(false);
            await delay(dwellMs);
            // 2 — Locate: show the map first, then filter down to the affected site.
            setDemoScriptStep("locate");
            setDemoStepIndex(1);
            setView("map");
            setTechPopupOpen(false);
            await delay(preActionMs);
            setSiteFilter(selected.siteId);
            setWoMessage(`Fleet map filtered to ${selected.siteName} — focused on ${selected.id}.`);
            logStep("locate", `Filtered map to refinery ${selected.siteName} and focused ${selected.id}`);
            await delay(dwellMs - preActionMs);
            // 3 — Inspect the digital twin: overall view first, then focus a process asset.
            setDemoScriptStep("twin");
            setDemoStepIndex(2);
            setView("twin");
            setTwinTab("overview");
            setDemoFocusPart(null);
            setWoMessage(`Digital twin: overall view of ${selected.id}.`);
            await delay(preActionMs);
            setDemoFocusPart("array");
            setWoMessage(`Digital twin: inspecting the column on ${selected.id} — reading asset & sensor signals.`);
            logStep("twin", "Overall view → focused the column asset and its sensors");
            await delay(dwellMs - preActionMs);
            // 4 — Analyze the ontology graph: show it first, then filter/traverse.
            setDemoScriptStep("graph");
            setDemoStepIndex(3);
            setView("graph");
            logStep("graph", `Analyzed the ontology graph (filter: ${selected.status})`);
            await delay(preActionMs);
            setGraphFilter(selected.status === "alarm" ? "alarm" : selected.status === "warning" ? "warning" : "all");
            await cycleGraphSelection(dwellMs - preActionMs);
            // 5 — Take action: show operations first, then launch the dispatch popup.
            setDemoScriptStep("dispatch");
            setDemoStepIndex(4);
            goToOps();
            setTechPopupOpen(false);
            await delay(preActionMs);
            setTechPopupOpen(true);
            const dispatched = await handleAutoHealNow();
            if (!dispatched) {
                await handleRaiseWorkOrder();
            }
            logStep("dispatch", "Opened dispatch popup and raised the work order");
            await delay(dwellMs - preActionMs);
            // 6 — Call field support, close the loop, and reset filters.
            setDemoScriptStep("support");
            setDemoStepIndex(5);
            setTechPopupOpen(false);
            goToOps();
            const closed = await handleCallFieldSupport();
            setSiteFilter("all");
            setGraphFilter("all");
            logStep("support", closed ? "Field support called — loop closed" : "Field support called — escalated for review");
            await delay(dwellMs);
            // 7 — Ask Fabric IQ: click Ask and read the recommendation.
            setDemoScriptStep("ask");
            setDemoStepIndex(6);
            setTechPopupOpen(false);
            setView("ask");
            const askPrompt = `Which units are at highest risk right now, and what should we prioritize for ${selected.siteName}?`;
            setQuestion(askPrompt);
            setWoMessage("Fabric IQ: submitting the prioritization question…");
            await runAskRef.current(askPrompt);
            logStep("ask", "Asked Fabric IQ for prioritization");
            setWoMessage("Fabric IQ answered — visualizing the trend in Analytics next.");
            await delay(dwellMs);
            // 8 — Analytics: visualize the trends behind the answer, then open the report.
            setDemoScriptStep("analytics");
            setDemoStepIndex(7);
            setView("analytics");
            setWoMessage("Analytics: throughput, deltas and the operating curve behind the recommendation.");
            logStep("analytics", "Reviewed performance analytics behind the answer");
            await delay(dwellMs);
            missionReportRef.current();
            logStep("analytics", "Opened the mission report");
            await delay(preActionMs);
            setWoMessage((prev) => `${prev ?? ""} Demo script executed.`.trim());
            setDemoRunCount((count) => count + 1);
        } finally {
            await delay(1500);
            setAutoPlayRunning(false);
            setDemoScriptStep("idle");
            setDemoFocusPart(null);
        }
    }, [autoPlayRunning, cycleGraphSelection, handleAutoHealNow, handleCallFieldSupport, handlePrimeDemoStory, handleRaiseWorkOrder, selected.id, selected.siteId, selected.siteName, selected.status]);

    const demoScriptLead = techPopupResponder ?? primaryResponder ?? suggestedResponders[0] ?? null;
    const demoScriptSteps = useMemo<DemoScriptStep[]>(() => [
        {
            id: "story",
            label: "1. Frame incident",
            detail: "Prime the incident narrative and choose the lead engineer.",
            action: () => {
                setDemoScriptStep("story");
                setView("map");
                handlePrimeDemoStory();
            },
        },
        {
            id: "locate",
            label: "2. Locate on map",
            detail: "Filter the fleet map to the affected refinery.",
            action: () => {
                setDemoScriptStep("locate");
                setView("map");
                setSiteFilter(selected.siteId);
                setWoMessage(`Fleet map filtered to ${selected.siteName} — focused on ${selected.id}.`);
            },
        },
        {
            id: "twin",
            label: "3. Digital twin",
            detail: "Open the twin, then focus the column asset.",
            action: async () => {
                setDemoScriptStep("twin");
                setView("twin");
                setTwinTab("overview");
                setDemoFocusPart(null);
                await new Promise<void>((resolve) => window.setTimeout(resolve, 2500));
                setDemoFocusPart("array");
            },
        },
        {
            id: "graph",
            label: "4. Ontology graph",
            detail: "Filter the ontology graph to the unit's severity.",
            action: async () => {
                setDemoScriptStep("graph");
                setGraphFilter(selected.status === "alarm" ? "alarm" : selected.status === "warning" ? "warning" : "all");
                setView("graph");
                await cycleGraphSelection(8000);
            },
        },
        {
            id: "dispatch",
            label: "5. Dispatch lead",
            detail: "Open the dispatch popup and send the selected responder.",
            action: async () => {
                setDemoScriptStep("dispatch");
                goToOps();
                setTechPopupOpen(true);
                if (!demoScriptLead) {
                    const ok = await handleAutoHealNow();
                    if (!ok) {
                        await handleRaiseWorkOrder();
                    }
                    return;
                }
                await handleDispatchResponder(demoScriptLead);
            },
        },
        {
            id: "support",
            label: "6. Field support",
            detail: "Call on-site field support and close the loop.",
            action: async () => {
                setDemoScriptStep("support");
                goToOps();
                setTechPopupOpen(false);
                await handleCallFieldSupport();
            },
        },
        {
            id: "ask",
            label: "7. Ask Fabric IQ",
            detail: "Ask a prioritization question and read the recommendation.",
            action: async () => {
                setDemoScriptStep("ask");
                setTechPopupOpen(false);
                setView("ask");
                const prompt = `Which units are at highest risk right now, and what should we prioritize for ${selected.siteName}?`;
                setQuestion(prompt);
                await runAskRef.current(prompt);
            },
        },
        {
            id: "analytics",
            label: "8. Analytics & report",
            detail: "Visualize the trend in Analytics, then open the mission report.",
            action: async () => {
                setDemoScriptStep("analytics");
                setTechPopupOpen(false);
                setView("analytics");
                await new Promise<void>((resolve) => window.setTimeout(resolve, 1500));
                missionReportRef.current();
            },
        },
    ], [demoScriptLead, cycleGraphSelection, handleAutoHealNow, handleCallFieldSupport, handleDispatchResponder, handlePrimeDemoStory, handleRaiseWorkOrder, selected.id, selected.siteId, selected.siteName, selected.status]);

    const handleStartDemoFromIntro = useCallback(() => {
        setDemoIntroOpen(false);
        void handleAutoRunDemo();
    }, [handleAutoRunDemo]);

    const recordMissionRun = useCallback((report: MissionReport) => {
        setRunHistory((history) => pushMissionRun(history, report, 10));
    }, []);

    const buildDemoReport = useCallback((): MissionReport => buildMissionReport({
        turbineId: selected.id,
        siteName: selected.siteName,
        component: suggestedComponent,
        priority: suggestedPriority,
        responder: primaryResponder?.name ?? null,
        dispatchQualityScore: dispatchQuality.score,
        challengeScore: missionChallenge.score,
        challengeVerdict: missionChallenge.verdict,
        events: demoRunLog.length > 0 ? demoRunLog : [{ step: "manual", at: new Date().toISOString(), detail: "No scripted run recorded yet" }],
        outcome: selectedOpenOrder ? `Open order (${selectedOpenOrder.priority})` : "No open order",
    }), [demoRunLog, dispatchQuality.score, missionChallenge.score, missionChallenge.verdict, primaryResponder, selected.id, selected.siteName, selectedOpenOrder, suggestedComponent, suggestedPriority]);

    const handleOpenMissionReport = useCallback(() => {
        const report = buildDemoReport();
        recordMissionRun(report);
        setReportModal(report);
        setWoMessage(`Mission report ready (${report.stepCount} steps, ${(report.durationMs / 1000).toFixed(1)}s).`);
    }, [buildDemoReport, recordMissionRun]);

    const handleDownloadMissionReport = useCallback(() => {
        const report = buildDemoReport();
        recordMissionRun(report);
        const blob = new Blob([JSON.stringify(report, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `mission-report-${selected.id}-${Date.now()}.json`;
        a.click();
        URL.revokeObjectURL(url);
        setWoMessage(`Mission report exported (${report.stepCount} steps, ${(report.durationMs / 1000).toFixed(1)}s).`);
    }, [buildDemoReport, recordMissionRun, selected.id]);

    // Persist run history and keep the report/ask refs current for the orchestrator.
    useEffect(() => {
        localStorage.setItem("refinery-run-history", JSON.stringify(runHistory.slice(0, 10)));
    }, [runHistory]);

    useEffect(() => {
        missionReportRef.current = handleOpenMissionReport;
    }, [handleOpenMissionReport]);

    // Compile a mission report automatically once an auto-run finishes.
    const autoPlayPrevRef = useRef(false);
    useEffect(() => {
        const finished = autoPlayPrevRef.current && !autoPlayRunning;
        autoPlayPrevRef.current = autoPlayRunning;
        if (!finished || demoRunLog.length === 0) {
            return;
        }
        const report = buildMissionReport({
            turbineId: selected.id,
            siteName: selected.siteName,
            component: suggestedComponent,
            priority: suggestedPriority,
            responder: primaryResponder?.name ?? null,
            dispatchQualityScore: dispatchQuality.score,
            challengeScore: missionChallenge.score,
            challengeVerdict: missionChallenge.verdict,
            events: demoRunLog,
            outcome: selectedOpenOrder ? `Open order (${selectedOpenOrder.priority})` : "No open order",
        });
        recordMissionRun(report);
    }, [autoPlayRunning, demoRunLog, dispatchQuality.score, missionChallenge.score, missionChallenge.verdict, primaryResponder, recordMissionRun, selected.id, selected.siteName, selectedOpenOrder, suggestedComponent, suggestedPriority]);

    // Demo keyboard shortcuts: ← / → step, Enter run step, Esc close — gated on the panel.
    useEffect(() => {
        if (!demoPanelOpen) {
            return;
        }
        const onKey = (e: KeyboardEvent) => {
            const target = e.target as HTMLElement | null;
            const typing = !!target && /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName);
            if (e.key === "Escape") {
                setDemoPanelOpen(false);
                return;
            }
            if (typing || autoPlayRunning) {
                return;
            }
            if (e.key === "ArrowLeft") {
                e.preventDefault();
                setDemoStepIndex((i) => Math.max(0, i - 1));
            } else if (e.key === "ArrowRight") {
                e.preventDefault();
                setDemoStepIndex((i) => Math.min(demoScriptSteps.length - 1, i + 1));
            } else if (e.key === "Enter") {
                e.preventDefault();
                void demoScriptSteps[demoStepIndex]?.action();
            }
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [autoPlayRunning, demoPanelOpen, demoScriptSteps, demoStepIndex]);

    const demoNarration = demoScriptStep !== "idle" ? narrateStep(REFINERY_DEMO_MANIFEST, demoScriptStep) : null;
    const runHistorySummaries = useMemo(() => runHistory.map((r) => summarizeMissionRun(r)), [runHistory]);

    // ---- Twin device graph admin -----------------------------------------
    const twinDeviceDefinitions = useMemo(() => (focusedTwinPart ? twinDeviceGraph[focusedTwinPart] : []), [focusedTwinPart, twinDeviceGraph]);
    const twinDeviceLayer = useMemo(() => {
        if (!focusedTwinPart || !focusedTwinDevice) {
            return null;
        }
        const def = twinDeviceDefinitions.find((d) => d.key === focusedTwinDevice) ?? null;
        if (!def) {
            return null;
        }
        return { ...def, status: def.status(selected), value: def.value(selected) };
    }, [focusedTwinDevice, focusedTwinPart, selected, twinDeviceDefinitions]);

    const handleTwinDraftChange = useCallback((patch: Partial<RefineryDeviceDraft>) => {
        setDeviceDraft((prev) => (prev ? { ...prev, ...patch } : prev));
        setDeviceDraftDirty(true);
    }, []);

    const handleTwinDraftReset = useCallback(() => {
        if (!focusedTwinPart || !focusedTwinDevice) {
            return;
        }
        const componentDevices = twinDeviceGraph[focusedTwinPart] ?? [];
        const node = componentDevices.find((d) => d.key === focusedTwinDevice);
        if (!node) {
            return;
        }
        const row = twinDeviceRows.find((r) => r.deviceKey === focusedTwinDevice);
        const sortOrder = row?.sortOrder ?? componentDevices.findIndex((d) => d.key === focusedTwinDevice);
        setDeviceDraft(draftFromUnitDevice(node, sortOrder));
        setDeviceDraftDirty(false);
        setDeviceSaveMessage("Editor reset to persisted values.");
    }, [focusedTwinDevice, focusedTwinPart, twinDeviceGraph, twinDeviceRows]);

    const persistTwinRows = useCallback((rows: UnitDeviceRecord[]) => {
        setTwinDeviceRows(rows);
        setTwinDeviceGraph(mergeUnitDeviceGraph(rows, UNIT_COMPONENT_DEVICES));
    }, []);

    const handleTwinDraftSave = useCallback(async () => {
        if (!focusedTwinPart || !focusedTwinDevice || !deviceDraft) {
            return;
        }
        const componentDevices = twinDeviceGraph[focusedTwinPart] ?? [];
        const node = componentDevices.find((d) => d.key === focusedTwinDevice);
        if (!node) {
            return;
        }
        const row = twinDeviceRows.find((r) => r.deviceKey === focusedTwinDevice);
        setDeviceSaveBusy(true);
        setDeviceSaveMessage(null);
        try {
            const normalized = normalizeUnitDeviceDraft(deviceDraft, {
                zoom: node.zoom,
                sortOrder: row?.sortOrder ?? componentDevices.findIndex((d) => d.key === focusedTwinDevice),
                anchor: node.anchor,
                lookAt: node.lookAt,
                offset: node.offset,
            });
            const patch: Partial<Omit<UnitDeviceRecord, "id" | "deviceKey">> = {
                label: normalized.label,
                property: normalized.property,
                unit: normalized.unit,
                note: normalized.note,
                zoom: normalized.zoom,
                sortOrder: normalized.sortOrder,
                anchorX: normalized.anchor[0], anchorY: normalized.anchor[1], anchorZ: normalized.anchor[2],
                lookAtX: normalized.lookAt[0], lookAtY: normalized.lookAt[1], lookAtZ: normalized.lookAt[2],
                offsetX: normalized.offset[0], offsetY: normalized.offset[1], offsetZ: normalized.offset[2],
            };
            await updateUnitDevice({ id: row?.id, deviceKey: focusedTwinDevice }, patch);
            const rows = await listUnitDevices();
            if (rows.length > 0) {
                persistTwinRows(rows);
            }
            setDeviceDraftDirty(false);
            setDeviceSaveMessage("Twin graph device saved to backend.");
        } catch {
            setDeviceSaveMessage("Backend unreachable — device changes not persisted.");
        } finally {
            setDeviceSaveBusy(false);
        }
    }, [deviceDraft, focusedTwinDevice, focusedTwinPart, persistTwinRows, twinDeviceGraph, twinDeviceRows]);

    const handleTwinAddSibling = useCallback(async () => {
        if (!focusedTwinPart || !focusedTwinDevice) {
            return;
        }
        const base = twinDeviceDefinitions.find((d) => d.key === focusedTwinDevice);
        if (!base) {
            return;
        }
        const suffix = Math.floor(Date.now() / 1000).toString(36);
        const newKey = `${focusedTwinPart}.${suffix}`;
        const maxSort = twinDeviceRows.reduce((m, r) => Math.max(m, r.sortOrder), 0);
        setDeviceSaveBusy(true);
        setDeviceSaveMessage(null);
        try {
            await createUnitDevice({
                deviceKey: newKey,
                component: focusedTwinPart,
                label: `${base.label} copy`,
                property: base.property,
                unit: base.unit,
                note: `${base.note} (new)`,
                anchorX: base.anchor[0], anchorY: base.anchor[1], anchorZ: base.anchor[2],
                lookAtX: base.lookAt[0], lookAtY: base.lookAt[1], lookAtZ: base.lookAt[2],
                offsetX: base.offset[0], offsetY: base.offset[1], offsetZ: base.offset[2],
                zoom: base.zoom,
                sortOrder: maxSort + 1,
            });
            const rows = await listUnitDevices();
            if (rows.length > 0) {
                persistTwinRows(rows);
            }
            setFocusedTwinDevice(newKey);
            setDeviceSaveMessage("Sibling device created.");
        } catch {
            setDeviceSaveMessage("Backend unreachable — sibling device not created.");
        } finally {
            setDeviceSaveBusy(false);
        }
    }, [focusedTwinDevice, focusedTwinPart, persistTwinRows, twinDeviceDefinitions, twinDeviceRows]);

    const handleTwinDeleteDevice = useCallback(async () => {
        if (!focusedTwinPart || !focusedTwinDevice) {
            return;
        }
        const componentCount = twinDeviceGraph[focusedTwinPart]?.length ?? 0;
        if (componentCount <= 1) {
            setDeviceSaveMessage("At least one child device must remain for this asset.");
            return;
        }
        const row = twinDeviceRows.find((r) => r.deviceKey === focusedTwinDevice);
        setDeviceSaveBusy(true);
        setDeviceSaveMessage(null);
        try {
            await deleteUnitDevice({ id: row?.id, deviceKey: focusedTwinDevice });
            const rows = await listUnitDevices();
            if (rows.length > 0) {
                persistTwinRows(rows);
            }
            const nextDevice = rows.find((r) => r.component === focusedTwinPart)?.deviceKey ?? null;
            setFocusedTwinDevice(nextDevice);
            setDeviceSaveMessage("Device deleted from backend graph.");
        } catch {
            setDeviceSaveMessage("Backend unreachable — device not deleted.");
        } finally {
            setDeviceSaveBusy(false);
        }
    }, [focusedTwinDevice, focusedTwinPart, persistTwinRows, twinDeviceGraph, twinDeviceRows]);

    const runAsk = useCallback(async (override?: string) => {
        setAskLoading(true);
        setAskError(null);
        try {
            const trimmed = (override ?? question).trim();
            if (!trimmed) {
                throw new Error("Please enter a question.");
            }
            const started = performance.now();

            const alarmsNow = turbines.filter((t) => t.status === "alarm").length;
            const warningsNow = turbines.filter((t) => t.status === "warning").length;
            const fleetKwNow = turbines.reduce((sum, t) => sum + t.powerKw, 0);
            const signature = `${turbines.length}:${alarmsNow}:${warningsNow}:${Math.round(fleetKwNow / 50)}:${selected.id}`;
            const cacheKey = `${normalizeAskQuestion(trimmed)}|${signature}`;
            const now = Date.now();
            const cached = askCacheRef.current.get(cacheKey);
            if (cached && now - cached.at <= ASK_CACHE_TTL_MS) {
                const cachedResult = { ...cached.result, cacheHit: true };
                setAskResult(cachedResult);
                setAskHistory((prev) => [{
                    at: new Date().toISOString(),
                    question: trimmed,
                    source: cachedResult.source,
                    transport: cachedResult.transport,
                    latencyMs: Math.max(0, Math.round(performance.now() - started)),
                    cacheHit: true,
                    fallbackReason: cachedResult.fallbackReason,
                }, ...prev].slice(0, ASK_HISTORY_MAX));
                return;
            }

            const topAlerts = [...turbines]
                .sort((a, b) => anomalyScore(b) - anomalyScore(a))
                .slice(0, 8)
                .map((t) => ({
                    id: t.id,
                    siteId: t.siteId,
                    status: t.status,
                    powerKw: t.powerKw,
                    moduleTempC: t.moduleTempC,
                    inverterLoadPct: t.inverterLoadPct,
                }));
            const topOutput = [...turbines]
                .sort((a, b) => b.powerKw - a.powerKw)
                .slice(0, 8)
                .map((t) => ({ id: t.id, siteId: t.siteId, powerKw: t.powerKw, feedRateKbd: t.irradianceWm2 }));

            const result = await askFabricIQ(trimmed, {
                selectedTurbineId: selected.id,
                selectedSite: selected.siteName,
                fleet: {
                    units: turbines.length,
                    alarms: alarmsNow,
                    warnings: warningsNow,
                    totalKbd: Number(fleetKwNow.toFixed(1)),
                },
                topAlerts,
                topOutput,
                telemetry: turbines.slice(0, 24).map((t) => ({
                    id: t.id,
                    siteId: t.siteId,
                    siteName: t.siteName,
                    powerKw: t.powerKw,
                    irradianceWm2: t.irradianceWm2,
                    moduleTempC: t.moduleTempC,
                    inverterLoadPct: t.inverterLoadPct,
                    status: t.status,
                })),
            });

            const storable = { ...result, cacheHit: false };
            askCacheRef.current.set(cacheKey, { at: now, result: storable });
            if (askCacheRef.current.size > 80) {
                const oldest = askCacheRef.current.keys().next().value as string | undefined;
                if (oldest) {
                    askCacheRef.current.delete(oldest);
                }
            }
            setAskResult(storable);
            setAskHistory((prev) => [{
                at: new Date().toISOString(),
                question: trimmed,
                source: storable.source,
                transport: storable.transport,
                latencyMs: Math.max(0, Math.round(performance.now() - started)),
                cacheHit: false,
                fallbackReason: storable.fallbackReason,
            }, ...prev].slice(0, ASK_HISTORY_MAX));
        } catch (err) {
            setAskError(err instanceof Error ? err.message : String(err));
        } finally {
            setAskLoading(false);
        }
    }, [question, selected.id, selected.siteName, turbines]);

    useEffect(() => {
        runAskRef.current = runAsk;
    }, [runAsk]);

    const runAgentConnectionCheck = useCallback(async () => {
        setAgentCheckLoading(true);
        try {
            const result = await testDataAgentConnection({
                selectedUnitId: selected.id,
                selectedSite: selected.siteName,
            });
            setAgentCheckResult(result);
            if (!result.ok) {
                setAskError(result.message);
            }
        } finally {
            setAgentCheckLoading(false);
        }
    }, [selected.id, selected.siteName]);

    const handleWriteback = useCallback(async () => {
        setWritebackMessage(null);
        if (!canWriteback) {
            setWritebackMessage("Viewer mode — switch to Operator to write dispatch actions.");
            return;
        }
        const parsed = Number(wbSetpoint);
        const setpointKw =
            wbSetpoint.trim() === "" || Number.isNaN(parsed) ? selected.powerKw : Math.max(0, Math.round(parsed));
        const composedNote = `[${wbAction}] setpoint=${setpointKw}kW forecast(${forecastHorizon}t)=${forecast}kW${
            wbNote.trim() ? ` · ${wbNote.trim()}` : ""
        }`.slice(0, 500);
        try {
            const saved = await saveDispatchNote({
                turbineId: selected.id,
                siteId: selected.siteId,
                status: selected.status,
                powerKw: setpointKw,
                note: composedNote,
                author: "operator",
                createdAt: new Date().toISOString(),
            });
            const ref = saved.id ? ` (id ${saved.id.slice(0, 8)})` : "";
            setWritebackMessage(
                `${wbAction} written: setpoint ${setpointKw.toLocaleString()} kbd, forecast ${forecast.toLocaleString()} kbd${ref}.`,
            );
            setWbNote("");
            void loadNotes();
        } catch {
            const fallback = JSON.parse(localStorage.getItem("refinery-writeback-log") ?? "[]") as unknown[];
            fallback.push({
                turbineId: selected.id,
                siteId: selected.siteId,
                status: selected.status,
                powerKw: setpointKw,
                note: composedNote,
                createdAt: new Date().toISOString(),
            });
            localStorage.setItem("refinery-writeback-log", JSON.stringify(fallback));
            setWritebackMessage(`Backend unreachable. Saved locally (${fallback.length} records).`);
        }
    }, [
        canWriteback,
        forecast,
        forecastHorizon,
        loadNotes,
        selected.id,
        selected.powerKw,
        selected.siteId,
        selected.status,
        wbAction,
        wbNote,
        wbSetpoint,
    ]);

    const fleetRatedMw = SITES.reduce((s, x) => s + x.capacityMw, 0);
    const capacityFactor = fleetRatedMw > 0 ? (fleetPower / fleetRatedMw) * 100 : 0;
    const avgWind = visibleTurbines.length ? visibleTurbines.reduce((s, t) => s + t.irradianceWm2, 0) / visibleTurbines.length : 0;
    const avgTemp = visibleTurbines.length ? visibleTurbines.reduce((s, t) => s + t.moduleTempC, 0) / visibleTurbines.length : 0;
    const outputBars = sitesSummary.map((s, i) => ({
        label: s.name,
        value: s.sitePowerMw,
        display: `${s.sitePowerMw.toFixed(0)} kbd`,
        color: SITE_COLORS[i % SITE_COLORS.length],
    }));
    const topBars = topPerformers.map((t) => ({
        label: t.id,
        value: t.powerKw,
        display: `${t.powerKw.toLocaleString()} kbd`,
        color: STATUS_COLORS[t.status],
    }));
    const powerCurve = visibleTurbines.map((t) => ({
        x: t.irradianceWm2,
        y: t.powerKw,
        color: STATUS_COLORS[t.status],
        label: `${t.id} \u00b7 ${t.irradianceWm2} kbd feed \u00b7 ${t.powerKw.toLocaleString()} kbd \u00b7 ${t.status}`,
    }));
    const curvePowerMax = Math.max(60, ...powerCurve.map((p) => p.y));
    const curveFeedMax = Math.max(60, ...powerCurve.map((p) => p.x));

    const selectedSite = sites.find((s) => s.id === selected.siteId);
    const relatedNotes = notes.filter((n) => n.turbineId === selected.id);

    const toolbar = (
        <div className="flex flex-wrap items-center gap-2 text-xs">
            <label className="flex items-center gap-1 rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-300">
                <span>Mode</span>
                <select
                    value={operatorRole}
                    onChange={(e) => setOperatorRole(normalizeOperatorRole(e.target.value))}
                    className="bg-transparent text-slate-100 outline-none"
                >
                    <option value="operator">Operator</option>
                    <option value="viewer">Viewer</option>
                </select>
            </label>

            <select
                value={siteFilter}
                onChange={(e) => setSiteFilter(e.target.value)}
                aria-label="Filter by site"
                className="rounded border border-slate-700 bg-[#08142a] px-2 py-1"
            >
                <option value="all">All sites</option>
                {sites.map((s) => (
                    <option key={s.id} value={s.id}>{s.name}</option>
                ))}
            </select>

            <div role="group" aria-label="Filter by status" className="flex overflow-hidden rounded border border-slate-700">
                {(["all", "healthy", "warning", "alarm"] as StatusFilter[]).map((s) => (
                    <button
                        key={s}
                        type="button"
                        onClick={() => setStatusFilter(s)}
                        aria-pressed={statusFilter === s}
                        className={`px-2 py-1 capitalize ${statusFilter === s ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300"}`}
                    >
                        {s}
                    </button>
                ))}
            </div>

            <div className="flex items-center gap-1">
                <input
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    onKeyDown={(e) => { if (e.key === "Enter") handleSearch(); }}
                    placeholder="Find unit…"
                    aria-label="Find unit by id"
                    className="w-32 rounded border border-slate-700 bg-[#08142a] px-2 py-1"
                />
                <button type="button" onClick={handleSearch} className="rounded bg-slate-700 px-2 py-1 text-white">Go</button>
            </div>

            <button type="button" onClick={handleExport} className="rounded bg-slate-700 px-2 py-1 text-white">⬇ CSV</button>

            {(siteFilter !== "all" || statusFilter !== "all") && (
                <button
                    type="button"
                    onClick={() => { setSiteFilter("all"); setStatusFilter("all"); }}
                    className="rounded bg-slate-700/80 px-2 py-1 text-white"
                >
                    Clear
                </button>
            )}
        </div>
    );

    return (
        <main className="flex h-full flex-col bg-[radial-gradient(circle_at_12%_8%,#13223e_0%,#050911_55%,#02050a_100%)] text-slate-100">
            <header className="flex flex-wrap items-center gap-3 border-b border-slate-800/60 bg-[#071126cc] px-3 py-3 backdrop-blur-sm sm:px-5">
                <div className="mr-2">
                    <p className="text-[11px] uppercase tracking-[0.2em] text-cyan-300">Fabric Rayfin App</p>
                    <h1 className="text-xl font-semibold leading-tight">Geo Refinery Twin Command Center · Worldwide</h1>
                    <p className="mt-0.5 text-[10px] text-amber-300/80" title="Live fleet telemetry is simulated in-browser; ontology sites and dispatch notes persist to the Fabric Rayfin backend.">
                        ◐ Simulated telemetry · ontology + dispatch notes persisted to Fabric
                    </p>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                    <KpiPill label="Fleet" value={`${fleetPower.toLocaleString()} kbd`} color="#6ee7ff" />
                    <KpiPill label="Healthy" value={String(healthy)} color={STATUS_COLORS.healthy} />
                    <KpiPill label="Warning" value={String(warnings)} color={STATUS_COLORS.warning} />
                    <KpiPill label="Alarm" value={String(alarms)} color={STATUS_COLORS.alarm} />
                    <KpiPill label="Units" value={`${visibleTurbines.length}/${turbines.length}`} />
                </div>

                <div className="ml-auto flex w-full flex-wrap items-center justify-end gap-2 text-xs text-slate-300 sm:w-auto">
                    <div className="relative">
                        <button
                            type="button"
                            onClick={() => setDemoPanelOpen((v) => !v)}
                            className={`flex items-center gap-1.5 rounded border px-2.5 py-1.5 font-semibold ${demoPanelOpen || autoPlayRunning ? "border-cyan-400 bg-cyan-600/25 text-cyan-100" : "border-cyan-700/50 bg-[#08182c] text-cyan-200 hover:bg-cyan-900/30"}`}
                            aria-expanded={demoPanelOpen}
                            title="Guided demo walkthrough"
                        >
                            🎬 {autoPlayRunning ? "Running…" : "Guided demo"}
                        </button>
                        {demoPanelOpen && (
                            <div className="absolute right-0 top-full z-40 mt-2 w-80 rounded-lg border border-slate-700 bg-[#07142a] p-3 text-left shadow-[0_18px_50px_rgba(0,0,0,0.6)]">
                                <div className="flex items-center justify-between">
                                    <p className="text-sm font-semibold text-cyan-200">Guided demo</p>
                                    <button type="button" onClick={() => setDemoPanelOpen(false)} className="rounded p-0.5 text-slate-400 hover:bg-slate-800 hover:text-slate-100">✕</button>
                                </div>
                                <p className="mt-1 text-[11px] text-slate-400">Detection → resolution on one live refinery incident. Use ← / → to step, Enter to run a step, Esc to close.</p>
                                <div className="mt-2 flex gap-2">
                                    <button type="button" onClick={() => setDemoIntroOpen(true)} className="flex-1 rounded bg-slate-700 px-2 py-1.5 text-xs font-medium text-white hover:bg-slate-600">Intro</button>
                                    <button type="button" onClick={() => void handleAutoRunDemo()} disabled={autoPlayRunning} className="flex-1 rounded bg-cyan-600 px-2 py-1.5 text-xs font-semibold text-white hover:bg-cyan-500 disabled:opacity-50">▶ Run all</button>
                                </div>
                                <ol className="mt-2 max-h-56 space-y-1 overflow-y-auto">
                                    {demoScriptSteps.map((step, i) => (
                                        <li key={step.id}>
                                            <button
                                                type="button"
                                                onClick={() => { setDemoStepIndex(i); void step.action(); }}
                                                disabled={autoPlayRunning}
                                                className={`w-full rounded px-2 py-1 text-left text-[11px] ${i === demoStepIndex ? "bg-cyan-900/40 text-cyan-100" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"} disabled:opacity-60`}
                                            >
                                                <span className="font-semibold">{step.label}</span>
                                                <span className="block text-[10px] text-slate-500">{step.detail}</span>
                                            </button>
                                        </li>
                                    ))}
                                </ol>
                                <div className="mt-2 flex items-center justify-between gap-2">
                                    <button type="button" onClick={handleOpenMissionReport} className="flex-1 rounded bg-emerald-700 px-2 py-1.5 text-[11px] font-medium text-white hover:bg-emerald-600">Mission report</button>
                                    <span className="text-[10px] text-slate-500">{runHistorySummaries.length} run{runHistorySummaries.length === 1 ? "" : "s"}</span>
                                </div>
                            </div>
                        )}
                    </div>
                    <button
                        type="button"
                        onClick={() => setLive((v) => !v)}
                        className={`rounded px-2.5 py-1.5 font-medium text-white ${live ? "bg-emerald-600" : "bg-slate-600"}`}
                    >
                        {live ? "⏸ Pause" : "▶ Resume"}
                    </button>
                    <select
                        value={refreshMs}
                        onChange={(e) => setRefreshMs(Number(e.target.value))}
                        className="rounded border border-slate-700 bg-[#08142a] px-2 py-1.5"
                    >
                        <option value={1000}>1s</option>
                        <option value={2500}>2.5s</option>
                        <option value={5000}>5s</option>
                    </select>
                    <div className="hidden text-right sm:block">
                        <div className="text-slate-400">pde_refineryworld</div>
                        <div>{new Date().toLocaleTimeString()}</div>
                    </div>
                </div>
            </header>

            {demoNarration && (
                <div className="border-b border-cyan-800/50 bg-[#06182fee] px-3 py-2 sm:px-5">
                    <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                        <span className="rounded-full bg-cyan-600/30 px-2 py-0.5 text-[10px] font-semibold text-cyan-100">Step {demoNarration.index}/{demoNarration.total}</span>
                        <span className="text-sm font-semibold text-cyan-100">{demoNarration.title}</span>
                        {autoPlayRunning && <span className="text-[10px] text-cyan-300">● auto-running</span>}
                        <p className="w-full text-[11px] leading-snug text-slate-300">{demoNarration.caption}</p>
                        {demoNarration.focus && <span className="text-[10px] text-slate-400">Look at: {demoNarration.focus}</span>}
                    </div>
                </div>
            )}

            <div className="flex min-h-0 flex-1 flex-col md:flex-row">
                <NavRail view={view} onChange={setView} badges={{ alerts: unackedAlerts.length }} />

                <section className="relative min-h-0 flex-1">
                    {view === "map" && (
                        <div className="relative h-full min-h-[420px] md:min-h-[520px]">
                            <SceneErrorBoundary label="Fleet map">
                                <Suspense fallback={<div className="h-full w-full animate-pulse bg-[#051020]" />}>
                                    <LazySolarFleetScene turbines={turbines} sites={sites} selectedId={selected.id} dimmedIds={dimmedIds} paused={!live} onSelect={openTurbine} />
                                </Suspense>
                            </SceneErrorBoundary>

                            <div className="absolute left-2 right-2 top-2 rounded-lg border border-slate-700/60 bg-[#06101fd9] p-2 backdrop-blur sm:left-3 sm:right-auto sm:max-w-[78%]">
                                {toolbar}
                            </div>

                            <div className="absolute right-3 top-3 hidden w-60 rounded-lg border border-slate-700/60 bg-[#07162de6] p-3 backdrop-blur sm:block">
                                <p className="text-[10px] uppercase tracking-wide text-slate-400">Selected</p>
                                <p className="text-sm font-semibold">{selected.id}</p>
                                <p className="text-xs text-slate-400">{selected.siteName}</p>
                                <p className="text-xs" style={{ color: STATUS_COLORS[selected.status] }}>
                                    {selected.status} · {selected.powerKw.toLocaleString()} kbd · {selected.irradianceWm2} kbd feed
                                </p>
                                <div className="mt-2"><Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} /></div>
                                <div className="mt-2 flex gap-1">
                                    <button type="button" onClick={() => setDetailOpen(true)} className="flex-1 rounded bg-cyan-600 px-2 py-1 text-xs font-medium text-white">Details</button>
                                    <button type="button" onClick={goToOps} className="flex-1 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white">Dispatch</button>
                                </div>
                            </div>

                            <div className="absolute bottom-3 left-3 hidden rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-1.5 text-xs text-slate-400 sm:block">
                                Click any refinery to open its live detail popup · drag to pan · scroll or use ＋ / － to zoom.
                            </div>
                        </div>
                    )}

                    {view === "twin" && twinTab === "overview" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3 flex flex-wrap items-center gap-2">
                                <div className="flex overflow-hidden rounded-lg border border-slate-700 text-xs">
                                    <button type="button" onClick={() => setTwinTab("overview")} className={`px-3 py-1.5 font-medium ${twinTab === "overview" ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"}`}>Overview</button>
                                    <button type="button" onClick={() => setTwinTab("ops")} className={`px-3 py-1.5 font-medium ${twinTab === "ops" ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"}`}>Operations &amp; Orders</button>
                                </div>
                                <span className="text-xs text-slate-400">{selected.id} · {selected.siteName}</span>
                            </div>
                            <div className="grid grid-cols-1 gap-3 xl:grid-cols-3">
                                <div className="flex flex-col gap-3 xl:col-span-2">
                                    <div className="flex flex-wrap items-center gap-2">
                                        <span className="text-xs uppercase tracking-wide text-slate-400">Inspecting</span>
                                        <select
                                            value={selectedId}
                                            onChange={(e) => setSelectedId(e.target.value)}
                                            className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm"
                                        >
                                            {turbines.map((t) => (
                                                <option key={t.id} value={t.id}>{t.id} — {t.siteName}</option>
                                            ))}
                                        </select>
                                        <button
                                            type="button"
                                            onClick={() => { const i = turbines.findIndex((t) => t.id === selectedId); setSelectedId(turbines[(i - 1 + turbines.length) % turbines.length].id); }}
                                            className="rounded bg-slate-700 px-2 py-1 text-xs text-white"
                                        >
                                            ‹ Prev
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => { const i = turbines.findIndex((t) => t.id === selectedId); setSelectedId(turbines[(i + 1) % turbines.length].id); }}
                                            className="rounded bg-slate-700 px-2 py-1 text-xs text-white"
                                        >
                                            Next ›
                                        </button>
                                        <span
                                            className="ml-auto inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium"
                                            style={{ backgroundColor: `${STATUS_COLORS[selected.status]}22`, color: STATUS_COLORS[selected.status] }}
                                        >
                                            ● {selected.status.toUpperCase()}
                                        </span>
                                    </div>

                                    <div className="relative h-[480px] overflow-hidden rounded-xl border border-slate-700/60 bg-[#051020]">
                                        <SceneErrorBoundary label="Digital twin">
                                            <Suspense fallback={<div className="h-full w-full animate-pulse bg-[#051020]" />}>
                                                <LazyPlantTwinScene turbine={selected} paused={!live} />
                                            </Suspense>
                                        </SceneErrorBoundary>
                                        <div className="absolute left-3 top-3 rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-2 backdrop-blur">
                                            <p className="text-sm font-semibold">{selected.id}</p>
                                            <p className="text-xs text-slate-400">{selected.siteName} · {selected.latitude.toFixed(2)}, {selected.longitude.toFixed(2)}</p>
                                        </div>
                                        {demoFocusPart && (
                                            <div className="absolute bottom-3 left-3 right-3 rounded-lg border border-cyan-500/50 bg-[#06182fdd] px-3 py-2 text-xs text-cyan-100 backdrop-blur">
                                                Guided focus · {(TWIN_PARTS.find((p) => p.key === demoFocusPart)?.caption ?? demoFocusPart)} — reading asset &amp; sensor signals for {selected.id}.
                                            </div>
                                        )}
                                    </div>
                                </div>

                                <div className="space-y-3">
                                    <Panel title="Ontology entity">
                                        <dl className="space-y-1 text-sm text-slate-300">
                                            <div className="flex justify-between"><dt className="text-slate-400">Entity type</dt><dd className="text-cyan-200">RefineryUnit</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Instance</dt><dd>{selected.id}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">belongsTo → RefinerySite</dt><dd>{selected.siteName}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Region</dt><dd>{selectedSite?.region ?? "—"}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Coordinates</dt><dd>{selected.latitude.toFixed(3)}, {selected.longitude.toFixed(3)}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Rated capacity</dt><dd>{selectedSite ? `${selectedSite.capacityMw} kbd` : "—"}</dd></div>
                                        </dl>
                                    </Panel>

                                    <Panel
                                        title="Twin device graph (admin)"
                                        action={<span className="text-[10px] text-slate-500">{twinDeviceRows.length > 0 ? `${twinDeviceRows.length} rows` : "bundled"}</span>}
                                    >
                                        <p className="mb-2 text-[11px] text-slate-400">Edit the asset → device tree behind the twin. Changes persist to the ontology <span className="text-cyan-200">UnitDevice</span> store (fallback-safe).</p>
                                        <div className="flex flex-wrap gap-1">
                                            {TWIN_PARTS.map((p) => (
                                                <button
                                                    key={p.key}
                                                    type="button"
                                                    onClick={() => { setFocusedTwinPart(p.key as RefineryPartKey); setFocusedTwinDevice((twinDeviceGraph[p.key as RefineryPartKey] ?? [])[0]?.key ?? null); }}
                                                    className={`rounded px-2 py-0.5 text-[11px] ${focusedTwinPart === p.key ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"}`}
                                                >
                                                    {p.caption}
                                                </button>
                                            ))}
                                        </div>

                                        {!focusedTwinPart ? (
                                            <p className="mt-2 text-[11px] text-slate-500">Select an asset above to inspect and edit its devices.</p>
                                        ) : (
                                            <>
                                                <div className="mt-2 flex flex-wrap gap-1">
                                                    {twinDeviceDefinitions.map((d) => (
                                                        <button
                                                            key={d.key}
                                                            type="button"
                                                            onClick={() => setFocusedTwinDevice(d.key)}
                                                            className={`rounded px-2 py-0.5 text-[11px] ${focusedTwinDevice === d.key ? "bg-emerald-600 text-white" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"}`}
                                                        >
                                                            {d.label}
                                                        </button>
                                                    ))}
                                                </div>

                                                {twinDeviceLayer && (
                                                    <p className="mt-2 rounded bg-[#06101f] px-2 py-1 text-[11px]">
                                                        <span className="text-slate-400">Live · {twinDeviceLayer.property}:</span>{" "}
                                                        <span style={{ color: STATUS_COLORS[twinDeviceLayer.status] }}>{twinDeviceLayer.value} {twinDeviceLayer.unit} · {twinDeviceLayer.status}</span>
                                                    </p>
                                                )}

                                                {deviceDraft && (
                                                    <div className="mt-2 space-y-1.5">
                                                        <label className="block text-[10px] text-slate-400">Label
                                                            <input value={deviceDraft.label} onChange={(e) => handleTwinDraftChange({ label: e.target.value })} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                        </label>
                                                        <div className="grid grid-cols-2 gap-1.5">
                                                            <label className="block text-[10px] text-slate-400">Property
                                                                <input value={deviceDraft.property} onChange={(e) => handleTwinDraftChange({ property: e.target.value })} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                            </label>
                                                            <label className="block text-[10px] text-slate-400">Unit
                                                                <input value={deviceDraft.unit} onChange={(e) => handleTwinDraftChange({ unit: e.target.value })} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                            </label>
                                                        </div>
                                                        <label className="block text-[10px] text-slate-400">Note
                                                            <textarea value={deviceDraft.note} onChange={(e) => handleTwinDraftChange({ note: e.target.value })} rows={2} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                        </label>
                                                        <div className="grid grid-cols-2 gap-1.5">
                                                            <label className="block text-[10px] text-slate-400">Zoom
                                                                <input value={deviceDraft.zoom} onChange={(e) => handleTwinDraftChange({ zoom: e.target.value })} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                            </label>
                                                            <label className="block text-[10px] text-slate-400">Sort order
                                                                <input value={deviceDraft.sortOrder} onChange={(e) => handleTwinDraftChange({ sortOrder: e.target.value })} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                            </label>
                                                        </div>
                                                        <label className="block text-[10px] text-slate-400">Anchor (x, y, z)
                                                            <input value={deviceDraft.anchor} onChange={(e) => handleTwinDraftChange({ anchor: e.target.value })} className="mt-0.5 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-[11px] text-slate-100" />
                                                        </label>
                                                        <div className="flex flex-wrap gap-1 pt-1">
                                                            <button type="button" onClick={handleTwinDraftSave} disabled={deviceSaveBusy || !deviceDraftDirty} className="rounded bg-emerald-600 px-2 py-1 text-[10px] font-medium text-white hover:bg-emerald-500 disabled:opacity-40">Save</button>
                                                            <button type="button" onClick={handleTwinDraftReset} disabled={deviceSaveBusy} className="rounded bg-slate-700 px-2 py-1 text-[10px] text-white hover:bg-slate-600 disabled:opacity-40">Reset</button>
                                                            <button type="button" onClick={handleTwinAddSibling} disabled={deviceSaveBusy} className="rounded bg-cyan-700 px-2 py-1 text-[10px] text-white hover:bg-cyan-600 disabled:opacity-40">Add sibling</button>
                                                            <button type="button" onClick={handleTwinDeleteDevice} disabled={deviceSaveBusy} className="rounded bg-rose-700 px-2 py-1 text-[10px] text-white hover:bg-rose-600 disabled:opacity-40">Delete</button>
                                                        </div>
                                                    </div>
                                                )}
                                                {deviceSaveMessage && <p className="mt-1.5 text-[10px] text-cyan-300">{deviceSaveMessage}</p>}
                                            </>
                                        )}
                                    </Panel>

                                    <Panel title="Live signals (timeseries)">
                                        <div className="mb-2">
                                            <p className="text-xs text-slate-400">Throughput</p>
                                            <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} />
                                            <p className="mt-0.5 text-right text-xs text-cyan-200">{selected.powerKw.toLocaleString()} kbd</p>
                                        </div>
                                        <div className="space-y-2">
                                            <Meter label="Feed rate" value={selected.irradianceWm2} unit="kbd" max={260} warn={SIGNAL_METADATA.irradiance.warn} alarm={SIGNAL_METADATA.irradiance.alarm} property={SIGNAL_METADATA.irradiance.ontologyProperty} />
                                            <Meter label="Unit temp" value={selected.moduleTempC} unit="°C" max={500} warn={SIGNAL_METADATA.moduleTemp.warn} alarm={SIGNAL_METADATA.moduleTemp.alarm} property={SIGNAL_METADATA.moduleTemp.ontologyProperty} />
                                            <Meter label="Utilization" value={selected.inverterLoadPct} unit="%" max={100} warn={SIGNAL_METADATA.inverterLoad.warn} alarm={SIGNAL_METADATA.inverterLoad.alarm} property={SIGNAL_METADATA.inverterLoad.ontologyProperty} />
                                        </div>
                                        <p className="mt-2 text-[10px] leading-tight text-slate-500">
                                            Bands sourced from the ontology Sensor thresholds{thresholdsPublished != null ? ` · published to Fabric (${thresholdsPublished})` : ""}. Hover a meter for its property &amp; limits.
                                        </p>
                                    </Panel>

                                    <Panel
                                        title="Related dispatch notes"
                                        action={
                                            <button type="button" onClick={loadNotes} className="text-xs text-emerald-300 hover:text-emerald-100" title="Refresh from backend">
                                                {notesLoading ? "…" : "↻"}
                                            </button>
                                        }
                                    >
                                        {relatedNotes.length === 0 ? (
                                            <p className="text-xs text-slate-400">No notes linked to this unit yet.</p>
                                        ) : (
                                            <ul className="space-y-1 text-xs">
                                                {relatedNotes.map((n) => (
                                                    <li key={n.id ?? `${n.turbineId}-${n.createdAt}`} className="rounded bg-[#0b2a20aa] px-2 py-1">
                                                        <span style={{ color: STATUS_COLORS[n.status as PlantStatus] ?? "#94a3b8" }}>
                                                            {n.status} · {n.powerKw.toLocaleString()} kbd
                                                        </span>
                                                        {n.note && <p className="mt-0.5 truncate text-[11px] text-slate-400" title={n.note}>{n.note}</p>}
                                                    </li>
                                                ))}
                                            </ul>
                                        )}
                                        <button type="button" onClick={goToOps} className="mt-2 w-full rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-500">Dispatch / writeback →</button>
                                    </Panel>

                                    <Panel title="Site report" action={siteReport ? <span className="text-[10px] text-slate-500">{siteReport.unitCount} units</span> : undefined}>
                                        {siteReport ? (
                                            <>
                                                <div className="grid grid-cols-3 gap-2">
                                                    <MetricCard label="Throughput" value={`${siteReport.totalKbd.toLocaleString()} kbd`} sub={`${siteReport.unitCount} units`} />
                                                    <MetricCard label="Utilization" value={`${siteReport.utilization.toFixed(0)}%`} sub={`of ${siteReport.ratedKbd.toLocaleString()} kbd`} accent="text-emerald-300" />
                                                    <MetricCard label="Avg unit temp" value={`${siteReport.avgUnitTempC.toFixed(0)} °C`} sub={`util ${siteReport.avgUtilizationPct.toFixed(0)}%`} accent="text-amber-300" />
                                                </div>
                                                <div className="mt-2"><HealthBar healthy={siteReport.healthy} warning={siteReport.warnings} alarm={siteReport.alarms} /></div>
                                                <ul className="mt-2 max-h-40 space-y-1 overflow-y-auto pr-1">
                                                    {siteUnits.map((t) => (
                                                        <li key={t.id}>
                                                            <button type="button" onClick={() => setSelectedId(t.id)} className={`flex w-full items-center justify-between rounded px-2 py-1 text-left text-xs ${t.id === selected.id ? "bg-cyan-900/40" : "bg-[#0a1830] hover:bg-slate-800"}`}>
                                                                <span className="flex items-center gap-2"><span className="inline-block h-2 w-2 rounded-full" style={{ background: STATUS_COLORS[t.status] }} /><span className="text-slate-200">{t.id}</span></span>
                                                                <span className="text-slate-400">{t.powerKw.toLocaleString()} kbd · {t.inverterLoadPct}%</span>
                                                            </button>
                                                        </li>
                                                    ))}
                                                </ul>
                                            </>
                                        ) : <p className="text-xs text-slate-400">No site summary available.</p>}
                                    </Panel>
                                </div>
                            </div>
                        </div>
                    )}

                    {view === "alerts" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                                <Panel
                                    title={`Active alarms & warnings (${unackedAlerts.length}/${activeAlerts.length})`}
                                    action={
                                        <button type="button" onClick={() => setShowAcked((v) => !v)} className="text-xs text-cyan-300 hover:text-cyan-100">
                                            {showAcked ? "Hide acknowledged" : "Show acknowledged"}
                                        </button>
                                    }
                                >
                                    {(showAcked ? activeAlerts : unackedAlerts).length === 0 ? (
                                        <p className="text-xs text-slate-400">No active alerts to show.</p>
                                    ) : (
                                        <ul className="space-y-2">
                                            {(showAcked ? activeAlerts : unackedAlerts).map((t) => (
                                                <li key={t.id} className="rounded-md border border-slate-700/60 bg-[#0b1d38aa] px-3 py-2">
                                                    <div className="flex items-center justify-between">
                                                        <div>
                                                            <span className="font-medium text-slate-100">{t.id}</span>
                                                            <span className="ml-2 text-xs text-slate-400">{t.siteName}</span>
                                                        </div>
                                                        <span
                                                            className="rounded-full px-2 py-0.5 text-[11px] font-medium"
                                                            style={{ backgroundColor: `${STATUS_COLORS[t.status]}22`, color: STATUS_COLORS[t.status] }}
                                                        >
                                                            {t.status.toUpperCase()}
                                                        </span>
                                                    </div>
                                                    <div className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5 text-[11px] text-slate-400">
                                                        <span>Unit <span style={{ color: signalColor("moduleTemp", t.moduleTempC) }}>{t.moduleTempC} C</span></span>
                                                        <span>Utilization <span style={{ color: signalColor("inverterLoad", t.inverterLoadPct) }}>{t.inverterLoadPct} %</span></span>
                                                        <span>Throughput {t.powerKw.toLocaleString()} kbd</span>
                                                    </div>
                                                    <div className="mt-2 flex gap-2">
                                                        <button type="button" onClick={() => goToTwin(t.id)} className="rounded bg-slate-700 px-2 py-1 text-xs text-white">Inspect</button>
                                                        {ackLog[t.id] ? (
                                                            <span className="rounded bg-emerald-900/60 px-2 py-1 text-xs text-emerald-300">✓ Ack {new Date(ackLog[t.id].at).toLocaleTimeString()} · {ackLog[t.id].by}</span>
                                                        ) : (
                                                            <button type="button" onClick={() => void acknowledgeAlert(t)} disabled={!canWriteback} className="rounded bg-emerald-600 px-2 py-1 text-xs text-white hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-50">{canWriteback ? "Acknowledge" : "Operator only"}</button>
                                                        )}
                                                    </div>
                                                </li>
                                            ))}
                                        </ul>
                                    )}
                                    {ackMessage && <p className="mt-2 text-xs text-emerald-300">{ackMessage}</p>}
                                    <p className="mt-2 text-[11px] text-slate-500">Auto-logged {autoLogCount} alarm onset{autoLogCount === 1 ? "" : "s"} to dispatch notes this session.{isTeamsAlertConfigured() ? " · Teams alerts on" : ""}</p>
                                </Panel>

                                <Panel title="Anomaly watch (predictive)">
                                    <p className="mb-2 text-[11px] text-slate-400">Process units trending toward thresholds, ranked by anomaly score with a slope-based escalation trend and ETA to alarm.</p>
                                    <ul className="space-y-2">
                                        {anomalyWatch.map((a) => (
                                            <li key={a.t.id}>
                                                <div className="flex justify-between text-xs">
                                                    <button type="button" onClick={() => goToTwin(a.t.id)} className="text-slate-200 hover:text-cyan-200">{a.t.id} · {a.t.siteName}</button>
                                                    <span className="flex items-center gap-2">
                                                        <span className={a.forecast.direction === "rising" ? "text-amber-300" : a.forecast.direction === "falling" ? "text-emerald-300" : "text-slate-500"}>
                                                            {a.forecast.direction === "rising" ? "↑" : a.forecast.direction === "falling" ? "↓" : "→"}
                                                            {a.forecast.etaToAlarmTicks != null && ` ~${a.forecast.etaToAlarmTicks}t`}
                                                        </span>
                                                        <span className="text-slate-300">{Math.round(a.score * 100)}%</span>
                                                    </span>
                                                </div>
                                                <div className="mt-1 h-2 rounded bg-slate-800">
                                                    <div
                                                        className="h-2 rounded"
                                                        style={{ width: `${a.score * 100}%`, background: a.score > 0.75 ? STATUS_COLORS.alarm : a.score > 0.5 ? STATUS_COLORS.warning : STATUS_COLORS.healthy }}
                                                    />
                                                </div>
                                            </li>
                                        ))}
                                    </ul>
                                </Panel>
                            </div>
                        </div>
                    )}

                    {view === "graph" && (
                        <div className="h-full overflow-hidden p-4">
                            <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                                <p className="text-xs uppercase tracking-wide text-slate-400">Ontology relationships · Fleet → Site → Unit → Signal</p>
                                <div className="flex items-center gap-2 text-xs">
                                    <div className="flex overflow-hidden rounded border border-slate-700">
                                        {(["all", "healthy", "warning", "alarm"] as StatusFilter[]).map((s) => (
                                            <button key={s} type="button" onClick={() => setGraphFilter(s)} className={`px-2 py-1 capitalize ${graphFilter === s ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300"}`}>{s}</button>
                                        ))}
                                    </div>
                                    <button type="button" onClick={() => setGraphNonce((n) => n + 1)} className="rounded bg-slate-700 px-2 py-1 text-white">Reset view</button>
                                </div>
                            </div>
                            <div className="relative h-[calc(100%-2rem)] rounded-xl border border-slate-700/60 bg-[#051020]">
                                <RelationshipGraph key={graphNonce} turbines={turbines} sites={sites} selectedId={selected.id} statusFilter={graphFilter} onSelect={(id) => setSelectedId(id)} />
                                <div className="pointer-events-none absolute right-3 top-2 rounded-md border border-slate-700/60 bg-[#08142acc] px-2.5 py-2 text-[11px] text-slate-300 backdrop-blur-sm">
                                    <p className="mb-1 font-medium text-slate-400">Node types</p>
                                    <div className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-[#6ee7ff] bg-[#0e2a4d]" /> Fleet</div>
                                    <div className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-slate-400 bg-[#0b2747]" /> Site</div>
                                    <div className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full bg-[#58d68d]" /> Unit</div>
                                    <div className="mt-1 flex items-center gap-1.5"><span className="inline-block h-0 w-3 border-t border-dashed border-slate-400" /> Signal (emits)</div>
                                    <p className="mt-1.5 text-slate-500">healthy · <span className="text-[#ffd166]">warn</span> · <span className="text-[#ef476f]">alarm</span></p>
                                    <p className="mt-1 max-w-[160px] text-[10px] leading-tight text-slate-500">Bands from ontology Sensor thresholds — hover a signal for its property &amp; limits.</p>
                                </div>
                                <div className="pointer-events-none absolute bottom-2 left-3 text-[11px] text-slate-500">Scroll to zoom · drag to pan · click a unit to drill into its signals</div>
                            </div>
                        </div>
                    )}

                    {view === "analytics" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3">{toolbar}</div>
                            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                                <MetricCard label="Fleet throughput" value={`${fleetPower.toLocaleString()} kbd`} sub={`${visibleTurbines.length} units online`} />
                                <MetricCard label="Utilization" value={`${capacityFactor.toFixed(0)}%`} sub={`of ${fleetRatedMw.toFixed(0)} kbd rated`} accent="text-emerald-300" />
                                <MetricCard label="Avg feed rate" value={`${avgWind.toFixed(0)} kbd`} sub="across visible fleet" />
                                <MetricCard label="Avg unit temp" value={`${avgTemp.toFixed(1)} °C`} sub="thermal load" accent="text-amber-300" />
                            </div>

                            <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-3">
                                <Panel title="Throughput share by site"><DonutChart items={outputBars} unit="kbd" /></Panel>
                                <Panel title="Fleet health">
                                    <HealthBar healthy={healthy} warning={warnings} alarm={alarms} />
                                    <div className="mt-3 flex flex-wrap gap-4 text-xs">
                                        <span style={{ color: STATUS_COLORS.healthy }}>● Healthy {healthy}</span>
                                        <span style={{ color: STATUS_COLORS.warning }}>● Warning {warnings}</span>
                                        <span style={{ color: STATUS_COLORS.alarm }}>● Alarm {alarms}</span>
                                    </div>
                                </Panel>
                                <Panel title="Top performers"><BarList items={topBars} /></Panel>
                            </div>

                            <div className="mt-3">
                                <Panel
                                    title="Yield curve · feed rate vs throughput"
                                    action={
                                        <div className="flex flex-wrap gap-3 text-[10px]">
                                            <span style={{ color: STATUS_COLORS.healthy }}>● Healthy</span>
                                            <span style={{ color: STATUS_COLORS.warning }}>● Warning</span>
                                            <span style={{ color: STATUS_COLORS.alarm }}>● Alarm</span>
                                        </div>
                                    }
                                >
                                    <ScatterPlot points={powerCurve} xLabel="Feed rate (kbd)" yLabel="Throughput (kbd)" xMax={curveFeedMax} yMax={curvePowerMax} />
                                    <p className="mt-1 text-[10px] leading-tight text-slate-500">
                                        Each dot is a visible process unit. Points hugging the upper-right convert feed to product efficiently; low-throughput dots at high feed rate are underperformers worth inspecting.
                                    </p>
                                </Panel>
                            </div>
                        </div>
                    )}

                    {view === "scenario" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3">{toolbar}</div>
                            <div className="mx-auto max-w-5xl space-y-3">
                                <Panel
                                    title={`Scenario Lab · ${selected.siteName} · ${selected.id}`}
                                    action={<span className="text-[11px] text-slate-400">Baseline {selected.powerKw.toLocaleString()} kbd</span>}
                                >
                                    <p className="text-xs text-slate-400">
                                        Compare operating plans (throughput curtailment + maintenance downtime), import your own plans or actuals from Excel/CSV, weigh them against forecast-vs-realised throughput, then ask GenAI for a full read.
                                    </p>
                                    <div className="mt-2 flex flex-wrap items-center gap-2">
                                        <input
                                            ref={scenarioFileRef}
                                            type="file"
                                            accept=".xlsx,.xls,.csv"
                                            className="hidden"
                                            onChange={(e) => { const f = e.target.files?.[0]; if (f) { void handleScenarioImport(f); } e.target.value = ""; }}
                                        />
                                        <button type="button" onClick={() => scenarioFileRef.current?.click()} className="rounded border border-slate-600 bg-[#0a1830] px-3 py-1.5 text-xs font-medium text-slate-200 hover:border-cyan-500 hover:text-cyan-200">
                                            ⬆ Import Excel / CSV
                                        </button>
                                        <button type="button" onClick={() => void handleScenarioExport()} className="rounded border border-slate-600 bg-[#0a1830] px-3 py-1.5 text-xs font-medium text-slate-200 hover:border-cyan-500 hover:text-cyan-200">
                                            ⬇ Export to Excel
                                        </button>
                                        {importedActuals && (
                                            <button type="button" onClick={() => { setImportedActuals(null); setImportStatus(null); }} className="rounded border border-slate-700 px-2 py-1.5 text-[11px] text-slate-400 hover:text-rose-300">Clear import</button>
                                        )}
                                        {importStatus && <span className="text-[11px] text-slate-400">{importStatus}</span>}
                                    </div>
                                    <p className="mt-1 text-[10px] text-slate-500">
                                        Recognised columns — scenarios: label, curtailment %, downtime, horizon · actuals: period, actual, forecast.
                                    </p>
                                </Panel>

                                <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
                                    {scenarioComparison.scenarios.map((s) => (
                                        <div key={s.id} className={`rounded-xl border p-3 ${s.isBest ? "border-emerald-500/70 bg-emerald-950/20" : "border-slate-700/70 bg-[#08142a]"}`}>
                                            <div className="flex items-center justify-between gap-2">
                                                <input
                                                    value={s.label}
                                                    onChange={(e) => updateScenario(s.id, { label: e.target.value })}
                                                    className="min-w-0 flex-1 rounded bg-transparent text-sm font-semibold text-slate-100 outline-none focus:bg-[#0a1830] focus:px-1"
                                                    aria-label="Scenario label"
                                                />
                                                {s.isBest && <span className="rounded-full bg-emerald-900/70 px-2 py-0.5 text-[10px] font-semibold text-emerald-200">BEST</span>}
                                                {scenarios.length > 1 && (
                                                    <button type="button" onClick={() => removeScenario(s.id)} aria-label="Remove scenario" className="rounded p-0.5 text-slate-500 hover:bg-slate-800 hover:text-rose-300">✕</button>
                                                )}
                                            </div>
                                            <div className="mt-2 space-y-2 text-[11px] text-slate-300">
                                                <label className="block">
                                                    <span className="flex justify-between"><span>Curtailment</span><span className="font-semibold text-cyan-200">{s.curtailmentPct}%</span></span>
                                                    <input type="range" min={0} max={100} value={s.curtailmentPct} onChange={(e) => updateScenario(s.id, { curtailmentPct: Number(e.target.value) })} className="w-full" aria-label="Curtailment percent" />
                                                </label>
                                                <label className="block">
                                                    <span className="flex justify-between"><span>Downtime</span><span className="font-semibold text-cyan-200">{s.downtimeTicks} t</span></span>
                                                    <input type="range" min={0} max={s.horizonTicks} value={s.downtimeTicks} onChange={(e) => updateScenario(s.id, { downtimeTicks: Number(e.target.value) })} className="w-full" aria-label="Downtime ticks" />
                                                </label>
                                                <label className="block">
                                                    <span className="flex justify-between"><span>Horizon</span><span className="font-semibold text-cyan-200">{s.horizonTicks} t</span></span>
                                                    <input type="range" min={1} max={48} value={s.horizonTicks} onChange={(e) => updateScenario(s.id, { horizonTicks: Number(e.target.value) })} className="w-full" aria-label="Horizon ticks" />
                                                </label>
                                            </div>
                                            <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 border-t border-slate-700/60 pt-2 text-[11px]">
                                                <dt className="text-slate-400">Projected</dt><dd className="text-right text-slate-100">{s.projectedKbd.toLocaleString()} kbd</dd>
                                                <dt className="text-slate-400">Volume Δ</dt><dd className={`text-right ${s.volumeDelta < 0 ? "text-rose-300" : "text-emerald-300"}`}>{s.volumeDelta >= 0 ? "+" : ""}{s.volumeDelta.toLocaleString()} kbd·t</dd>
                                                <dt className="text-slate-400">vs baseline</dt><dd className={`text-right ${s.deltaPct < 0 ? "text-rose-300" : "text-emerald-300"}`}>{s.deltaPct >= 0 ? "+" : ""}{s.deltaPct}%</dd>
                                                <dt className="text-slate-400">Rank</dt><dd className="text-right text-slate-100">#{s.rank}</dd>
                                            </dl>
                                        </div>
                                    ))}
                                    {scenarios.length < 8 && (
                                        <button type="button" onClick={addScenario} className="flex min-h-[8rem] items-center justify-center rounded-xl border border-dashed border-slate-600 text-sm text-slate-400 hover:border-cyan-500 hover:text-cyan-200">+ Add scenario</button>
                                    )}
                                </div>

                                <Panel title="Insights">
                                    <div className="grid grid-cols-2 gap-2 text-center sm:grid-cols-3 lg:grid-cols-6">
                                        <div className="rounded-lg bg-[#08142a] p-2"><p className="text-[10px] text-slate-400">Plans</p><p className="text-sm font-semibold text-slate-100">{scenarioInsights.planCount}</p></div>
                                        <div className="rounded-lg bg-[#08142a] p-2"><p className="text-[10px] text-slate-400">Best</p><p className="truncate text-sm font-semibold text-emerald-300" title={scenarioInsights.bestLabel ?? ""}>{scenarioInsights.bestLabel ?? "—"}</p></div>
                                        <div className="rounded-lg bg-[#08142a] p-2"><p className="text-[10px] text-slate-400">Spread</p><p className="text-sm font-semibold text-slate-100">{scenarioInsights.spread.toLocaleString()}</p></div>
                                        <div className="rounded-lg bg-[#08142a] p-2"><p className="text-[10px] text-slate-400">Avg Δ</p><p className={`text-sm font-semibold ${scenarioInsights.avgDelta < 0 ? "text-rose-300" : "text-emerald-300"}`}>{scenarioInsights.avgDelta >= 0 ? "+" : ""}{scenarioInsights.avgDelta.toLocaleString()}</p></div>
                                        <div className="rounded-lg bg-[#08142a] p-2"><p className="text-[10px] text-slate-400">Dispersion</p><p className="text-sm font-semibold text-slate-100">{scenarioInsights.deltaStdDev.toLocaleString()}</p></div>
                                        <div className="rounded-lg bg-[#08142a] p-2"><p className="text-[10px] text-slate-400">Riskiest</p><p className="truncate text-sm font-semibold text-amber-300" title={scenarioInsights.riskiestLabel ?? ""}>{scenarioInsights.riskiestLabel ?? "—"}</p></div>
                                    </div>
                                </Panel>

                                {importedActuals && importedSummary && (
                                    <Panel
                                        title={`Imported actuals · ${importedActuals.name}`}
                                        action={<span className="text-[11px] text-slate-400">{importedSummary.n} pts · {importedSummary.trend}</span>}
                                    >
                                        <div className="grid grid-cols-3 gap-2 text-center text-[11px]">
                                            <div><dt className="text-slate-400">Mean</dt><dd className="font-semibold text-slate-100">{importedSummary.mean.toLocaleString()} kbd</dd></div>
                                            <div><dt className="text-slate-400">Min</dt><dd className="font-semibold text-slate-100">{importedSummary.min.toLocaleString()}</dd></div>
                                            <div><dt className="text-slate-400">Max</dt><dd className="font-semibold text-slate-100">{importedSummary.max.toLocaleString()}</dd></div>
                                        </div>
                                        <div className="mt-2 max-h-40 overflow-y-auto">
                                            <table className="w-full text-left text-[11px]">
                                                <thead className="text-slate-400"><tr><th className="py-1">Period</th><th className="py-1 text-right">Actual</th><th className="py-1 text-right">Forecast</th></tr></thead>
                                                <tbody>
                                                    {importedActuals.points.slice(0, 24).map((p, i) => (
                                                        <tr key={`${p.label}-${i}`} className="border-t border-slate-800/60">
                                                            <td className="py-1 text-slate-300">{p.label}</td>
                                                            <td className="py-1 text-right text-slate-100">{p.actual.toLocaleString()}</td>
                                                            <td className="py-1 text-right text-slate-400">{p.forecast != null ? p.forecast.toLocaleString() : "—"}</td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    </Panel>
                                )}

                                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                                    <Panel title="Forecast vs realised · selected unit">
                                        <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} />
                                        <dl className="mt-2 grid grid-cols-3 gap-2 text-center text-[11px]">
                                            <div><dt className="text-slate-400">Realised</dt><dd className="font-semibold text-slate-100">{scenarioVariance.realisedMean.toLocaleString()} kbd</dd></div>
                                            <div><dt className="text-slate-400">Forecast</dt><dd className="font-semibold text-cyan-200">{scenarioVariance.forecast.toLocaleString()} kbd</dd></div>
                                            <div><dt className="text-slate-400">Accuracy</dt><dd className={`font-semibold ${scenarioVariance.accuracyPct >= 90 ? "text-emerald-300" : scenarioVariance.accuracyPct >= 75 ? "text-amber-300" : "text-rose-300"}`}>{scenarioVariance.accuracyPct}%</dd></div>
                                        </dl>
                                        <p className="mt-1 text-[10px] leading-tight text-slate-500">
                                            Forecast is running {scenarioVariance.bias === "on-track" ? "on track with" : `${scenarioVariance.absErrorPct}% ${scenarioVariance.bias}`} realised throughput.
                                        </p>
                                    </Panel>
                                    <Panel title="Volume delta vs baseline">
                                        <div className="space-y-2">
                                            {scenarioComparison.scenarios.map((s) => {
                                                const maxAbs = Math.max(1, ...scenarioComparison.scenarios.map((x) => Math.abs(x.volumeDelta)));
                                                const pct = Math.round((Math.abs(s.volumeDelta) / maxAbs) * 100);
                                                return (
                                                    <div key={s.id} className="text-[11px]">
                                                        <div className="flex justify-between"><span className="text-slate-300">{s.label}</span><span className={s.volumeDelta < 0 ? "text-rose-300" : "text-emerald-300"}>{s.volumeDelta >= 0 ? "+" : ""}{s.volumeDelta.toLocaleString()} kbd·t</span></div>
                                                        <div className="mt-0.5 h-2 rounded bg-[#0a1830]"><div className={`h-2 rounded ${s.volumeDelta < 0 ? "bg-rose-500/70" : "bg-emerald-500/70"} ${s.isBest ? "ring-1 ring-emerald-300" : ""}`} style={{ width: `${pct}%` }} /></div>
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    </Panel>
                                </div>

                                <Panel
                                    title="GenAI analysis"
                                    action={scenarioNarrativeSource ? (
                                        <span className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${scenarioNarrativeSource === "fabriciq" ? "bg-emerald-900/60 text-emerald-300" : "bg-slate-700/60 text-slate-300"}`}>
                                            {scenarioNarrativeSource === "fabriciq" ? "● Fabric Data Agent (live)" : "○ Local engine (offline)"}
                                        </span>
                                    ) : undefined}
                                >
                                    <div className="flex flex-wrap gap-2">
                                        <button type="button" onClick={() => void runScenarioNarrative()} disabled={scenarioNarrativeLoading} className="rounded bg-cyan-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-cyan-500 disabled:opacity-50">{scenarioNarrativeLoading ? "Analyzing…" : "✨ Recommend best plan"}</button>
                                        <button type="button" onClick={() => void runScenarioNarrative("Explain the forecast vs realised variance and what is driving it")} disabled={scenarioNarrativeLoading} className="rounded border border-slate-600 bg-[#0a1830] px-3 py-1.5 text-xs font-medium text-slate-200 hover:border-cyan-500 hover:text-cyan-200 disabled:opacity-50">Explain variance</button>
                                        <button type="button" onClick={() => void runScenarioNarrative("Assess the downtime and operational risk across the plans")} disabled={scenarioNarrativeLoading} className="rounded border border-slate-600 bg-[#0a1830] px-3 py-1.5 text-xs font-medium text-slate-200 hover:border-cyan-500 hover:text-cyan-200 disabled:opacity-50">Assess risk</button>
                                        <button type="button" onClick={() => void runScenarioNarrative("Give a full side-by-side comparison of all plans")} disabled={scenarioNarrativeLoading} className="rounded border border-slate-600 bg-[#0a1830] px-3 py-1.5 text-xs font-medium text-slate-200 hover:border-cyan-500 hover:text-cyan-200 disabled:opacity-50">Full comparison</button>
                                    </div>
                                    <div className="mt-2 flex gap-2">
                                        <input
                                            value={scenarioQuestion}
                                            onChange={(e) => setScenarioQuestion(e.target.value)}
                                            onKeyDown={(e) => { if (e.key === "Enter" && scenarioQuestion.trim()) { e.preventDefault(); void runScenarioNarrative(scenarioQuestion.trim()); } }}
                                            placeholder="Ask anything about these scenarios or the imported data…"
                                            className="flex-1 rounded border border-slate-700 bg-[#08142a] px-2 py-1.5 text-sm"
                                        />
                                        <button type="button" onClick={() => { if (scenarioQuestion.trim()) { void runScenarioNarrative(scenarioQuestion.trim()); } }} disabled={scenarioNarrativeLoading || !scenarioQuestion.trim()} className="rounded bg-cyan-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-cyan-500 disabled:opacity-50">Ask</button>
                                    </div>
                                    {scenarioNarrative
                                        ? <p className="mt-2 whitespace-pre-line text-sm leading-relaxed text-slate-200">{scenarioNarrative}</p>
                                        : <p className="mt-2 text-xs text-slate-500">Ask across the {scenarioComparison.scenarios.length} plans{importedActuals ? " and your imported data" : ""} using the Data Agent when configured, or an offline engine grounded in the comparison.</p>}
                                </Panel>
                            </div>
                        </div>
                    )}

                    {view === "twin" && twinTab === "ops" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3 flex flex-wrap items-center gap-2">
                                <div className="flex overflow-hidden rounded-lg border border-slate-700 text-xs">
                                    <button type="button" onClick={() => setTwinTab("overview")} className={`px-3 py-1.5 font-medium ${twinTab === "overview" ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"}`}>Overview</button>
                                    <button type="button" onClick={() => setTwinTab("ops")} className={`px-3 py-1.5 font-medium ${twinTab === "ops" ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300 hover:bg-slate-800"}`}>Operations &amp; Orders</button>
                                </div>
                                <span className="text-xs text-slate-400">{selected.id} · {selected.siteName}</span>
                            </div>
                            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                                <Panel title="Selected unit">
                                    <p className="text-lg font-semibold">{selected.id}</p>
                                    <p className="text-sm text-slate-400">{selected.siteName}</p>
                                    <p className="text-sm" style={{ color: STATUS_COLORS[selected.status] }}>Status: {selected.status}</p>
                                    <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-sm text-slate-300">
                                        <div className="flex justify-between"><dt>Throughput</dt><dd>{selected.powerKw.toLocaleString()} kbd</dd></div>
                                        <div className="flex justify-between"><dt>Feed rate</dt><dd>{selected.irradianceWm2} kbd</dd></div>
                                        <div className="flex justify-between"><dt>Unit temp</dt><dd>{selected.moduleTempC} °C</dd></div>
                                        <div className="flex justify-between"><dt>Utilization</dt><dd>{selected.inverterLoadPct} %</dd></div>
                                    </dl>

                                    <div className="mt-2">
                                        <div className="mb-1 flex items-center justify-between gap-2">
                                            <p className="text-xs text-slate-400">Throughput trend (live)</p>
                                            <div className="flex overflow-hidden rounded border border-slate-700 text-[10px]">
                                                {HISTORY_WINDOWS.map((window) => (
                                                    <button key={window} type="button" onClick={() => setHistoryWindow(window)} className={`px-2 py-1 ${historyWindow === window ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300"}`}>
                                                        {window}
                                                    </button>
                                                ))}
                                            </div>
                                        </div>
                                        <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} />
                                    </div>
                                    <div className="mt-2 rounded border border-cyan-900/60 bg-[#06182f] px-2 py-2 text-xs">
                                        <div className="flex items-center justify-between">
                                            <span className="text-slate-400">Forecast (+{forecastHorizon} ticks)</span>
                                            <span className="font-semibold text-cyan-200">{forecast.toLocaleString()} kbd</span>
                                        </div>
                                        <div className="mt-0.5 flex items-center justify-between text-[11px] text-slate-400">
                                            <span>Range {fc.lo.toLocaleString()}–{fc.hi.toLocaleString()} kbd</span>
                                            <span>Confidence {fc.confidence}%</span>
                                        </div>
                                        <div className="mt-1.5 grid grid-cols-3 gap-1">
                                            {multiForecast.map((m) => (
                                                <div key={m.h} className="rounded bg-[#08142a] px-1.5 py-1 text-center">
                                                    <div className="text-[10px] text-slate-500">+{m.h}t</div>
                                                    <div className="text-[11px] font-medium text-cyan-200">{m.value.toLocaleString()}</div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>

                                    <div className="mt-3 space-y-2 rounded border border-emerald-900/50 bg-[#06231b] p-2">
                                        <p className="text-xs uppercase tracking-wide text-emerald-300">Writeback / Dispatch</p>
                                        <p className="text-[11px] text-slate-400">
                                            Current mode: <span className={canWriteback ? "text-emerald-300" : "text-amber-300"}>{canWriteback ? "Operator" : "Viewer"}</span>
                                            {canWriteback ? " — ontology writeback enabled." : " — dispatch actions are read-only until switched to Operator."}
                                        </p>
                                        <div className="flex gap-2">
                                            <label className="flex-1 text-xs text-slate-400">
                                                Action
                                                <select
                                                    value={wbAction}
                                                    onChange={(e) => setWbAction(e.target.value)}
                                                    disabled={!canWriteback}
                                                    className="mt-1 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm text-slate-100"
                                                >
                                                    {["Acknowledge", "Inspect", "Throttle", "Boost", "Shutdown"].map((a) => (
                                                        <option key={a} value={a}>{a}</option>
                                                    ))}
                                                </select>
                                            </label>
                                            <label className="flex-1 text-xs text-slate-400">
                                                Setpoint (kbd)
                                                <input
                                                    type="number"
                                                    value={wbSetpoint}
                                                    onChange={(e) => setWbSetpoint(e.target.value)}
                                                    disabled={!canWriteback}
                                                    placeholder={String(selected.powerKw)}
                                                    className="mt-1 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm text-slate-100"
                                                />
                                            </label>
                                        </div>
                                        <div className="flex items-center gap-2 text-xs text-slate-400">
                                            <span>Forecast horizon</span>
                                            <select
                                                value={forecastHorizon}
                                                onChange={(e) => setForecastHorizon(Number(e.target.value))}
                                                aria-label="Forecast horizon"
                                                className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100"
                                            >
                                                <option value={3}>3 ticks</option>
                                                <option value={5}>5 ticks</option>
                                                <option value={10}>10 ticks</option>
                                            </select>
                                            <span className="ml-2 text-slate-500">History</span>
                                            <select
                                                value={historyWindow}
                                                onChange={(e) => setHistoryWindow(e.target.value as HistoryWindow)}
                                                aria-label="History window"
                                                className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100"
                                            >
                                                {HISTORY_WINDOWS.map((window) => (
                                                    <option key={window} value={window}>{window}</option>
                                                ))}
                                            </select>
                                        </div>
                                        <input
                                            value={wbNote}
                                            onChange={(e) => setWbNote(e.target.value)}
                                            disabled={!canWriteback}
                                            placeholder="Optional note…"
                                            className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm text-slate-100"
                                        />
                                        <button
                                            type="button"
                                            onClick={handleWriteback}
                                            disabled={!canWriteback}
                                            className="w-full rounded bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-50"
                                        >
                                            {canWriteback ? `Write ${wbAction} to ontology` : "Viewer mode — writeback disabled"}
                                        </button>
                                        {writebackMessage && <p className="text-xs text-emerald-300">{writebackMessage}</p>}
                                    </div>
                                </Panel>

                                <Panel title="What-if simulator">
                                    <p className="mb-2 text-[11px] text-slate-400">Model turndown and a maintenance window on {selected.id} against its current throughput baseline.</p>
                                    <div className="grid grid-cols-3 gap-2 text-xs text-slate-400">
                                        <label className="flex flex-col gap-1">
                                            Turndown %
                                            <input type="number" min={0} max={100} value={simCurtail} onChange={(e) => setSimCurtail(Number(e.target.value))} aria-label="Turndown percent" className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                        </label>
                                        <label className="flex flex-col gap-1">
                                            Downtime (t)
                                            <input type="number" min={0} max={simHorizon} value={simDowntime} onChange={(e) => setSimDowntime(Number(e.target.value))} aria-label="Maintenance downtime ticks" className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                        </label>
                                        <label className="flex flex-col gap-1">
                                            Horizon (t)
                                            <input type="number" min={1} max={96} value={simHorizon} onChange={(e) => setSimHorizon(Number(e.target.value))} aria-label="Scenario horizon ticks" className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                        </label>
                                    </div>
                                    <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm text-slate-300">
                                        <div className="flex justify-between"><dt className="text-slate-400">Projected throughput</dt><dd>{scenario.projectedKw.toLocaleString()} kbd</dd></div>
                                        <div className="flex justify-between"><dt className="text-slate-400">Running</dt><dd>{scenario.runningTicks}/{simHorizon} t</dd></div>
                                        <div className="col-span-2 flex justify-between border-t border-slate-700/60 pt-1"><dt className="text-slate-400">Volume vs baseline</dt><dd className={scenario.energyDeltaKwt < 0 ? "text-red-300" : "text-emerald-300"}>{scenario.energyDeltaKwt >= 0 ? "+" : ""}{scenario.energyDeltaKwt.toLocaleString()} kbd·t</dd></div>
                                    </dl>
                                </Panel>

                                <div className="lg:col-span-2">
                                    <Panel
                                        title="Predictive dispatch & mission ops"
                                        action={
                                            <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${missionChallenge.verdict === "ready" ? "bg-emerald-900/60 text-emerald-200" : missionChallenge.verdict === "watch" ? "bg-amber-900/60 text-amber-200" : "bg-rose-900/60 text-rose-200"}`}>
                                                {missionChallenge.verdict.toUpperCase()} · {missionChallenge.score}%
                                            </span>
                                        }
                                    >
                                        <div className="grid grid-cols-1 gap-3 xl:grid-cols-3">
                                            {/* Incident triage */}
                                            <div className="space-y-2 rounded border border-slate-700/60 bg-[#08142a] p-2">
                                                <p className="text-[10px] uppercase tracking-wide text-slate-400">Incident triage · {selected.id}</p>
                                                <div className="flex items-center gap-2">
                                                    <span className="rounded bg-cyan-900/50 px-2 py-0.5 text-xs font-semibold text-cyan-200">{suggestedComponent}</span>
                                                    <span className={`rounded px-2 py-0.5 text-xs font-semibold ${suggestedPriority === "P1" ? "bg-rose-900/60 text-rose-200" : suggestedPriority === "P2" ? "bg-amber-900/60 text-amber-200" : "bg-slate-700/60 text-slate-200"}`}>{suggestedPriority}</span>
                                                    {selectedForecast.etaToAlarmTicks != null && <span className="text-[11px] text-amber-300">ETA alarm ~{selectedForecast.etaToAlarmTicks}t</span>}
                                                </div>
                                                <p className="text-[11px] leading-relaxed text-slate-300">{incidentStory}</p>
                                                {selectedEvidence && (
                                                    <div className="flex items-center gap-2">
                                                        <img src={selectedEvidence.image} alt={selectedEvidence.label} className="h-12 w-20 rounded border border-slate-700 object-cover" />
                                                        <div className="min-w-0">
                                                            <p className="truncate text-[11px] text-slate-200">{selectedEvidence.label}</p>
                                                            <div className="mt-1 flex flex-wrap gap-1">
                                                                {matchingEvidence.map((ev) => (
                                                                    <button key={ev.id} type="button" onClick={() => setSelectedEvidenceId(ev.id)} className={`rounded px-1.5 py-0.5 text-[10px] ${ev.id === selectedEvidence.id ? "bg-cyan-600 text-white" : "bg-slate-700 text-slate-300"}`}>{ev.label.split(" ")[0]}</button>
                                                                ))}
                                                            </div>
                                                        </div>
                                                    </div>
                                                )}
                                                <div className="rounded bg-[#06101f] p-2">
                                                    <p className="text-[11px] text-slate-300">Dispatch quality: <span className="font-semibold text-slate-100">{dispatchQuality.score}%</span></p>
                                                    <ul className="mt-1 space-y-0.5">
                                                        {dispatchQuality.checks.map((check) => (
                                                            <li key={check.label} className="flex items-center gap-1 text-[10px]">
                                                                <span className={check.ok ? "text-emerald-300" : "text-rose-300"}>{check.ok ? "✓" : "✗"}</span>
                                                                <span className="text-slate-400">{check.label}</span>
                                                            </li>
                                                        ))}
                                                    </ul>
                                                    <button type="button" onClick={handleRunDispatchQualityCheck} className="mt-1 rounded bg-slate-700 px-2 py-1 text-[10px] text-white hover:bg-slate-600">Run dispatch quality check</button>
                                                </div>
                                            </div>

                                            {/* Responder ranking */}
                                            <div className="space-y-2 rounded border border-slate-700/60 bg-[#08142a] p-2">
                                                <div className="flex items-center justify-between">
                                                    <p className="text-[10px] uppercase tracking-wide text-slate-400">WorkIQ responders</p>
                                                    <span className="text-[10px] text-slate-500">{responderAvailability.free} free · {responderAvailability.onCall} on-call</span>
                                                </div>
                                                <div className="flex flex-wrap items-center gap-1">
                                                    <div className="flex overflow-hidden rounded border border-slate-700 text-[10px]">
                                                        {(["all", "day", "swing", "night"] as const).map((s) => (
                                                            <button key={s} type="button" onClick={() => setResponderShiftFilter(s)} className={`px-1.5 py-0.5 capitalize ${responderShiftFilter === s ? "bg-cyan-600 text-white" : "bg-[#06101f] text-slate-300"}`}>{s}</button>
                                                        ))}
                                                    </div>
                                                    <label className="flex items-center gap-1 text-[10px] text-slate-400">
                                                        <input type="checkbox" checked={onCallOnly} onChange={(e) => setOnCallOnly(e.target.checked)} /> on-call
                                                    </label>
                                                </div>
                                                {suggestedResponders.length === 0 ? (
                                                    <p className="text-[11px] text-amber-300">No responders match the current filters — escalate for cover.</p>
                                                ) : (
                                                    <ul className="space-y-1">
                                                        {suggestedResponders.slice(0, 4).map((r, i) => (
                                                            <li key={r.id} className={`rounded p-1.5 ${i === 0 ? "border border-emerald-800/60 bg-emerald-950/30" : "bg-[#06101f]"}`}>
                                                                <div className="flex items-center gap-2">
                                                                    <img src={r.photo} alt={r.name} className="h-8 w-8 rounded-full border border-slate-700" />
                                                                    <div className="min-w-0 flex-1">
                                                                        <p className="truncate text-[11px] font-semibold text-slate-100">{r.name} <span className="font-normal text-slate-500">· score {r.score}</span></p>
                                                                        <p className="truncate text-[10px] text-slate-400">{r.role}</p>
                                                                    </div>
                                                                    <div className="flex flex-col gap-0.5">
                                                                        <button type="button" onClick={() => handleDispatchResponder(r)} className="rounded bg-emerald-600 px-1.5 py-0.5 text-[10px] font-medium text-white hover:bg-emerald-500">Dispatch</button>
                                                                        <button type="button" onClick={() => { setTechPopupResponderId(r.id); setTechPopupOpen(true); }} className="rounded bg-slate-700 px-1.5 py-0.5 text-[10px] text-white hover:bg-slate-600">View</button>
                                                                    </div>
                                                                </div>
                                                                <p className="mt-0.5 truncate text-[10px] text-slate-500" title={r.reason}>{r.reason}</p>
                                                            </li>
                                                        ))}
                                                    </ul>
                                                )}
                                                <button type="button" onClick={handleRaiseWorkOrder} className="w-full rounded bg-cyan-600 px-2 py-1 text-xs font-medium text-white hover:bg-cyan-500">Raise {suggestedPriority} work order</button>
                                            </div>

                                            {/* Escalation + SLA + close loop */}
                                            <div className="space-y-2 rounded border border-slate-700/60 bg-[#08142a] p-2">
                                                <p className="text-[10px] uppercase tracking-wide text-slate-400">Escalation & SLA</p>
                                                <div className="rounded bg-[#06101f] p-2">
                                                    <div className="flex items-center justify-between text-[11px]">
                                                        <span className="text-slate-400">SLA ({orderPriority})</span>
                                                        <span style={{ color: slaState.color }}>{slaState.label}</span>
                                                    </div>
                                                    <div className="mt-1 h-1.5 overflow-hidden rounded bg-slate-800">
                                                        <div className="h-full rounded" style={{ width: `${Math.round(slaState.fraction * 100)}%`, background: slaState.color }} />
                                                    </div>
                                                    <p className="mt-1 text-[10px] text-slate-500">
                                                        {selectedOpenOrder ? `Age ${orderAgeMin ?? 0}m of ${orderSlaMin}m · ${isSlaOverdue ? "overdue" : `${slaRemainingMin}m left`}` : `No open order · SLA budget ${orderSlaMin}m`}
                                                    </p>
                                                </div>
                                                <ol className="space-y-1">
                                                    {escalationTimeline.map((entry) => (
                                                        <li key={entry.id} className="flex items-center gap-2 text-[10px]">
                                                            <span className={entry.state === "done" ? "text-emerald-300" : entry.state === "current" ? "text-amber-300" : "text-slate-600"}>
                                                                {entry.state === "done" ? "●" : entry.state === "current" ? "◉" : "○"}
                                                            </span>
                                                            <span className="text-slate-300">{entry.label}</span>
                                                            <span className="ml-auto text-slate-500">{entry.note}</span>
                                                        </li>
                                                    ))}
                                                </ol>
                                                <div className="flex flex-wrap gap-1">
                                                    <button type="button" onClick={() => handleEscalateManager()} className="flex-1 rounded bg-amber-700 px-2 py-1 text-[10px] font-medium text-white hover:bg-amber-600">Escalate L1</button>
                                                    <button type="button" onClick={() => handleEscalateRegional()} disabled={!canEscalateRegional && escalationStage !== "manager"} className="flex-1 rounded bg-rose-700 px-2 py-1 text-[10px] font-medium text-white hover:bg-rose-600 disabled:opacity-40">Escalate L2</button>
                                                </div>
                                                <button type="button" onClick={handleCloseLoop} className="w-full rounded bg-emerald-700 px-2 py-1 text-[11px] font-medium text-white hover:bg-emerald-600">Close the loop</button>
                                                {needsEscalation && <p className="text-[10px] text-amber-300">Best match below {escalationThreshold} — consider escalation.</p>}
                                                <div>
                                                    <p className="mb-1 text-[10px] uppercase tracking-wide text-slate-500">Mission objectives</p>
                                                    <ul className="space-y-0.5">
                                                        {missionChallenge.objectives.map((o) => (
                                                            <li key={o.id} className="flex items-center gap-1 text-[10px]">
                                                                <span className={o.ok ? "text-emerald-300" : "text-slate-600"}>{o.ok ? "✓" : "○"}</span>
                                                                <span className="text-slate-400">{o.label}</span>
                                                                <span className="ml-auto text-slate-600">{o.detail}</span>
                                                            </li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            </div>
                                        </div>
                                        {woMessage && <p className="mt-2 rounded bg-[#06182f] px-2 py-1 text-[11px] text-cyan-200">{woMessage}</p>}
                                    </Panel>
                                </div>

                                <Panel
                                    title="Dispatch notes (ontology)"
                                    action={
                                        <button type="button" onClick={loadNotes} className="text-xs text-emerald-300 hover:text-emerald-100" title="Refresh from backend">
                                            {notesLoading ? "…" : "↻"}
                                        </button>
                                    }
                                >
                                    {notes.length === 0 ? (
                                        <p className="text-xs text-slate-400">No notes yet. Use the writeback form to persist one to the backend.</p>
                                    ) : (
                                        <ul className="space-y-1 text-xs">
                                            {notes.map((n) => (
                                                <li key={n.id ?? `${n.turbineId}-${n.createdAt}`} className="rounded bg-[#0b2a20aa] px-2 py-1">
                                                    <div className="flex items-center justify-between">
                                                        <span className="text-slate-200">{n.turbineId}</span>
                                                        <span style={{ color: STATUS_COLORS[n.status as PlantStatus] ?? "#94a3b8" }}>
                                                            {n.status} · {n.powerKw.toLocaleString()} kbd
                                                        </span>
                                                    </div>
                                                    {n.note && <p className="mt-0.5 truncate text-[11px] text-slate-400" title={n.note}>{n.note}</p>}
                                                </li>
                                            ))}
                                        </ul>
                                    )}
                                </Panel>

                                <div className="lg:col-span-2">
                                    <Panel
                                        title="Signal thresholds (ontology)"
                                        action={
                                            <span
                                                className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${thresholdsPublished != null ? "bg-emerald-900/60 text-emerald-300" : "bg-slate-700/60 text-slate-300"}`}
                                                title="Bands seeded from SIGNAL_METADATA and published to the Fabric SensorThreshold entity"
                                            >
                                                {thresholdsPublished != null ? `● Published to Fabric (${thresholdsPublished})` : "○ Pending"}
                                            </span>
                                        }
                                    >
                                        <p className="mb-2 text-[11px] text-slate-400">
                                            The single source of truth for warn / alarm bands. These drive unit status, the
                                            relationship-graph tooltips, and the digital-twin meters — and are published to the
                                            ontology backend so they are auditable outside the client.
                                        </p>
                                        <div className="overflow-hidden rounded border border-slate-700/60">
                                            <table className="w-full text-left text-xs">
                                                <thead className="bg-[#08142a] text-slate-400">
                                                    <tr>
                                                        <th className="px-2 py-1 font-medium">Signal</th>
                                                        <th className="px-2 py-1 font-medium">Ontology property</th>
                                                        <th className="px-2 py-1 font-medium">Bands</th>
                                                        <th className="px-2 py-1 font-medium">Governs health</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {thresholdRows().map((row) => (
                                                        <tr key={row.signalKey} className="border-t border-slate-800/80">
                                                            <td className="px-2 py-1 capitalize text-slate-200">{row.signalKey}</td>
                                                            <td className="px-2 py-1 text-cyan-200">{row.ontologyProperty}</td>
                                                            <td className="px-2 py-1 text-slate-300">{formatBand(row)}</td>
                                                            <td className="px-2 py-1">
                                                                {row.governsHealth
                                                                    ? <span className="text-emerald-300">yes</span>
                                                                    : <span className="text-slate-500">no</span>}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    </Panel>
                                </div>

                                <div className="lg:col-span-2">
                                    <Panel title="Ask Fabric IQ · operations & orders">
                                        <div className="flex gap-2">
                                            <input
                                                value={question}
                                                onChange={(e) => setQuestion(e.target.value)}
                                                onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); void runAsk(); } }}
                                                placeholder="Ask about this unit, its site, or what order to raise…"
                                                className="flex-1 rounded border border-slate-700 bg-[#08142a] px-2 py-1.5 text-sm"
                                            />
                                            <button type="button" onClick={() => void runAsk()} disabled={askLoading} className="rounded bg-cyan-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50">{askLoading ? "Asking…" : "Ask"}</button>
                                        </div>
                                        {askResult && <p className="mt-2 text-sm leading-relaxed text-slate-200">{askResult.summary}</p>}
                                    </Panel>
                                </div>
                            </div>
                        </div>
                    )}

                    {view === "ask" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mx-auto max-w-3xl space-y-3">
                                <Panel
                                    title="Ask Fabric IQ"
                                    action={
                                        <span className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${isDataAgentConfigured() ? "bg-emerald-900/60 text-emerald-300" : "bg-slate-700/60 text-slate-300"}`}>
                                            {isDataAgentConfigured() ? "● Live Data Agent" : "○ Local engine"}
                                        </span>
                                    }
                                >
                                    <p className="text-xs text-slate-400">
                                        {isDataAgentConfigured()
                                            ? "Routed to the deployed Fabric Data Agent (Lakehouse/Eventhouse), with a local fallback."
                                            : "Answered by a deterministic in-browser engine grounded in simulated telemetry, ontology sites, and dispatch-note history. Set VITE_DATA_AGENT_URL to route to a real Fabric Data Agent."}
                                    </p>
                                    <textarea
                                        value={question}
                                        onChange={(e) => setQuestion(e.target.value)}
                                        onKeyDown={(e) => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); void runAsk(); } }}
                                        rows={3}
                                        placeholder="Ask about throughput, alarms, utilization, unit temperature, dispatch notes…  (Ctrl+Enter to send)"
                                        className="mt-2 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1.5 text-sm"
                                    />
                                    <div className="mt-2 flex flex-wrap gap-1">
                                        {SUGGESTED.map((q) => (
                                            <button
                                                key={q}
                                                type="button"
                                                onClick={() => { setQuestion(q); void runAsk(q); }}
                                                className="rounded-full border border-slate-700 bg-[#0a1830] px-2.5 py-1 text-[11px] text-slate-300 hover:border-cyan-500 hover:text-cyan-200"
                                            >
                                                {q}
                                            </button>
                                        ))}
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => void runAsk()}
                                        disabled={askLoading}
                                        className="mt-2 w-full rounded bg-cyan-600 px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
                                    >
                                        {askLoading ? "Asking…" : "Ask Question"}
                                    </button>
                                    {askError && <p className="mt-2 text-xs text-red-300">{askError}</p>}
                                    <div className="mt-2 space-y-2">
                                        <button
                                            type="button"
                                            onClick={() => void runAgentConnectionCheck()}
                                            disabled={agentCheckLoading}
                                            className="flex items-center gap-1.5 rounded-lg border border-slate-600 bg-[#0a1830] px-3 py-1.5 text-xs font-medium text-slate-200 transition-all duration-200 hover:border-cyan-500 hover:text-cyan-200 disabled:opacity-50"
                                        >
                                            <span aria-hidden="true">🔌</span>{agentCheckLoading ? "Testing…" : "Test Data Agent"}
                                        </button>
                                        {agentCheckResult && (
                                            <div className={`rounded-lg border p-2 text-xs ${agentCheckResult.ok ? "border-emerald-700/70 bg-emerald-900/20 text-emerald-200" : "border-red-800/70 bg-red-900/20 text-red-200"}`}>
                                                <p className="font-medium">{agentCheckResult.message}</p>
                                                <div className="mt-1 flex flex-wrap gap-3 text-[11px] opacity-90">
                                                    <span>Mode: {agentCheckResult.mode.toUpperCase()}</span>
                                                    <span>Auth: {agentCheckResult.authScheme}</span>
                                                    {agentCheckResult.transportUsed && <span>Transport: {agentCheckResult.transportUsed.toUpperCase()}</span>}
                                                    {agentCheckResult.transportTried.length > 0 && (
                                                        <span>Tried: {agentCheckResult.transportTried.map((t) => t.toUpperCase()).join(" → ")}</span>
                                                    )}
                                                </div>
                                                {agentCheckResult.url && <p className="mt-1 break-all text-[11px] opacity-90">URL: {agentCheckResult.url}</p>}
                                                {agentCheckResult.sampleAnswer && (
                                                    <p className="mt-1 text-[11px] opacity-90">Sample: {agentCheckResult.sampleAnswer.slice(0, 140)}</p>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                    {askResult && (
                                        <div className="mt-3 rounded border border-slate-700 bg-[#081226] p-3 text-sm text-slate-200">
                                            <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-400">
                                                <span>Source: {sourceLabel(askResult.source)}</span>
                                                <span className="flex items-center gap-2">
                                                    {askResult.source === "fabriciq" && askResult.transport && (
                                                        <span>Transport: {askResult.transport.toUpperCase()}</span>
                                                    )}
                                                    {askResult.fallbackReason && <span>Fallback: {askResult.fallbackReason}</span>}
                                                    {typeof askResult.confidence === "number" && <span>Confidence: {Math.round(askResult.confidence * 100)}%</span>}
                                                    {askResult.cacheHit && <span>cached</span>}
                                                    <span>{new Date(askResult.generatedAt).toLocaleTimeString()}</span>
                                                </span>
                                            </div>
                                            <p className="mt-1">{askResult.summary}</p>
                                            {askResult.evidence && askResult.evidence.length > 0 && (
                                                <ul className="mt-2 list-disc pl-4 text-xs text-slate-400">
                                                    {askResult.evidence.slice(0, 3).map((ev, i) => (
                                                        <li key={`${ev}-${i}`}>{ev}</li>
                                                    ))}
                                                </ul>
                                            )}
                                            {askResult.queryText && <p className="mt-2 text-xs text-slate-400">Trace: {askResult.queryText}</p>}
                                        </div>
                                    )}
                                    {askHistory.length > 0 && (
                                        <div className="mt-3 rounded border border-slate-700 bg-[#071424] p-3 text-xs text-slate-300">
                                            <p className="mb-2 font-medium text-slate-200">Recent Ask Diagnostics</p>
                                            <div className="space-y-1.5">
                                                {askHistory.map((h, i) => (
                                                    <div key={`${h.at}-${i}`} className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800 pb-1 last:border-b-0 last:pb-0">
                                                        <span className="truncate text-slate-400" title={h.question}>{h.question}</span>
                                                        <span className="flex items-center gap-2 text-slate-400">
                                                            <span>{sourceLabel(h.source)}</span>
                                                            {h.source === "fabriciq" && h.transport && <span>{h.transport.toUpperCase()}</span>}
                                                            {h.fallbackReason && <span>fallback:{h.fallbackReason}</span>}
                                                            <span>{h.latencyMs}ms</span>
                                                            {h.cacheHit && <span>cached</span>}
                                                        </span>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </Panel>
                            </div>
                        </div>
                    )}
                </section>
            </div>

            {detailOpen && selected && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={() => setDetailOpen(false)}>
                    <div className="w-full max-w-lg rounded-xl border border-slate-700 bg-[#07142a] p-5 shadow-[0_24px_70px_rgba(0,0,0,0.6)]" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-start justify-between">
                            <div>
                                <p className="text-[11px] uppercase tracking-wide text-cyan-300">Unit detail</p>
                                <h2 className="text-xl font-semibold">{selected.id}</h2>
                                <p className="text-sm text-slate-400">{selected.siteName} · {selected.latitude.toFixed(2)}, {selected.longitude.toFixed(2)}</p>
                            </div>
                            <button type="button" onClick={() => setDetailOpen(false)} className="rounded p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100">✕</button>
                        </div>

                        <span
                            className="mt-2 inline-block rounded-full px-2 py-0.5 text-xs font-medium"
                            style={{ backgroundColor: `${STATUS_COLORS[selected.status]}22`, color: STATUS_COLORS[selected.status] }}
                        >
                            {selected.status.toUpperCase()}
                        </span>

                        <dl className="mt-3 grid grid-cols-2 gap-x-6 gap-y-2 text-sm text-slate-300">
                            <div className="flex justify-between"><dt className="text-slate-400">Throughput</dt><dd>{selected.powerKw.toLocaleString()} kbd</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Feed rate</dt><dd>{selected.irradianceWm2} kbd</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Unit temp</dt><dd>{selected.moduleTempC} °C</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Utilization</dt><dd>{selected.inverterLoadPct} %</dd></div>
                        </dl>

                        <div className="mt-3">
                            <div className="mb-1 flex items-center justify-between gap-2">
                                <p className="text-xs text-slate-400">Throughput trend (live)</p>
                                <div className="flex overflow-hidden rounded border border-slate-700 text-[10px]">
                                    {HISTORY_WINDOWS.map((window) => (
                                        <button key={window} type="button" onClick={() => setHistoryWindow(window)} className={`px-2 py-1 ${historyWindow === window ? "bg-cyan-600 text-white" : "bg-[#08142a] text-slate-300"}`}>
                                            {window}
                                        </button>
                                    ))}
                                </div>
                            </div>
                            <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} />
                        </div>
                        <div className="mt-2 flex items-center justify-between rounded border border-cyan-900/60 bg-[#06182f] px-3 py-1.5 text-xs">
                            <span className="text-slate-400">Forecast (+{forecastHorizon} ticks)</span>
                            <span className="font-semibold text-cyan-200">{forecast.toLocaleString()} kbd</span>
                        </div>

                        <div className="mt-4 flex gap-2">
                            <button type="button" onClick={() => { goToOps(); setDetailOpen(false); }} className="flex-1 rounded bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500">Open in Operations</button>
                            <button type="button" onClick={() => { setView("analytics"); setDetailOpen(false); }} className="flex-1 rounded bg-slate-700 px-3 py-2 text-sm font-medium text-white hover:bg-slate-600">View analytics</button>
                        </div>
                    </div>
                </div>
            )}

            {techPopupOpen && techPopupResponder && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={() => setTechPopupOpen(false)}>
                    <div className="w-full max-w-md rounded-xl border border-slate-700 bg-[#07142a] p-5 shadow-[0_24px_70px_rgba(0,0,0,0.6)]" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-start justify-between">
                            <div className="flex items-center gap-3">
                                <img src={techPopupResponder.photo} alt={techPopupResponder.name} className="h-14 w-14 rounded-full border border-slate-700" />
                                <div>
                                    <p className="text-[11px] uppercase tracking-wide text-cyan-300">WorkIQ responder</p>
                                    <h2 className="text-lg font-semibold">{techPopupResponder.name}</h2>
                                    <p className="text-sm text-slate-400">{techPopupResponder.role}</p>
                                </div>
                            </div>
                            <button type="button" onClick={() => setTechPopupOpen(false)} className="rounded p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100">✕</button>
                        </div>
                        <dl className="mt-3 grid grid-cols-2 gap-x-6 gap-y-1 text-sm text-slate-300">
                            <div className="flex justify-between"><dt className="text-slate-400">Shift</dt><dd className="capitalize">{techPopupResponder.shift}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">On-call</dt><dd className={techPopupResponder.onCall ? "text-emerald-300" : "text-slate-500"}>{techPopupResponder.onCall ? "yes" : "no"}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">ETA</dt><dd>{techPopupResponder.etaMin} min</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Match score</dt><dd className="text-cyan-200">{techPopupResponder.score}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Focus asset</dt><dd>{techPopupFocusComponent}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Active load</dt><dd>{techPopupResponder.currentLoad}</dd></div>
                        </dl>
                        <p className="mt-2 text-[11px] text-slate-500">{techPopupResponder.reason}</p>
                        {techPopupEvidence && (
                            <div className="mt-3 flex items-center gap-2 rounded border border-slate-700/60 bg-[#08142a] p-2">
                                <img src={techPopupEvidence.image} alt={techPopupEvidence.label} className="h-12 w-20 rounded border border-slate-700 object-cover" />
                                <div>
                                    <p className="text-[11px] text-slate-200">{techPopupEvidence.label}</p>
                                    <p className="text-[10px] text-slate-500">Captured {new Date(techPopupEvidence.capturedAt).toLocaleTimeString()}</p>
                                </div>
                            </div>
                        )}
                        <div className="mt-4 flex gap-2">
                            <button type="button" onClick={() => { void handleDispatchResponder(techPopupResponder); setTechPopupOpen(false); }} className="flex-1 rounded bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500">Dispatch {techPopupResponder.name.split(" ")[0]}</button>
                            <button type="button" onClick={() => { setWoAssignee(techPopupResponder.name); setTechPopupOpen(false); }} className="flex-1 rounded bg-slate-700 px-3 py-2 text-sm font-medium text-white hover:bg-slate-600">Assign only</button>
                        </div>
                    </div>
                </div>
            )}

            {demoIntroOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" onClick={() => setDemoIntroOpen(false)}>
                    <div className="w-full max-w-lg rounded-xl border border-cyan-800/60 bg-[#07142a] p-6 shadow-[0_24px_70px_rgba(0,0,0,0.6)]" onClick={(e) => e.stopPropagation()}>
                        <p className="text-[11px] uppercase tracking-wide text-cyan-300">Guided demo</p>
                        <h2 className="mt-1 text-xl font-semibold">{narrateStep(REFINERY_DEMO_MANIFEST, "story").title}</h2>
                        <p className="mt-2 text-sm leading-relaxed text-slate-300">{narrateStep(REFINERY_DEMO_MANIFEST, "story").caption}</p>
                        <ol className="mt-3 grid grid-cols-2 gap-1 text-[11px] text-slate-400">
                            {demoScriptSteps.map((s) => (
                                <li key={s.id} className="rounded bg-[#08142a] px-2 py-1">{s.label}</li>
                            ))}
                        </ol>
                        <div className="mt-4 flex gap-2">
                            <button type="button" onClick={handleStartDemoFromIntro} className="flex-1 rounded bg-cyan-600 px-3 py-2 text-sm font-semibold text-white hover:bg-cyan-500">▶ Run the full walkthrough</button>
                            <button type="button" onClick={() => setDemoIntroOpen(false)} className="rounded bg-slate-700 px-3 py-2 text-sm font-medium text-white hover:bg-slate-600">Close</button>
                        </div>
                    </div>
                </div>
            )}

            {reportModal && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" onClick={() => setReportModal(null)}>
                    <div className="w-full max-w-lg rounded-xl border border-emerald-800/60 bg-[#07142a] p-5 shadow-[0_24px_70px_rgba(0,0,0,0.6)]" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-start justify-between">
                            <div>
                                <p className="text-[11px] uppercase tracking-wide text-emerald-300">Mission report</p>
                                <h2 className="text-xl font-semibold">{reportModal.turbineId} · {reportModal.siteName}</h2>
                            </div>
                            <button type="button" onClick={() => setReportModal(null)} className="rounded p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100">✕</button>
                        </div>
                        <dl className="mt-3 grid grid-cols-2 gap-x-6 gap-y-1 text-sm text-slate-300">
                            <div className="flex justify-between"><dt className="text-slate-400">Probable asset</dt><dd>{reportModal.component}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Priority</dt><dd>{reportModal.priority}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Responder</dt><dd>{reportModal.responder}</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Dispatch quality</dt><dd>{reportModal.dispatchQualityScore}%</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Challenge</dt><dd className="capitalize">{reportModal.challengeVerdict} · {reportModal.challengeScore}%</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Duration</dt><dd>{(reportModal.durationMs / 1000).toFixed(1)}s · {reportModal.stepCount} steps</dd></div>
                            <div className="col-span-2 flex justify-between border-t border-slate-700/60 pt-1"><dt className="text-slate-400">Outcome</dt><dd className="text-emerald-300">{reportModal.outcome}</dd></div>
                        </dl>
                        <div className="mt-3 max-h-40 space-y-1 overflow-y-auto rounded border border-slate-700/60 bg-[#08142a] p-2">
                            {reportModal.events.map((ev, i) => (
                                <div key={`${ev.step}-${i}`} className="flex items-baseline gap-2 text-[11px]">
                                    <span className="w-16 shrink-0 capitalize text-cyan-300">{ev.step}</span>
                                    <span className="text-slate-400">{ev.detail}</span>
                                </div>
                            ))}
                        </div>
                        {runHistorySummaries.length > 1 && (
                            <div className="mt-2 text-[10px] text-slate-500">
                                Run history: {runHistorySummaries.slice(0, 5).map((r) => `${r.turbineId} (${r.challengeScore}%)`).join(" · ")}
                            </div>
                        )}
                        <div className="mt-4 flex gap-2">
                            <button type="button" onClick={handleDownloadMissionReport} className="flex-1 rounded bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500">⬇ Download JSON</button>
                            <button type="button" onClick={() => setReportModal(null)} className="rounded bg-slate-700 px-3 py-2 text-sm font-medium text-white hover:bg-slate-600">Close</button>
                        </div>
                    </div>
                </div>
            )}
        </main>
    );
}

export default App;
