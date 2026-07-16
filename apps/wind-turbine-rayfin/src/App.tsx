//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState, type ReactNode, type PointerEvent as ReactPointerEvent, type WheelEvent as ReactWheelEvent } from "react";
import * as THREE from "three";
import {
    answerQuestion,
    askOntology,
    createTurbineDevice,
    deleteTurbineDevice,
    ensureTurbineDevices,
    ensureOntologySites,
    ensureSignalThresholds,
    listTurbineDevices,
    listSignalThresholds,
    recentDispatchNotes,
    recentMaintenanceOrders,
    saveDispatchNote,
    saveMaintenanceOrder,
    type DispatchNoteRecord,
    type MaintenanceOrderRecord,
    type SignalThresholdRecord,
    type TurbineDeviceRecord,
    updateTurbineDevice,
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
import {
    buildMissionChallenge,
    buildMissionReport,
    buildEscalationTimeline,
    pushMissionRun,
    summarizeMissionRun,
    summarizeResponderAvailability,
    slaUrgency,
    demoNarration,
    type DemoScriptStepId,
    type EscalationStage,
    type MissionReport,
    type MissionReportEvent,
} from "@/services/demo-experience.service";
export {
    buildMissionChallenge,
    buildMissionReport,
    buildEscalationTimeline,
    pushMissionRun,
    summarizeMissionRun,
    summarizeResponderAvailability,
    slaUrgency,
    demoNarration,
    narrateStep,
    getDemoManifest,
    WIND_DEMO_MANIFEST,
    SOLAR_DEMO_MANIFEST,
    REFINERY_DEMO_MANIFEST,
    DEMO_MANIFESTS,
    DEMO_STEP_ORDER,
} from "@/services/demo-experience.service";
export type {
    DemoScriptStepId,
    EscalationStage,
    EscalationTimelineEntry,
    EscalationTimelineState,
    MissionChallengeInput,
    MissionChallengeObjective,
    MissionChallengeResult,
    MissionReport,
    MissionReportEvent,
    MissionReportInput,
    MissionRunSummary,
    ResponderAvailabilityInput,
    ResponderAvailabilitySummary,
    SlaUrgency,
    DemoNarration,
    DomainDemoManifest,
    DemoManifestStep,
} from "@/services/demo-experience.service";
import { SceneErrorBoundary } from "@/components/SceneErrorBoundary";
import { isTeamsAlertConfigured, postTeamsAlert } from "@/services/teams-alert.service";
import type { TwinPartKey } from "@/scenes/TurbineTwinScene";

type TurbineStatus = "healthy" | "warning" | "alarm";

type TurbineTelemetry = {
    id: string;
    siteId: string;
    siteName: string;
    latitude: number;
    longitude: number;
    x: number;
    z: number;
    powerKw: number;
    windMs: number;
    nacelleTempC: number;
    vibrationMmS: number;
    status: TurbineStatus;
};

type WindSite = {
    id: string;
    name: string;
    country: string;
    lat: number;
    lon: number;
    turbineCount: number;
    capacityMw: number;
};

type TwinPartLayer = {
    key: TwinPartKey;
    title: string;
    property: string;
    value: string;
    status: TurbineStatus;
    note: string;
};

export type TwinDeviceKey =
    | "rotor.pitch-control"
    | "rotor.hub-bearing"
    | "nacelle.generator"
    | "nacelle.converter"
    | "drivetrain.gearbox"
    | "drivetrain.main-shaft"
    | "base.transformer"
    | "base.control-cabinet";

export type TwinDeviceNode = {
    key: TwinDeviceKey;
    component: TwinPartKey;
    label: string;
    property: string;
    unit: string;
    note: string;
    anchor: [number, number, number];
    lookAt: [number, number, number];
    offset: [number, number, number];
    zoom: number;
    value: (t: TurbineTelemetry) => string;
    status: (t: TurbineTelemetry) => TurbineStatus;
};

type TwinDeviceDraft = {
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

type TurbineRenderRefs = {
    blades: THREE.Group;
    nacelleMat: THREE.MeshStandardMaterial;
    ringMat: THREE.MeshBasicMaterial;
    ring: THREE.Mesh;
    spin: number;
};

type SceneState = {
    byId: Map<string, TurbineRenderRefs>;
    siteRefs: Map<string, SiteRenderRefs>;
    cleanup: () => void;
};

type SiteRenderRefs = {
    marker: THREE.Mesh;
    markerMat: THREE.MeshStandardMaterial;
    glow: THREE.Mesh;
    glowMat: THREE.MeshBasicMaterial;
    baseY: number;
};

export const STATUS_COLORS: Record<TurbineStatus, string> = {
    healthy: "#58d68d",
    warning: "#ffd166",
    alarm: "#ef476f",
};

export const SITE_COLORS = ["#b8c1cc", "#9ea9b6", "#8b97a5", "#788493", "#666f7d", "#555d6a"];

const SITES: WindSite[] = [
    { id: "SITE-TX", name: "Panhandle Ridge", country: "US", lat: 35.2, lon: -101.9, turbineCount: 6, capacityMw: 4.8 },
    { id: "SITE-DK", name: "Jutland Coast", country: "DK", lat: 56.4, lon: 8.8, turbineCount: 5, capacityMw: 5.1 },
    { id: "SITE-IN", name: "Gujarat Flats", country: "IN", lat: 22.6, lon: 69.7, turbineCount: 7, capacityMw: 3.9 },
    { id: "SITE-GB", name: "Moray Belt", country: "GB", lat: 57.8, lon: -3.9, turbineCount: 4, capacityMw: 6.2 },
    { id: "SITE-MA", name: "Tarfaya Wind Zone", country: "MA", lat: 27.9, lon: -12.9, turbineCount: 5, capacityMw: 4.6 },
    { id: "SITE-BR", name: "Sergipe Shore", country: "BR", lat: -10.9, lon: -37.1, turbineCount: 4, capacityMw: 4.3 },
];

export function seededRand(seed: number) {
    const s = Math.sin(seed) * 10000;
    return s - Math.floor(s);
}

type DemoScriptStep = {
    id: DemoScriptStepId;
    label: string;
    detail: string;
    action: () => Promise<unknown> | void;
};

export function projectLonToX(lon: number) {
    return ((lon + 180) / 360) * 92 - 46;
}

export function projectLatToZ(lat: number) {
    // Equirectangular is a 2:1 projection (360° lon × 180° lat). With the map
    // plane 92 units wide, the depth must be 46 (92 / 2) so continents keep their
    // true proportions instead of being stretched vertically.
    return ((90 - lat) / 180) * 46 - 23;
}

// Real-world coastline outlines in [lon, lat], anchored so turbine sites land on
// their true continents. Detailed enough to read as a recognizable world map.
const WORLD: number[][][] = [
    // North America (incl. Florida, Gulf of Mexico, Baja, Alaska)
    [[-166, 68], [-157, 71], [-150, 70], [-140, 70], [-125, 70], [-110, 69], [-95, 70], [-85, 73], [-80, 70], [-78, 62], [-64, 60], [-60, 52], [-66, 48], [-70, 45], [-74, 40], [-76, 36], [-81, 31], [-80, 25], [-83, 28], [-90, 30], [-94, 29], [-97, 26], [-97, 22], [-105, 22], [-110, 24], [-114, 28], [-117, 32], [-121, 35], [-124, 40], [-124, 46], [-125, 49], [-130, 54], [-136, 58], [-145, 60], [-152, 59], [-158, 63], [-163, 66]],
    // Greenland
    [[-46, 60], [-50, 64], [-54, 68], [-58, 72], [-55, 76], [-48, 79], [-38, 82], [-25, 82], [-18, 78], [-20, 73], [-26, 69], [-33, 66], [-40, 63]],
    // South America (Brazil bulge, Cape Horn)
    [[-78, 8], [-72, 11], [-62, 10], [-52, 5], [-50, 0], [-44, -2], [-38, -5], [-35, -8], [-39, -14], [-43, -23], [-48, -26], [-54, -33], [-58, -35], [-63, -41], [-66, -45], [-69, -50], [-72, -53], [-74, -50], [-71, -44], [-72, -38], [-71, -30], [-70, -23], [-70, -18], [-74, -15], [-77, -12], [-79, -6], [-81, -4], [-80, 1], [-78, 5]],
    // Africa (Gulf of Guinea, Horn of Africa, Med + Red Sea coasts)
    [[-16, 21], [-13, 28], [-9, 32], [-6, 35], [-1, 36], [5, 36], [12, 33], [18, 31], [24, 32], [30, 31], [33, 30], [34, 26], [35, 22], [37, 18], [40, 15], [42, 12], [44, 11], [48, 11], [51, 9], [51, 6], [49, 2], [45, -1], [43, -5], [41, -11], [39, -16], [35, -21], [33, -25], [32, -29], [28, -33], [24, -34], [20, -34], [18, -29], [16, -23], [15, -18], [13, -12], [12, -6], [9, -2], [9, 2], [8, 4], [4, 6], [-2, 5], [-8, 5], [-14, 9], [-17, 15]],
    // Madagascar
    [[44, -16], [47, -15], [50, -20], [48, -25], [45, -24], [43, -19]],
    // Europe (Iberia, France, Italy, Scandinavia, Baltic)
    [[-10, 37], [-9, 41], [-9, 44], [-4, 44], [-1, 46], [-4, 48], [-1, 49], [2, 51], [4, 52], [7, 53], [7, 55], [10, 58], [6, 58], [5, 60], [8, 62], [11, 64], [15, 67], [20, 69], [26, 71], [28, 69], [26, 65], [30, 62], [30, 57], [27, 57], [23, 56], [20, 55], [16, 54], [13, 54], [12, 50], [14, 46], [18, 46], [19, 42], [16, 40], [14, 42], [12, 45], [8, 44], [4, 43], [2, 42], [-3, 43]],
    // Great Britain
    [[-5, 50], [-4, 53], [-5, 55], [-3, 57], [-2, 58], [0, 58], [1, 53], [1, 52], [-2, 50]],
    // Ireland
    [[-10, 52], [-10, 54], [-7, 55], [-6, 53], [-7, 51]],
    // Asia (Arabia, India peninsula, SE Asia, China, Korea, Kamchatka, Siberia)
    [[30, 57], [40, 55], [50, 52], [55, 48], [52, 45], [50, 42], [48, 41], [45, 40], [44, 37], [40, 37], [36, 36], [36, 31], [34, 28], [38, 25], [44, 24], [50, 27], [56, 25], [60, 24], [63, 25], [66, 25], [68, 23], [70, 21], [72, 17], [74, 13], [77, 8], [79, 10], [80, 15], [83, 18], [87, 21], [89, 22], [91, 21], [94, 18], [97, 16], [99, 10], [103, 6], [105, 9], [105, 14], [108, 18], [110, 21], [113, 22], [117, 23], [120, 26], [122, 30], [121, 34], [123, 37], [126, 39], [127, 42], [130, 43], [131, 45], [135, 44], [138, 46], [140, 46], [143, 49], [142, 52], [145, 54], [150, 57], [154, 59], [159, 62], [164, 66], [170, 66], [177, 68], [180, 70], [172, 72], [160, 71], [148, 73], [135, 73], [120, 74], [105, 76], [90, 76], [78, 74], [70, 73], [62, 71], [55, 70], [48, 68], [40, 66], [34, 62], [31, 59]],
    // Japan
    [[132, 33], [136, 35], [139, 37], [141, 40], [142, 42], [140, 42], [137, 36], [134, 33]],
    // Australia
    [[114, -22], [114, -27], [116, -31], [119, -34], [124, -33], [129, -32], [131, -31], [134, -33], [138, -35], [141, -38], [144, -38], [147, -38], [150, -37], [153, -31], [153, -28], [152, -25], [149, -21], [146, -19], [143, -14], [141, -17], [137, -12], [135, -15], [131, -12], [128, -15], [124, -16], [122, -18], [118, -20]],
    // Tasmania
    [[145, -41], [148, -41], [147, -43], [144, -43]],
    // Central America bridge
    [[-98, 22], [-95, 20], [-91, 18], [-88, 19], [-86, 17], [-84, 10], [-78, 8], [-80, 6], [-86, 8], [-90, 11], [-93, 15], [-97, 18]],
    // Caribbean arc
    [[-84, 23], [-81, 24], [-79, 23], [-77, 22], [-75, 21], [-73, 20], [-71, 19], [-69, 18], [-67, 18], [-66, 17], [-68, 16], [-71, 16], [-74, 17], [-77, 18], [-80, 19], [-82, 21]],
    // Arabian Peninsula (separate silhouette for readability)
    [[35, 31], [39, 32], [45, 30], [50, 26], [55, 24], [56, 20], [52, 16], [47, 14], [43, 16], [40, 20], [36, 24]],
    // India peninsula detail
    [[68, 23], [71, 23], [74, 21], [77, 18], [79, 13], [80, 8], [78, 8], [75, 12], [73, 16], [70, 20]],
    // Sumatra
    [[95, 5], [100, 3], [103, 0], [105, -3], [102, -5], [98, -1], [95, 3]],
    // Java
    [[105, -6], [110, -6], [114, -7], [112, -9], [107, -9], [105, -8]],
    // Borneo
    [[109, 3], [114, 5], [118, 4], [119, 0], [116, -3], [111, -3], [109, 1]],
    // Philippines
    [[120, 18], [122, 16], [123, 14], [124, 12], [123, 10], [121, 9], [120, 11], [119, 14]],
    // New Guinea
    [[131, -2], [137, -3], [143, -4], [149, -7], [145, -9], [138, -8], [133, -6]],
    // New Zealand
    [[173, -35], [176, -37], [178, -41], [174, -43], [169, -46], [166, -45], [169, -41], [171, -38]],
    // Antarctica fringe (stylized cap)
    [[-180, -62], [-160, -64], [-130, -66], [-95, -67], [-60, -68], [-20, -69], [20, -69], [60, -68], [95, -67], [130, -66], [160, -64], [180, -62], [180, -74], [-180, -74]],
];

// Tapered, slightly twisted turbine blade (length along +Y).
export function createBladeGeometry() {
    const shape = new THREE.Shape();
    shape.moveTo(0.16, 0);
    shape.lineTo(0.22, 0.3);
    shape.lineTo(0.15, 1.2);
    shape.lineTo(0.09, 2.0);
    shape.lineTo(0.04, 2.5);
    shape.lineTo(0.0, 2.6);
    shape.lineTo(-0.05, 2.5);
    shape.lineTo(-0.1, 2.0);
    shape.lineTo(-0.16, 1.2);
    shape.lineTo(-0.2, 0.3);
    shape.lineTo(-0.14, 0);
    shape.closePath();
    const geo = new THREE.ExtrudeGeometry(shape, {
        depth: 0.06,
        bevelEnabled: true,
        bevelThickness: 0.02,
        bevelSize: 0.02,
        bevelSegments: 1,
        steps: 1,
    });
    geo.translate(0, 0, -0.04);
    return geo;
}

type SignalKey = "power" | "wind" | "temp" | "vibration";

interface SignalMetadata {
    key: SignalKey;
    label: string;
    ontologyProperty: string;
    unit: string;
    warn: number;
    alarm: number;
    governsHealth: boolean;
    get: (t: TurbineTelemetry) => number;
}

// Single source of truth for signal bands, mirrored from the WindTurbine ontology
// Sensor metadata (Unit, MinThreshold/MaxThreshold) and the Data Agent threshold
// guidance (e.g. vibration: normal <4, warning 4–6, high 6–8, critical >8 mm/s).
// Health is governed only by condition signals (temp, vibration); power/wind are
// informational context, so they never drive a turbine's alarm status.
const SIGNAL_METADATA: Record<SignalKey, SignalMetadata> = {
    power: { key: "power", label: "Power", ontologyProperty: "PowerOutputKW", unit: "kW", warn: Infinity, alarm: Infinity, governsHealth: false, get: (t) => t.powerKw },
    wind: { key: "wind", label: "Wind", ontologyProperty: "WindSpeedMs", unit: "m/s", warn: 16, alarm: 22, governsHealth: false, get: (t) => t.windMs },
    temp: { key: "temp", label: "Nacelle", ontologyProperty: "GeneratorTempC", unit: "\u00b0C", warn: 67, alarm: 79, governsHealth: true, get: (t) => t.nacelleTempC },
    vibration: { key: "vibration", label: "Vibration", ontologyProperty: "VibrationMmS", unit: "mm/s", warn: 4, alarm: 8, governsHealth: true, get: (t) => t.vibrationMmS },
};

const SIGNAL_ORDER: SignalKey[] = ["power", "wind", "temp", "vibration"];
const STATUS_RANK: Record<TurbineStatus, number> = { healthy: 0, warning: 1, alarm: 2 };

// Runtime warn/alarm overrides sourced from the ontology backend (SensorThreshold
// store) at startup. Empty by default so the pure classifiers fall back to the
// static SIGNAL_METADATA bands — keeping default behaviour and unit tests
// deterministic. Applying overrides is fallback-safe: an unreachable backend
// simply leaves the compiled-in defaults in place.
const THRESHOLD_OVERRIDES = new Map<SignalKey, { warn: number; alarm: number }>();

function isSignalKey(key: string): key is SignalKey {
    return key === "power" || key === "wind" || key === "temp" || key === "vibration";
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
export function signalState(key: SignalKey, value: number): TurbineStatus {
    const m = activeBands(key);
    if (value >= m.alarm) return "alarm";
    if (value >= m.warn) return "warning";
    return "healthy";
}

export function signalColor(key: SignalKey, value: number): string {
    return STATUS_COLORS[signalState(key, value)];
}

// Overall turbine status = worst band across the health-governing signals only.
export function deriveTurbineStatus(nacelleTempC: number, vibrationMmS: number): TurbineStatus {
    let worst: TurbineStatus = "healthy";
    for (const key of SIGNAL_ORDER) {
        if (!SIGNAL_METADATA[key].governsHealth) {
            continue;
        }
        const value = key === "temp" ? nacelleTempC : vibrationMmS;
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
    turbineCount: number;
    totalKw: number;
    ratedMw: number;
    capacityFactor: number;
    alarms: number;
    warnings: number;
    healthy: number;
    avgWindMs: number;
    avgNacelleTempC: number;
    avgVibrationMmS: number;
}

// Pure per-site aggregation for the site drill-down view: rolls each site's
// turbines into scoped KPIs (output, capacity factor, health counts, averages).
// Sites with no turbines report zeros so the UI can render them safely.
export function summarizeSites(
    turbines: { siteId: string; status: TurbineStatus; powerKw: number; windMs: number; nacelleTempC: number; vibrationMmS: number }[],
    sites: { id: string; name: string; capacityMw: number }[],
): SiteSummary[] {
    return sites.map((site) => {
        const local = turbines.filter((t) => t.siteId === site.id);
        const n = local.length;
        const totalKw = local.reduce((sum, t) => sum + t.powerKw, 0);
        const alarms = local.filter((t) => t.status === "alarm").length;
        const warnings = local.filter((t) => t.status === "warning").length;
        const ratedMw = site.capacityMw * n;
        const avg = (sel: (t: (typeof local)[number]) => number) => (n > 0 ? local.reduce((sum, t) => sum + sel(t), 0) / n : 0);
        return {
            id: site.id,
            name: site.name,
            turbineCount: n,
            totalKw,
            ratedMw,
            capacityFactor: ratedMw > 0 ? (totalKw / 1000 / ratedMw) * 100 : 0,
            alarms,
            warnings,
            healthy: n - alarms - warnings,
            avgWindMs: +avg((t) => t.windMs).toFixed(1),
            avgNacelleTempC: +avg((t) => t.nacelleTempC).toFixed(1),
            avgVibrationMmS: +avg((t) => t.vibrationMmS).toFixed(1),
        };
    });
}

// Anchor points (in twin-scene world space) for the part value callouts.
export const TWIN_PARTS: { key: string; caption: string; pos: [number, number, number] }[] = [
    { key: "rotor", caption: "Rotor", pos: [2.7, 12.6, 0] },
    { key: "nacelle", caption: "Nacelle", pos: [-2.0, 11.1, 0] },
    { key: "drivetrain", caption: "Drivetrain", pos: [1.6, 7.4, 0] },
    { key: "base", caption: "Output", pos: [1.4, 0.9, 0] },
];

export const TWIN_COMPONENT_DEVICES: Record<TwinPartKey, TwinDeviceNode[]> = {
    rotor: [
        {
            key: "rotor.pitch-control",
            component: "rotor",
            label: "Pitch control",
            property: "BladePitchDeg",
            unit: "°",
            note: "Controls blade angle to capture wind efficiently and cap loads.",
            anchor: [4.4, 13.6, -0.4],
            lookAt: [4.4, 13.6, -0.4],
            offset: [6.5, 1.6, 4.4],
            zoom: 0.52,
            value: (t) => `${Math.min(90, Math.round(t.windMs * 2.8))}`,
            status: (t) => signalState("wind", t.windMs),
        },
        {
            key: "rotor.hub-bearing",
            component: "rotor",
            label: "Hub bearing",
            property: "HubBearingTempC",
            unit: "°C",
            note: "Mechanical bearing load under the rotor hub.",
            anchor: [1.9, 12.9, 0.7],
            lookAt: [1.9, 12.9, 0.7],
            offset: [6.0, 1.8, 5.3],
            zoom: 0.58,
            value: (t) => `${(t.nacelleTempC + 2.5).toFixed(1)}`,
            status: (t) => signalState("temp", t.nacelleTempC),
        },
    ],
    nacelle: [
        {
            key: "nacelle.generator",
            component: "nacelle",
            label: "Generator",
            property: "GeneratorTempC",
            unit: "°C",
            note: "Thermal state of the generator windings and cooling path.",
            anchor: [-0.2, 11.8, 0.6],
            lookAt: [-0.2, 11.8, 0.6],
            offset: [5.0, 1.8, 5.7],
            zoom: 0.6,
            value: (t) => `${t.nacelleTempC.toFixed(1)}`,
            status: (t) => signalState("temp", t.nacelleTempC),
        },
        {
            key: "nacelle.converter",
            component: "nacelle",
            label: "Power converter",
            property: "ConverterLoadPct",
            unit: "%",
            note: "Electrical conversion and grid interface subsystem.",
            anchor: [-2.6, 10.7, -0.3],
            lookAt: [-2.6, 10.7, -0.3],
            offset: [4.7, 2.1, 6.0],
            zoom: 0.63,
            value: (t) => `${Math.min(100, Math.round((t.powerKw / 5200) * 100))}`,
            status: (t) => signalState("power", t.powerKw),
        },
    ],
    drivetrain: [
        {
            key: "drivetrain.gearbox",
            component: "drivetrain",
            label: "Gearbox",
            property: "GearboxVibrationMmS",
            unit: "mm/s",
            note: "Mechanical transmission between rotor and generator.",
            anchor: [2.0, 8.3, 0.1],
            lookAt: [2.0, 8.3, 0.1],
            offset: [6.1, 1.7, 6.5],
            zoom: 0.7,
            value: (t) => `${(t.vibrationMmS * 1.15).toFixed(1)}`,
            status: (t) => signalState("vibration", t.vibrationMmS),
        },
        {
            key: "drivetrain.main-shaft",
            component: "drivetrain",
            label: "Main shaft",
            property: "ShaftLoadPct",
            unit: "%",
            note: "Transfers rotor torque into the drivetrain chain.",
            anchor: [1.0, 6.9, -0.2],
            lookAt: [1.0, 6.9, -0.2],
            offset: [6.4, 1.4, 6.8],
            zoom: 0.74,
            value: (t) => `${Math.min(100, Math.round((t.windMs / 25) * 100))}`,
            status: (t) => signalState("wind", t.windMs),
        },
    ],
    base: [
        {
            key: "base.transformer",
            component: "base",
            label: "Transformer",
            property: "TransformerTempC",
            unit: "°C",
            note: "Output conditioning and grid-step-up hardware.",
            anchor: [2.7, 1.5, 0.4],
            lookAt: [2.7, 1.5, 0.4],
            offset: [6.9, 2.1, 7.2],
            zoom: 0.82,
            value: (t) => `${(t.nacelleTempC + 7).toFixed(1)}`,
            status: (t) => signalState("temp", t.nacelleTempC),
        },
        {
            key: "base.control-cabinet",
            component: "base",
            label: "Control cabinet",
            property: "ControllerHealthPct",
            unit: "%",
            note: "Supervisory control, telemetry and alarms.",
            anchor: [0.5, 0.8, -0.6],
            lookAt: [0.5, 0.8, -0.6],
            offset: [7.3, 2.5, 7.8],
            zoom: 0.86,
            value: (t) => `${Math.max(0, 100 - Math.round(t.vibrationMmS * 4))}`,
            status: (t) => signalState("vibration", t.vibrationMmS),
        },
    ],
};

export function twinDeviceSeeds(graph: Record<TwinPartKey, TwinDeviceNode[]>): TurbineDeviceRecord[] {
    let order = 0;
    return (Object.keys(graph) as TwinPartKey[]).flatMap((component) =>
        graph[component].map((d) => ({
            deviceKey: d.key,
            component: d.component,
            label: d.label,
            property: d.property,
            unit: d.unit,
            note: d.note,
            anchorX: d.anchor[0],
            anchorY: d.anchor[1],
            anchorZ: d.anchor[2],
            lookAtX: d.lookAt[0],
            lookAtY: d.lookAt[1],
            lookAtZ: d.lookAt[2],
            offsetX: d.offset[0],
            offsetY: d.offset[1],
            offsetZ: d.offset[2],
            zoom: d.zoom,
            sortOrder: order++,
        }))
    );
}

export function mergeTwinDeviceGraph(
    rows: TurbineDeviceRecord[],
    fallback: Record<TwinPartKey, TwinDeviceNode[]>,
): Record<TwinPartKey, TwinDeviceNode[]> {
    const fallbackByKey = new Map<TwinDeviceKey, TwinDeviceNode>(
        (Object.keys(fallback) as TwinPartKey[]).flatMap((component) =>
            fallback[component].map((d) => [d.key, d] as const)
        ),
    );

    const next: Record<TwinPartKey, TwinDeviceNode[]> = {
        rotor: [],
        nacelle: [],
        drivetrain: [],
        base: [],
    };

    rows
        .slice()
        .sort((a, b) => a.sortOrder - b.sortOrder)
        .forEach((r) => {
            const component = r.component as TwinPartKey;
            if (!(component in next)) {
                return;
            }
            const key = r.deviceKey as TwinDeviceKey;
            const fallbackNode = fallbackByKey.get(key);
            if (!fallbackNode) {
                return;
            }
            next[component].push({
                ...fallbackNode,
                key,
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

    (Object.keys(next) as TwinPartKey[]).forEach((component) => {
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
    const parts = input
        .split(",")
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n));
    if (parts.length !== 3) {
        return fallback;
    }
    return [parts[0], parts[1], parts[2]];
}

function draftFromDevice(device: TwinDeviceNode, sortOrder: number): TwinDeviceDraft {
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

export function normalizeDraft(
    draft: TwinDeviceDraft,
    base: { zoom: number; sortOrder: number; anchor: [number, number, number]; lookAt: [number, number, number]; offset: [number, number, number] },
): {
    label: string;
    property: string;
    unit: string;
    note: string;
    zoom: number;
    sortOrder: number;
    anchor: [number, number, number];
    lookAt: [number, number, number];
    offset: [number, number, number];
} {
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

export function getTwinDeviceLabel(part: TwinPartKey, deviceKey: TwinDeviceKey): string {
    return TWIN_COMPONENT_DEVICES[part].find((d) => d.key === deviceKey)?.label ?? deviceKey;
}

export function createMapTexture(sites: WindSite[]) {
    const canvas = document.createElement("canvas");
    canvas.width = 2048;
    canvas.height = 1024;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
        return new THREE.CanvasTexture(canvas);
    }

    const w = canvas.width;
    const h = canvas.height;
    const toPx = (lon: number, lat: number): [number, number] => [((lon + 180) / 360) * w, ((90 - lat) / 180) * h];

    // Transparent overlay: the lit ocean plane shows through; we only paint land.
    ctx.strokeStyle = "rgba(150, 205, 255, 0.10)";
    ctx.lineWidth = 1;
    for (let lon = -180; lon <= 180; lon += 15) {
        const x = ((lon + 180) / 360) * w;
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
        ctx.stroke();
    }
    for (let lat = -75; lat <= 75; lat += 15) {
        const y = ((90 - lat) / 180) * h;
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

    // Pass 1 - shallow-water coastal halo.
    ctx.save();
    ctx.shadowColor = "rgba(120, 235, 205, 0.85)";
    ctx.shadowBlur = 20;
    ctx.fillStyle = "rgba(92, 84, 68, 1)";
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.fill();
    });
    ctx.restore();

    // Pass 2 - crisp land with subtle relief gradient + coastline stroke.
    const landGrad = ctx.createLinearGradient(0, 0, 0, h);
    landGrad.addColorStop(0, "#8f886f");
    landGrad.addColorStop(0.5, "#7a735e");
    landGrad.addColorStop(1, "#655f4f");
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.fillStyle = landGrad;
        ctx.fill();
        ctx.strokeStyle = "rgba(233, 219, 185, 0.82)";
        ctx.lineWidth = 2.5;
        ctx.stroke();
    });

    sites.forEach((site, idx) => {
        const x = ((site.lon + 180) / 360) * w;
        const y = ((90 - site.lat) / 180) * h;
        const color = SITE_COLORS[idx % SITE_COLORS.length];

        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(x, y, 10, 0, Math.PI * 2);
        ctx.fill();

        ctx.strokeStyle = color;
        ctx.globalAlpha = 0.75;
        ctx.beginPath();
        ctx.arc(x, y, 24, 0, Math.PI * 2);
        ctx.stroke();
        ctx.globalAlpha = 1;

        ctx.fillStyle = "#f2e6cd";
        ctx.font = "20px sans-serif";
        ctx.fillText(site.country, x + 14, y - 12);
    });

    const texture = new THREE.CanvasTexture(canvas);
    texture.anisotropy = 4;
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.needsUpdate = true;
    return texture;
}

// Vertical sky gradient backdrop with selectable day/night cinematic palettes.
export function createSkyTexture(mode: "day" | "night" = "day") {
    const canvas = document.createElement("canvas");
    canvas.width = 32;
    canvas.height = 512;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
        return new THREE.CanvasTexture(canvas);
    }

    const grad = ctx.createLinearGradient(0, 0, 0, canvas.height);
    if (mode === "night") {
        grad.addColorStop(0, "#07080b");
        grad.addColorStop(0.38, "#101217");
        grad.addColorStop(0.72, "#1a1d25");
        grad.addColorStop(1, "#2a2c36");
    } else {
        grad.addColorStop(0, "#0b0c0f");
        grad.addColorStop(0.42, "#17181c");
        grad.addColorStop(0.74, "#2a2520");
        grad.addColorStop(1, "#4c3c28");
    }
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    for (let i = 0; i < 70; i += 1) {
        const x = seededRand(i * 2 + 1) * canvas.width;
        const y = seededRand(i * 2 + 2) * canvas.height * 0.45;
        ctx.globalAlpha = 0.18 + seededRand(i + 5) * 0.5;
        ctx.fillStyle = mode === "night" ? "#cfd5e2" : "#eadbc2";
        ctx.fillRect(x, y, 1, 1);
    }
    ctx.globalAlpha = 1;

    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.needsUpdate = true;
    return texture;
}

// Lit-ocean texture: graphite depth + soft warm glint + drifting ripple bands.
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
    base.addColorStop(0, "#272c33");
    base.addColorStop(0.5, "#1f242b");
    base.addColorStop(1, "#171b21");
    ctx.fillStyle = base;
    ctx.fillRect(0, 0, w, h);

    const glint = ctx.createRadialGradient(w * 0.66, h * 0.32, 20, w * 0.66, h * 0.32, w * 0.52);
    glint.addColorStop(0, "rgba(220, 179, 104, 0.18)");
    glint.addColorStop(0.4, "rgba(186, 148, 84, 0.09)");
    glint.addColorStop(1, "rgba(186, 148, 84, 0)");
    ctx.fillStyle = glint;
    ctx.fillRect(0, 0, w, h);

    ctx.strokeStyle = "rgba(196, 170, 127, 0.04)";
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

function buildFarm(seedOffset: number): TurbineTelemetry[] {
    const rows: TurbineTelemetry[] = [];

    SITES.forEach((site, siteIdx) => {
        const cx = projectLonToX(site.lon);
        const cz = projectLatToZ(site.lat);

        for (let i = 0; i < site.turbineCount; i += 1) {
            const idx = siteIdx * 100 + i;
            const angle = (Math.PI * 2 * i) / Math.max(site.turbineCount, 1);
            const radius = 1.2 + (i % 3) * 0.85;
            const x = cx + Math.cos(angle) * radius;
            const z = cz + Math.sin(angle) * radius;

            const id = `${site.id}-WT-${String(i + 1).padStart(2, "0")}`;
            const windMs = 5 + seededRand(idx + 3 + seedOffset) * 13;
            const baseKw = site.capacityMw * 220;
            const powerKw = Math.round(baseKw + windMs * 140 + seededRand(idx + 7 + seedOffset) * 250);
            const nacelleTempC = 44 + seededRand(idx + 11 + seedOffset) * 44;
            const vibrationMmS = +(1.2 + seededRand(idx + 15 + seedOffset) * 7.3).toFixed(1);

            const status: TurbineStatus = deriveTurbineStatus(nacelleTempC, vibrationMmS);

            rows.push({
                id,
                siteId: site.id,
                siteName: site.name,
                latitude: site.lat + Math.sin(angle) * 0.18,
                longitude: site.lon + Math.cos(angle) * 0.22,
                x,
                z,
                powerKw,
                windMs: +windMs.toFixed(1),
                nacelleTempC: +nacelleTempC.toFixed(1),
                vibrationMmS,
                status,
            });
        }
    });

    return rows;
}

// Live refresh cadence for the real semantic-model feed (kept close to the
// synthetic seed cadence so the two paths feel consistent to the operator).
const LIVE_TELEMETRY_REFRESH_MS = 5000;

// Assemble live semantic-model rows into the same TurbineTelemetry shape the
// scene renders, reusing the ontology farm coordinates and the shared status
// bands so real data flows through the identical rendering path as buildFarm().
function assembleLiveView(snapshot: LiveTelemetrySnapshot): { turbines: TurbineTelemetry[]; sites: WindSite[] } {
    const metricsById = new Map(snapshot.metrics.map((m) => [m.turbineId, m]));
    const farmById = new Map(snapshot.farms.map((f) => [f.farmId, f]));
    const turbinesByFarm = new Map<string, string[]>();
    for (const t of snapshot.turbines) {
        const list = turbinesByFarm.get(t.farmId) ?? [];
        list.push(t.turbineId);
        turbinesByFarm.set(t.farmId, list);
    }

    const turbines: TurbineTelemetry[] = [];
    for (const farm of snapshot.farms) {
        const ids = turbinesByFarm.get(farm.farmId) ?? [];
        const cx = projectLonToX(farm.longitude);
        const cz = projectLatToZ(farm.latitude);
        ids.forEach((turbineId, i) => {
            const angle = (Math.PI * 2 * i) / Math.max(ids.length, 1);
            const radius = 1.2 + (i % 3) * 0.85;
            const m = metricsById.get(turbineId);
            const nacelleTempC = +(m?.nacelleTempC ?? 0).toFixed(1);
            const vibrationMmS = +(m?.vibrationMmS ?? 0).toFixed(1);
            turbines.push({
                id: turbineId,
                siteId: farm.farmId,
                siteName: farm.farmName,
                latitude: farm.latitude + Math.sin(angle) * 0.18,
                longitude: farm.longitude + Math.cos(angle) * 0.22,
                x: cx + Math.cos(angle) * radius,
                z: cz + Math.sin(angle) * radius,
                powerKw: Math.round(m?.powerKw ?? 0),
                windMs: +(m?.windMs ?? 0).toFixed(1),
                nacelleTempC,
                vibrationMmS,
                status: deriveTurbineStatus(nacelleTempC, vibrationMmS),
            });
        });
    }

    const sites: WindSite[] = snapshot.farms.map((farm) => ({
        id: farm.farmId,
        name: farm.farmName,
        country: "",
        lat: farm.latitude,
        lon: farm.longitude,
        turbineCount: (turbinesByFarm.get(farm.farmId) ?? []).length,
        capacityMw: farm.capacityMw,
    }));

    // Ignore farms with no coordinates so they cannot collapse turbines to origin.
    return {
        turbines: turbines.filter((t) => farmById.has(t.siteId)),
        sites,
    };
}

// React hook: when a live semantic-model connection is configured, poll it and
// return real turbines + sites; otherwise return null so callers fall back to
// the synthetic feed. Any query failure also yields null (fail-safe).
function useLiveTelemetry(): { turbines: TurbineTelemetry[]; sites: WindSite[] } | null {
    const [snapshot, setSnapshot] = useState<{ turbines: TurbineTelemetry[]; sites: WindSite[] } | null>(null);

    useEffect(() => {
        if (!isLiveTelemetryConfigured()) {
            return;
        }
        let cancelled = false;
        const load = async () => {
            const data = await fetchLiveTelemetry();
            if (cancelled || !data || data.turbines.length === 0) {
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
    const telemetry = (context.telemetry as TurbineTelemetry[] | undefined) ?? [];
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

const LazyWindFarmScene = lazy(() => import("@/scenes/WindFarmScene"));
const LazyTurbineTwinScene = lazy(() => import("@/scenes/TurbineTwinScene"));

function WindFarmScene({
    turbines,
    sites,
    selectedId,
    dimmedIds,
    paused,
    onSelect,
}: {
    turbines: TurbineTelemetry[];
    sites: WindSite[];
    selectedId: string;
    dimmedIds: Set<string>;
    paused: boolean;
    onSelect: (id: string) => void;
}) {
    const hostRef = useRef<HTMLDivElement | null>(null);
    const sceneRef = useRef<SceneState | null>(null);
    const pausedRef = useRef(paused);
    const zoomRef = useRef(0.62);
    const [cameraMode, setCameraMode] = useState<"cinematic" | "manual">("cinematic");
    const cameraModeRef = useRef<"cinematic" | "manual">("cinematic");
    const orbitYawRef = useRef(0.58);
    const orbitPitchRef = useRef(0.44);
    const panRef = useRef({ x: 0, z: 0 });
    const dragRef = useRef({ active: false, moved: false, button: 0, lastX: 0, lastY: 0 });

    useEffect(() => {
        pausedRef.current = paused;
    }, [paused]);

    useEffect(() => {
        cameraModeRef.current = cameraMode;
    }, [cameraMode]);

    useEffect(() => {
        const host = hostRef.current;
        if (!host) {
            return;
        }

        const testCanvas = document.createElement("canvas");
        const hasWebGL = !!(testCanvas.getContext("webgl2") || testCanvas.getContext("webgl"));
        if (!hasWebGL) {
            host.innerHTML = "<div style='padding:12px;color:#9aa3b2'>WebGL unavailable in this environment.</div>";
            return;
        }

        const scene = new THREE.Scene();
        const skyTexture = createSkyTexture();
        scene.background = skyTexture;
        scene.fog = new THREE.Fog("#0e3a55", 170, 380);

        const camera = new THREE.PerspectiveCamera(52, host.clientWidth / host.clientHeight, 0.1, 500);
        camera.position.set(30, 36, 64);
        camera.lookAt(0, 0, 0);

        const renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: "high-performance" });
        renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
        renderer.setSize(host.clientWidth, host.clientHeight);
        renderer.shadowMap.enabled = true;
        renderer.shadowMap.type = THREE.PCFSoftShadowMap;
        renderer.outputColorSpace = THREE.SRGBColorSpace;
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.16;
        host.innerHTML = "";
        host.appendChild(renderer.domElement);

        // Image-based lighting from the sky gradient so towers, nacelles and
        // blades pick up soft, realistic reflections.
        const pmrem = new THREE.PMREMGenerator(renderer);
        const envRT = pmrem.fromEquirectangular(skyTexture);
        scene.environment = envRT.texture;
        pmrem.dispose();

        const ambient = new THREE.AmbientLight(0xaec6ff, 0.42);
        const sun = new THREE.DirectionalLight(0xd6ecff, 1.18);
        sun.position.set(34, 52, 18);
        sun.castShadow = true;
        sun.shadow.mapSize.width = 2048;
        sun.shadow.mapSize.height = 2048;
        sun.shadow.camera.near = 1;
        sun.shadow.camera.far = 220;
        sun.shadow.camera.left = -70;
        sun.shadow.camera.right = 70;
        sun.shadow.camera.top = 46;
        sun.shadow.camera.bottom = -46;
        sun.shadow.bias = -0.0004;
        sun.shadow.normalBias = 0.02;
        const rim = new THREE.DirectionalLight(0x4fa3ff, 0.24);
        rim.position.set(-24, 18, -24);
        const hemi = new THREE.HemisphereLight(0xbcd8ff, 0x16324a, 0.38);
        scene.add(ambient, sun, rim, hemi);

        // Single unified lit ocean plane (large enough to fill the horizon).
        const oceanTexture = createOceanTexture();
        const terrain = new THREE.Mesh(
            new THREE.PlaneGeometry(200, 120, 60, 36),
            new THREE.MeshStandardMaterial({ map: oceanTexture, roughness: 0.72, metalness: 0.12 })
        );
        terrain.rotation.x = -Math.PI / 2;
        terrain.position.y = -0.15;
        terrain.receiveShadow = true;
        const pos = terrain.geometry.attributes.position;
        for (let i = 0; i < pos.count; i += 1) {
            const x = pos.getX(i);
            const y = pos.getY(i);
            const wave = Math.sin(x * 0.12) * 0.08 + Math.cos(y * 0.17) * 0.06;
            pos.setZ(i, wave);
        }
        terrain.geometry.computeVertexNormals();
        scene.add(terrain);

        // Map plane spans exactly 92 x 46 world units (true 2:1 equirectangular) so
        // lon/lat -> X/Z matches projectLonToX / projectLatToZ, pinning every turbine
        // to its real location without vertical distortion.
        // Transparent + depthWrite:false so the ocean shows through and turbines stay on top.
        const mapTexture = createMapTexture(sites);
        const mapPlane = new THREE.Mesh(
            new THREE.PlaneGeometry(92, 46, 1, 1),
            new THREE.MeshBasicMaterial({ map: mapTexture, transparent: true, depthWrite: false })
        );
        mapPlane.rotation.x = -Math.PI / 2;
        mapPlane.position.y = 0.06;
        scene.add(mapPlane);

        const grid = new THREE.GridHelper(92, 24, 0x2a557f, 0x2a557f);
        grid.scale.z = 46 / 92; // constrain the square grid to the 92 x 46 map footprint
        grid.position.y = 0.02;
        grid.material.transparent = true;
        grid.material.opacity = 0.12;
        scene.add(grid);

        const byId = new Map<string, TurbineRenderRefs>();
        const siteRefs = new Map<string, SiteRenderRefs>();
        const pickables: THREE.Mesh[] = [];
        const raycaster = new THREE.Raycaster();
        const pointer = new THREE.Vector2();

        sites.forEach((site, idx) => {
            const markerMat = new THREE.MeshStandardMaterial({
                color: SITE_COLORS[idx % SITE_COLORS.length],
                emissive: SITE_COLORS[idx % SITE_COLORS.length],
                emissiveIntensity: 0.32,
            });
            const marker = new THREE.Mesh(
                new THREE.CylinderGeometry(0.24, 0.24, 0.9, 14),
                markerMat
            );
            marker.position.set(projectLonToX(site.lon), 0.45, projectLatToZ(site.lat));
            scene.add(marker);

            const glowMat = new THREE.MeshBasicMaterial({ color: SITE_COLORS[idx % SITE_COLORS.length], transparent: true, opacity: 0.55 });
            const glow = new THREE.Mesh(
                new THREE.RingGeometry(0.5, 0.95, 24),
                glowMat
            );
            glow.rotation.x = -Math.PI / 2;
            glow.position.set(projectLonToX(site.lon), 0.07, projectLatToZ(site.lat));
            scene.add(glow);

            siteRefs.set(site.id, {
                marker,
                markerMat,
                glow,
                glowMat,
                baseY: 0.45,
            });
        });

        const hubY = 7.4;
        const towerGeo = new THREE.CylinderGeometry(0.12, 0.4, 7.2, 24);
        const bladeGeo = createBladeGeometry();
        const baseGeo = new THREE.CylinderGeometry(0.62, 0.78, 0.3, 24);
        const nacelleGeo = new THREE.BoxGeometry(1.7, 0.55, 0.6);
        const spinnerGeo = new THREE.ConeGeometry(0.26, 0.6, 16);

        const towerMat = new THREE.MeshStandardMaterial({ color: "#dde4ec", roughness: 0.32, metalness: 0.55 });
        const bladeMat = new THREE.MeshStandardMaterial({ color: "#f6f8fc", roughness: 0.35, metalness: 0.18 });
        const baseMat = new THREE.MeshStandardMaterial({ color: "#7f8b9a", roughness: 0.68, metalness: 0.22 });
        const nacelleHousingMat = new THREE.MeshStandardMaterial({ color: "#e8edf3", roughness: 0.4, metalness: 0.5 });
        const hubMat = new THREE.MeshStandardMaterial({ color: "#cdd6e0", roughness: 0.35, metalness: 0.55 });

        turbines.forEach((t) => {
            const group = new THREE.Group();
            group.position.set(t.x, 0, t.z);
            scene.add(group);

            const pedestal = new THREE.Mesh(baseGeo, baseMat);
            pedestal.position.y = 0.14;
            pedestal.castShadow = true;
            pedestal.receiveShadow = true;
            group.add(pedestal);

            const tower = new THREE.Mesh(towerGeo, towerMat);
            tower.position.y = 3.7;
            tower.castShadow = true;
            tower.receiveShadow = true;
            group.add(tower);

            const nacelle = new THREE.Mesh(nacelleGeo, nacelleHousingMat);
            nacelle.position.set(-0.15, hubY, 0);
            nacelle.castShadow = true;
            group.add(nacelle);

            const spinner = new THREE.Mesh(spinnerGeo, hubMat);
            spinner.rotation.z = -Math.PI / 2;
            spinner.position.set(0.95, hubY, 0);
            spinner.castShadow = true;
            group.add(spinner);

            // Status beacon on the nacelle (color driven by live telemetry).
            const nacelleMat = new THREE.MeshStandardMaterial({
                color: STATUS_COLORS[t.status],
                emissive: STATUS_COLORS[t.status],
                emissiveIntensity: 0.55,
                roughness: 0.3,
                metalness: 0.2,
            });
            const beacon = new THREE.Mesh(new THREE.SphereGeometry(0.16, 12, 12), nacelleMat);
            beacon.position.set(-0.78, hubY + 0.42, 0);
            group.add(beacon);

            const blades = new THREE.Group();
            blades.position.set(0.95, hubY, 0);
            for (let i = 0; i < 3; i += 1) {
                const holder = new THREE.Group();
                holder.rotation.x = (i * (Math.PI * 2)) / 3;
                const blade = new THREE.Mesh(bladeGeo, bladeMat);
                blade.rotation.y = 0.22;
                blade.castShadow = true;
                holder.add(blade);
                blades.add(holder);
            }
            group.add(blades);

            const pickMesh = new THREE.Mesh(
                new THREE.CylinderGeometry(0.7, 0.7, 8.2, 10),
                new THREE.MeshBasicMaterial({ transparent: true, opacity: 0 })
            );
            pickMesh.position.y = 4;
            pickMesh.userData.turbineId = t.id;
            pickables.push(pickMesh);
            group.add(pickMesh);

            const ringMat = new THREE.MeshBasicMaterial({
                color: STATUS_COLORS[t.status],
                transparent: true,
                opacity: t.id === selectedId ? 0.9 : 0.55,
            });
            const ring = new THREE.Mesh(new THREE.RingGeometry(0.85, 1.15, 32), ringMat);
            ring.rotation.x = -Math.PI / 2;
            ring.position.y = 0.03;
            group.add(ring);

            byId.set(t.id, {
                nacelleMat,
                ringMat,
                ring,
                blades,
                spin: 0.08 + seededRand(t.id.length + t.powerKw * 0.001) * 0.12,
            });
        });

        const onClick = (event: MouseEvent) => {
            if (dragRef.current.moved) {
                dragRef.current.moved = false;
                return;
            }
            const rect = renderer.domElement.getBoundingClientRect();
            pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
            pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
            raycaster.setFromCamera(pointer, camera);
            const hits = raycaster.intersectObjects(pickables);
            if (hits.length > 0) {
                const id = hits[0].object.userData.turbineId as string;
                onSelect(id);
            }
        };
        renderer.domElement.addEventListener("click", onClick);

        const onMouseDown = (event: MouseEvent) => {
            if (event.button !== 0 && event.button !== 2) {
                return;
            }
            dragRef.current.active = true;
            dragRef.current.moved = false;
            dragRef.current.button = event.button;
            dragRef.current.lastX = event.clientX;
            dragRef.current.lastY = event.clientY;
        };

        const onMouseMove = (event: MouseEvent) => {
            if (!dragRef.current.active) {
                return;
            }
            const dx = event.clientX - dragRef.current.lastX;
            const dy = event.clientY - dragRef.current.lastY;
            dragRef.current.lastX = event.clientX;
            dragRef.current.lastY = event.clientY;
            if (Math.abs(dx) + Math.abs(dy) < 1) {
                return;
            }
            dragRef.current.moved = true;

            const panScale = 0.055 * zoomRef.current;
            const shouldPan = dragRef.current.button === 2 || event.shiftKey;
            if (cameraModeRef.current === "manual" && !shouldPan) {
                orbitYawRef.current -= dx * 0.005;
                orbitPitchRef.current = THREE.MathUtils.clamp(orbitPitchRef.current + dy * 0.0035, 0.2, 1.25);
                return;
            }

            panRef.current.x -= dx * panScale;
            panRef.current.z += dy * panScale;
        };

        const onMouseUp = () => {
            dragRef.current.active = false;
        };

        const onContextMenu = (event: MouseEvent) => {
            event.preventDefault();
        };

        renderer.domElement.addEventListener("mousedown", onMouseDown);
        renderer.domElement.addEventListener("mousemove", onMouseMove);
        renderer.domElement.addEventListener("mouseup", onMouseUp);
        renderer.domElement.addEventListener("mouseleave", onMouseUp);
        renderer.domElement.addEventListener("contextmenu", onContextMenu);

        const onWheel = (event: WheelEvent) => {
            event.preventDefault();
            zoomRef.current = THREE.MathUtils.clamp(zoomRef.current + event.deltaY * 0.0009, 0.4, 1.8);
        };
        renderer.domElement.addEventListener("wheel", onWheel, { passive: false });

        const onResize = () => {
            if (host.clientWidth === 0 || host.clientHeight === 0) {
                return;
            }
            camera.aspect = host.clientWidth / host.clientHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(host.clientWidth, host.clientHeight);
        };
        window.addEventListener("resize", onResize);

        let tick = 0;
        const animate = () => {
            if (!pausedRef.current) {
                tick += 0.03;
                byId.forEach((refs) => {
                    refs.blades.rotation.x += refs.spin;
                });
            }
            oceanTexture.offset.x = tick * 0.0015;
            oceanTexture.offset.y = Math.sin(tick * 0.05) * 0.01;
            const z = zoomRef.current;
            const panX = panRef.current.x;
            const panZ = panRef.current.z;
            if (cameraModeRef.current === "manual") {
                const distance = 78 * z;
                const yaw = orbitYawRef.current;
                const pitch = orbitPitchRef.current;
                const planar = Math.cos(pitch) * distance;
                camera.position.x = panX + Math.cos(yaw) * planar;
                camera.position.y = Math.sin(pitch) * distance;
                camera.position.z = panZ + Math.sin(yaw) * planar;
                camera.lookAt(panX, 0, panZ);
            } else {
                camera.position.x = panX + (26 + Math.sin(tick * 0.17) * 9) * z;
                camera.position.y = 36 * z;
                camera.position.z = panZ + (61 + Math.cos(tick * 0.13) * 5) * z;
                camera.lookAt(panX, 0, panZ);
            }
            renderer.render(scene, camera);
        };
        renderer.setAnimationLoop(animate);

        const cleanup = () => {
            renderer.setAnimationLoop(null);
            renderer.domElement.removeEventListener("click", onClick);
            renderer.domElement.removeEventListener("mousedown", onMouseDown);
            renderer.domElement.removeEventListener("mousemove", onMouseMove);
            renderer.domElement.removeEventListener("mouseup", onMouseUp);
            renderer.domElement.removeEventListener("mouseleave", onMouseUp);
            renderer.domElement.removeEventListener("contextmenu", onContextMenu);
            renderer.domElement.removeEventListener("wheel", onWheel);
            window.removeEventListener("resize", onResize);
            renderer.dispose();
            scene.traverse((obj) => {
                if (obj instanceof THREE.Mesh) {
                    obj.geometry.dispose();
                    const material = Array.isArray(obj.material) ? obj.material : [obj.material];
                    material.forEach((m) => m.dispose());
                }
            });
            mapTexture.dispose();
            oceanTexture.dispose();
            skyTexture.dispose();
            envRT.dispose();
        };

        sceneRef.current = {
            byId,
            siteRefs,
            cleanup,
        };

        return () => {
            sceneRef.current = null;
            cleanup();
        };
    }, [onSelect, sites]);

    useEffect(() => {
        const ref = sceneRef.current;
        if (!ref) {
            return;
        }

        ref.byId.forEach((meshRefs, id) => {
            const t = turbines.find((row) => row.id === id);
            if (!t) {
                return;
            }

            const dimmed = dimmedIds.has(id);
            meshRefs.nacelleMat.color.set(STATUS_COLORS[t.status]);
            meshRefs.nacelleMat.emissive.set(STATUS_COLORS[t.status]);
            meshRefs.nacelleMat.emissiveIntensity = dimmed ? 0.02 : 0.16;
            meshRefs.ringMat.color.set(STATUS_COLORS[t.status]);
            meshRefs.ringMat.opacity = dimmed ? 0.06 : t.id === selectedId ? 0.98 : 0.56;
            meshRefs.ring.scale.setScalar(t.id === selectedId ? 1.24 : dimmed ? 0.7 : 1);
            meshRefs.spin = dimmed ? 0.003 : 0.055 + Math.min(0.22, t.windMs / 80);
        });

        const selectedSiteId = turbines.find((t) => t.id === selectedId)?.siteId;
        const bySite = new Map<string, { warnings: number; alarms: number }>();
        turbines.forEach((t) => {
            const cur = bySite.get(t.siteId) ?? { warnings: 0, alarms: 0 };
            if (t.status === "alarm") cur.alarms += 1;
            if (t.status === "warning") cur.warnings += 1;
            bySite.set(t.siteId, cur);
        });

        ref.siteRefs.forEach((sref, siteId) => {
            const site = bySite.get(siteId) ?? { warnings: 0, alarms: 0 };
            const severity = THREE.MathUtils.clamp(site.alarms * 0.42 + site.warnings * 0.18, 0, 1);
            const isSelectedSite = selectedSiteId === siteId;
            const beaconHeight = 0.9 + severity * 1.45;
            sref.marker.scale.set(1, beaconHeight, 1);
            sref.marker.position.y = sref.baseY + (beaconHeight - 1) * 0.42;
            sref.markerMat.emissiveIntensity = (isSelectedSite ? 0.48 : 0.22) + severity * 0.9;
            sref.markerMat.roughness = 0.36 - severity * 0.12;
            sref.markerMat.metalness = 0.18 + severity * 0.18;

            const glowScale = (isSelectedSite ? 1.15 : 1) + severity * 1.35;
            sref.glow.scale.set(glowScale, glowScale, glowScale);
            sref.glowMat.opacity = (isSelectedSite ? 0.52 : 0.34) + severity * 0.45;
        });
    }, [selectedId, turbines, dimmedIds]);

    return (
        <div className="relative h-full w-full">
            <div ref={hostRef} className="h-full w-full" />
            <div className="absolute bottom-3 right-3 flex flex-col overflow-hidden rounded-lg border border-slate-700/70 bg-[#06101fcc] text-slate-100 backdrop-blur">
                <button
                    type="button"
                    title="Toggle camera mode"
                    onClick={() => setCameraMode((m) => (m === "cinematic" ? "manual" : "cinematic"))}
                    className="px-2.5 py-1.5 text-[11px] hover:bg-slate-700/60"
                >
                    {cameraMode === "cinematic" ? "AUTO" : "MANUAL"}
                </button>
                <button type="button" title="Zoom in" onClick={() => { zoomRef.current = THREE.MathUtils.clamp(zoomRef.current - 0.15, 0.4, 1.8); }} className="px-2.5 py-1.5 text-sm hover:bg-slate-700/60">＋</button>
                <button type="button" title="Zoom out" onClick={() => { zoomRef.current = THREE.MathUtils.clamp(zoomRef.current + 0.15, 0.4, 1.8); }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-sm hover:bg-slate-700/60">－</button>
                <button
                    type="button"
                    title="Reset camera"
                    onClick={() => {
                        zoomRef.current = 0.62;
                        orbitYawRef.current = 0.58;
                        orbitPitchRef.current = 0.44;
                        panRef.current = { x: 0, z: 0 };
                    }}
                    className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60"
                >
                    ⟳
                </button>
            </div>

            <div className="pointer-events-none absolute bottom-3 right-16 hidden rounded-lg border border-slate-700/70 bg-[#06101fcc] px-2.5 py-1.5 text-[10px] text-slate-300 backdrop-blur sm:block">
                <div className="font-medium text-slate-200">Site beacons + camera</div>
                <div>height/ring = warning + alarm severity</div>
                <div>drag = orbit (manual) · shift/right-drag = pan</div>
            </div>
        </div>
    );
}

type StatusFilter = TurbineStatus | "all";

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

// Predictive anomaly score (0-1) from nacelle temp and vibration approaching limits.
export function anomalyScore(t: TurbineTelemetry): number {
    const tempScore = (t.nacelleTempC - 60) / 30;
    const vibScore = (t.vibrationMmS - 4) / 4;
    return Math.min(1, Math.max(0, tempScore, vibScore));
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

// Pure what-if model: apply a curtailment (% output reduction while running) and a
// maintenance downtime (ticks offline) to a baseline output over a horizon, and
// report projected output plus the energy delta (output x ticks) vs doing nothing.
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

export type WorkOrderPriority = "P1" | "P2" | "P3";
export type WorkOrderComponent = "Gearbox" | "Generator";

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

type MockWindEvidence = {
    id: string;
    label: string;
    component: WorkOrderComponent;
    image: string;
    capturedAt: string;
};

function svgAvatar(name: string, seed: number, role: string, shift: "day" | "swing" | "night", onCall: boolean): string {
    const profile = role.toLowerCase();
    const isField = /field|maintainer|reliability/.test(profile);
    const isOps = /operations|lead/.test(profile);
    const isSpecialist = /specialist|engineer/.test(profile);
    // Simpsons-style flat cartoon palette: yellow skin, bold hair, role-based headwear.
    const skin = "#ffd21e";
    const skinShade = "#e9b400";
    const hair = ["#3a2a1a", "#5b3a29", "#1f2937", "#7a4a25", "#111111"][seed % 5];
    const jacket = isField ? "#0f766e" : isOps ? "#1d4ed8" : isSpecialist ? "#7c3aed" : ["#9a3412", "#0f172a"][seed % 2];
    const accent = onCall ? "#34d399" : ["#60a5fa", "#f59e0b", "#f472b6", "#a3e635"][seed % 4];
    const bg = shift === "night" ? "#0b1120" : shift === "swing" ? "#101827" : "#132238";
    // Hair styles vary by seed (spiky, side-part, bun, buzz, curly).
    const hairStyles = [
        `<path d='M50 46c2-14 14-24 30-24s28 10 30 24c-6-6-12-4-14 2-3-7-9-7-12-1-3-7-10-7-13 0-3-7-9-6-11 0-2-5-8-7-10-1Z' fill='${hair}'/>`,
        `<path d='M48 50c0-17 14-30 32-30s32 12 32 29c-8-8-16-9-24-9H62c-6 0-11 3-14 10Z' fill='${hair}'/>`,
        `<path d='M50 48c0-16 13-28 30-28s30 12 30 28c-6-6-13-8-20-8H70c-8 0-15 2-20 8Z' fill='${hair}'/><circle cx='80' cy='24' r='9' fill='${hair}'/>`,
        `<path d='M52 50c1-15 12-27 28-27s27 12 28 27c-7-5-14-7-28-7s-21 2-28 7Z' fill='${hair}'/>`,
        `<path d='M49 52c1-18 13-31 31-31s30 13 31 31c-7-7-9-2-13-9-4 7-9 2-13 9-4-7-9-2-13-9-4 7-6 2-23 9Z' fill='${hair}'/>`,
    ];
    const headwear = isField
        ? `<path d='M46 44c2-14 16-22 34-22s32 8 34 22H46Z' fill='#f6b73c'/><rect x='60' y='38' width='40' height='9' rx='3' fill='#e08a1e'/><rect x='44' y='44' width='72' height='6' rx='3' fill='#e08a1e'/>`
        : isOps
        ? `<path d='M50 40c4-13 16-18 30-18s26 5 30 18c-8-3-52-3-60 0Z' fill='#dbeafe'/><rect x='66' y='30' width='28' height='7' rx='3' fill='#93c5fd'/>`
        : "";
    const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 160 160'>
<defs>
    <linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>
        <stop offset='0%' stop-color='${bg}'/>
        <stop offset='100%' stop-color='${accent}' stop-opacity='0.22'/>
    </linearGradient>
</defs>
<rect width='160' height='160' rx='18' fill='url(#bg)'/>
<path d='M40 150c2-22 18-34 40-34s38 12 40 34Z' fill='${jacket}'/>
<path d='M64 118c4 8 28 8 32 0l-2 18H66Z' fill='${skin}'/>
<ellipse cx='80' cy='74' rx='30' ry='33' fill='${skin}'/>
<path d='M110 74c0 18-13 33-30 33 8 2 20-2 26-14 5-9 5-15 4-19Z' fill='${skinShade}' opacity='0.35'/>
${hairStyles[seed % hairStyles.length]}
<circle cx='70' cy='72' r='11' fill='#ffffff'/><circle cx='92' cy='72' r='11' fill='#ffffff'/>
<circle cx='72' cy='73' r='3.4' fill='#1a1a1a'/><circle cx='90' cy='73' r='3.4' fill='#1a1a1a'/>
<path d='M84 78c4 2 8 6 8 12' stroke='${skinShade}' stroke-width='3' fill='none' stroke-linecap='round'/>
<path d='M66 96c8 7 20 7 28 0' stroke='#7a4a12' stroke-width='3' fill='none' stroke-linecap='round'/>
<path d='M64 60c4-4 10-4 13-1M83 59c3-3 9-3 13 1' stroke='${hair}' stroke-width='3' fill='none' stroke-linecap='round'/>
${headwear}
<circle cx='122' cy='36' r='9' fill='${onCall ? "#34d399" : "rgba(255,255,255,0.18)"}'/>
</svg>`;
    return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}

function svgMockFieldCapture(label: string, seed: number, component: WorkOrderComponent): string {
    const isGenerator = component === "Generator";
    const isGearbox = component === "Gearbox";
    const subtitle = isGenerator ? "rotor / stator" : isGearbox ? "oil / vibration" : "cabinet / control";
    // Studio-Ghibli-inspired painterly palettes: soft skies, warm light, layered hills.
    const palette = isGenerator
        ? { skyTop: "#ffd9a8", skyBot: "#f7a072", sun: "#fff3d6", hillFar: "#e08a5c", hillMid: "#c96f4a", hillNear: "#8f4a3a", part: "#fbe8c9", partLine: "#7a3b28", cloud: "#fff1e0" }
        : isGearbox
        ? { skyTop: "#cdeef0", skyBot: "#8fd0c9", sun: "#f4ffe9", hillFar: "#8fc99b", hillMid: "#5ea67f", hillNear: "#356b57", part: "#eafaf1", partLine: "#2c5a4a", cloud: "#f4fffb" }
        : { skyTop: "#dcd2f5", skyBot: "#a99ad6", sun: "#fdeeff", hillFar: "#a98fc9", hillMid: "#7d63a6", hillNear: "#4c3a6b", part: "#f1eafc", partLine: "#3d2c5a", cloud: "#f6f0ff" };
    const part = isGenerator
        ? `<g><ellipse cx='150' cy='96' rx='34' ry='16' fill='${palette.partLine}' opacity='0.25'/><rect x='126' y='58' width='50' height='40' rx='12' fill='${palette.part}' stroke='${palette.partLine}' stroke-width='2.5'/><circle cx='151' cy='78' r='13' fill='none' stroke='${palette.partLine}' stroke-width='3'/><path d='M151 67v22M140 78h22M144 71l14 14M158 71l-14 14' stroke='${palette.partLine}' stroke-width='2.4' stroke-linecap='round'/></g>`
        : isGearbox
        ? `<g><ellipse cx='150' cy='98' rx='34' ry='14' fill='${palette.partLine}' opacity='0.25'/><circle cx='150' cy='76' r='22' fill='${palette.part}' stroke='${palette.partLine}' stroke-width='2.5'/><circle cx='150' cy='76' r='7' fill='${palette.partLine}'/><path d='M150 50v10M150 92v10M124 76h10M166 76h10M132 58l7 7M161 87l7 7M132 94l7-7M161 65l7-7' stroke='${palette.partLine}' stroke-width='2.6' stroke-linecap='round'/></g>`
        : `<g><ellipse cx='150' cy='98' rx='34' ry='14' fill='${palette.partLine}' opacity='0.25'/><rect x='128' y='56' width='46' height='44' rx='10' fill='${palette.part}' stroke='${palette.partLine}' stroke-width='2.5'/><path d='M136 66h30M136 76h30M136 86h20' stroke='${palette.partLine}' stroke-width='2.4' stroke-linecap='round'/></g>`;
    const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 220 132'>
<defs>
    <linearGradient id='sky${seed}' x1='0' y1='0' x2='0' y2='1'>
        <stop offset='0%' stop-color='${palette.skyTop}'/>
        <stop offset='100%' stop-color='${palette.skyBot}'/>
    </linearGradient>
    <radialGradient id='sun${seed}' cx='0.28' cy='0.32' r='0.5'>
        <stop offset='0%' stop-color='${palette.sun}' stop-opacity='0.95'/>
        <stop offset='100%' stop-color='${palette.sun}' stop-opacity='0'/>
    </radialGradient>
    <clipPath id='card${seed}'><rect width='220' height='132' rx='12'/></clipPath>
</defs>
<g clip-path='url(#card${seed})'>
    <rect width='220' height='132' fill='url(#sky${seed})'/>
    <circle cx='62' cy='42' r='20' fill='${palette.sun}' opacity='0.9'/>
    <rect width='220' height='132' fill='url(#sun${seed})'/>
    <ellipse cx='150' cy='30' rx='26' ry='11' fill='${palette.cloud}' opacity='0.85'/>
    <ellipse cx='172' cy='34' rx='18' ry='9' fill='${palette.cloud}' opacity='0.7'/>
    <ellipse cx='40' cy='58' rx='22' ry='9' fill='${palette.cloud}' opacity='0.6'/>
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
    { id: "wk-001", name: "Aline Laurent", role: "Field Reliability Engineer", skills: ["Gearbox"], confidenceBySkill: { Gearbox: 0.92, Generator: 0.58 }, siteCoverage: ["SITE-TX", "SITE-GB"], shift: "day", onCall: true, etaMin: 28, photo: svgAvatar("Aline Laurent", 1, "Field Reliability Engineer", "day", true) },
    { id: "wk-002", name: "Marc Delorme", role: "Generator Specialist", skills: ["Generator"], confidenceBySkill: { Gearbox: 0.42, Generator: 0.95 }, siteCoverage: ["SITE-DK", "SITE-GB", "SITE-MA"], shift: "night", onCall: true, etaMin: 35, photo: svgAvatar("Marc Delorme", 2, "Generator Specialist", "night", true) },
    { id: "wk-003", name: "Nora Haddad", role: "Remote Operations Lead", skills: ["Gearbox", "Generator"], confidenceBySkill: { Gearbox: 0.83, Generator: 0.8 }, siteCoverage: ["SITE-IN", "SITE-MA", "SITE-BR"], shift: "swing", onCall: true, etaMin: 18, photo: svgAvatar("Nora Haddad", 3, "Remote Operations Lead", "swing", true) },
    { id: "wk-004", name: "Victor Klein", role: "Mechanical Maintainer", skills: ["Gearbox"], confidenceBySkill: { Gearbox: 0.86, Generator: 0.44 }, siteCoverage: ["SITE-DK", "SITE-TX"], shift: "day", onCall: false, etaMin: 41, photo: svgAvatar("Victor Klein", 4, "Mechanical Maintainer", "day", false) },
    { id: "wk-005", name: "Sofia Ribeiro", role: "Grid & Conversion Engineer", skills: ["Generator"], confidenceBySkill: { Gearbox: 0.52, Generator: 0.9 }, siteCoverage: ["SITE-BR", "SITE-TX", "SITE-IN"], shift: "night", onCall: true, etaMin: 32, photo: svgAvatar("Sofia Ribeiro", 5, "Grid & Conversion Engineer", "night", true) },
];

const MOCK_WIND_EVIDENCE: MockWindEvidence[] = [
    { id: "wf-ev-gearbox-01", label: "Gearbox oil trace", component: "Gearbox", image: svgMockFieldCapture("Gearbox oil trace", 1, "Gearbox"), capturedAt: "2026-07-14T07:21:00Z" },
    { id: "wf-ev-gearbox-02", label: "Vibration spectrum anomaly", component: "Gearbox", image: svgMockFieldCapture("Vibration spectrum anomaly", 2, "Gearbox"), capturedAt: "2026-07-14T07:24:00Z" },
    { id: "wf-ev-generator-01", label: "Generator thermal hotspot", component: "Generator", image: svgMockFieldCapture("Generator thermal hotspot", 3, "Generator"), capturedAt: "2026-07-14T07:26:00Z" },
    { id: "wf-ev-generator-02", label: "Converter cabinet alarm", component: "Generator", image: svgMockFieldCapture("Converter cabinet alarm", 4, "Generator"), capturedAt: "2026-07-14T07:29:00Z" },
];

function currentShiftByHour(hour: number): "day" | "swing" | "night" {
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
) {
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
            ].join(" · ");
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

// Suspected component from the dominant abnormal signal, each normalised against
// its own warn->alarm band: vibration points at the mechanical drivetrain
// (gearbox), thermal load at the generator.
export function recommendComponent(nacelleTempC: number, vibrationMmS: number): WorkOrderComponent {
    const tempSeverity = (nacelleTempC - SIGNAL_METADATA.temp.warn) / Math.max(1, SIGNAL_METADATA.temp.alarm - SIGNAL_METADATA.temp.warn);
    const vibSeverity = (vibrationMmS - SIGNAL_METADATA.vibration.warn) / Math.max(1, SIGNAL_METADATA.vibration.alarm - SIGNAL_METADATA.vibration.warn);
    return vibSeverity >= tempSeverity ? "Gearbox" : "Generator";
}

// Hysteresis wrapper around recommendComponent: keeps the previously-shown component
// when the two signal severities are within `margin` of each other, so the probable
// cause (and the evidence/responders/priority derived from it) stops flip-flopping on
// every live refresh near the decision boundary.
export function recommendComponentStable(
    nacelleTempC: number,
    vibrationMmS: number,
    previous: WorkOrderComponent | null | undefined,
    margin = 0.18,
): WorkOrderComponent {
    const tempSeverity = (nacelleTempC - SIGNAL_METADATA.temp.warn) / Math.max(1, SIGNAL_METADATA.temp.alarm - SIGNAL_METADATA.temp.warn);
    const vibSeverity = (vibrationMmS - SIGNAL_METADATA.vibration.warn) / Math.max(1, SIGNAL_METADATA.vibration.alarm - SIGNAL_METADATA.vibration.warn);
    const raw: WorkOrderComponent = vibSeverity >= tempSeverity ? "Gearbox" : "Generator";
    if (!previous) {
        return raw;
    }
    return Math.abs(vibSeverity - tempSeverity) < margin ? previous : raw;
}

// Ids of turbines that transitioned into "alarm" since the previous status snapshot.
export function newlyAlarmed(prev: Record<string, TurbineStatus>, turbines: { id: string; status: TurbineStatus }[]): string[] {
    return turbines.filter((t) => t.status === "alarm" && prev[t.id] !== "alarm").map((t) => t.id);
}

type ViewKey = "map" | "twin" | "sites" | "alerts" | "graph" | "analytics" | "operations" | "ask";

const NAV: { key: ViewKey; label: string; icon: string }[] = [
    { key: "map", label: "Map", icon: "🗺" },
    { key: "sites", label: "Sites", icon: "🏢" },
    { key: "analytics", label: "Analytics", icon: "📊" },
    { key: "twin", label: "Digital Twin", icon: "🌀" },
    { key: "graph", label: "Graph", icon: "🕸" },
    { key: "ask", label: "Ask IQ", icon: "💬" },
    { key: "alerts", label: "Alerts", icon: "🚨" },
    { key: "operations", label: "Operations", icon: "🛠" },
];

const NAV_GROUPS: { title: string; keys: ViewKey[] }[] = [
    { title: "Monitor", keys: ["map", "sites", "analytics"] },
    { title: "Diagnose", keys: ["twin", "graph", "ask"] },
    { title: "Act", keys: ["alerts", "operations"] },
];

const VIEW_META: Record<ViewKey, { title: string; subtitle: string }> = {
    map: { title: "Global Fleet Map", subtitle: "Live situational awareness across all wind corridors." },
    twin: { title: "Turbine Digital Twin", subtitle: "Interactive component-level diagnostics and tuning." },
    sites: { title: "Site Intelligence", subtitle: "Compare performance, quality, and anomalies by site." },
    alerts: { title: "Alert Triage", subtitle: "Prioritize risk, acknowledge incidents, and dispatch fast." },
    graph: { title: "Ontology Graph", subtitle: "Trace asset links, properties, and operational context." },
    analytics: { title: "Performance Analytics", subtitle: "Track trends, deltas, and output behavior over time." },
    operations: { title: "Operations Deck", subtitle: "Coordinate work orders, notes, and control actions." },
    ask: { title: "Ask Fabric IQ", subtitle: "Natural language insights over telemetry and ontology data." },
};

const VIEW_AURA: Record<ViewKey, string> = {
    map: "radial-gradient(circle at 20% 50%, rgba(217, 164, 65, 0.28), rgba(217, 164, 65, 0))",
    twin: "radial-gradient(circle at 35% 50%, rgba(194, 138, 72, 0.28), rgba(194, 138, 72, 0))",
    sites: "radial-gradient(circle at 40% 50%, rgba(123, 196, 127, 0.24), rgba(123, 196, 127, 0))",
    alerts: "radial-gradient(circle at 30% 50%, rgba(216, 92, 87, 0.26), rgba(216, 92, 87, 0))",
    graph: "radial-gradient(circle at 45% 50%, rgba(181, 154, 110, 0.24), rgba(181, 154, 110, 0))",
    analytics: "radial-gradient(circle at 35% 50%, rgba(169, 133, 74, 0.24), rgba(169, 133, 74, 0))",
    operations: "radial-gradient(circle at 25% 50%, rgba(123, 196, 127, 0.24), rgba(123, 196, 127, 0))",
    ask: "radial-gradient(circle at 50% 50%, rgba(183, 139, 61, 0.26), rgba(183, 139, 61, 0))",
};

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
    "Which site has the highest output now?",
    "Any turbines in alarm?",
    "Which turbine has the most vibration?",
    "What is the hottest nacelle?",
    "Peak wind right now?",
    "How many turbines total?",
];

function NavRail({ view, onChange, badges }: { view: ViewKey; onChange: (v: ViewKey) => void; badges?: Partial<Record<ViewKey, number>> }) {
    const navByKey = new Map<ViewKey, { key: ViewKey; label: string; icon: string }>(NAV.map((n) => [n.key, n]));
    const navButton = (n: { key: ViewKey; label: string; icon: string }) => {
        const badge = badges?.[n.key] ?? 0;
        return (
            <button
                key={n.key}
                type="button"
                onClick={() => onChange(n.key)}
                aria-current={view === n.key ? "page" : undefined}
                aria-label={badge > 0 ? `${n.label}, ${badge} active` : n.label}
                className={`mx-2 flex items-center gap-3 rounded-lg px-3 py-2 text-left text-sm transition-all duration-200 ${view === n.key ? "nav-active-item bg-gradient-to-r from-[#8f6a30]/95 via-[#b0833f]/92 to-[#8f6a30]/95 text-[#f8f2e5] shadow-[0_10px_26px_rgba(176,131,63,0.35)]" : "text-slate-300 hover:-translate-y-[1px] hover:bg-[#252b34]/70 hover:text-[#efd7ab]"}`}
            >
                <span aria-hidden="true" className="text-lg leading-none">{n.icon}</span>
                <span className="inline md:inline">{n.label}</span>
                {view === n.key && <span aria-hidden="true" className="nav-active-dot" />}
                {badge > 0 && (
                    <span aria-hidden="true" className="ml-auto rounded-full bg-[#d85c57] px-1.5 text-[10px] font-semibold leading-4 text-white">{badge}</span>
                )}
            </button>
        );
    };

    return (
        <nav aria-label="Primary views" className="nav-wow flex w-full flex-row gap-1 overflow-x-auto border-b border-[#2a313b]/70 bg-[#14181dcc] px-2 py-2 md:w-52 md:flex-col md:overflow-visible md:border-b-0 md:border-r md:px-0 md:py-3">
            <div className="flex w-max flex-row gap-1 md:hidden">
                {NAV.map((n) => navButton(n))}
            </div>
            <div className="hidden md:flex md:flex-col md:gap-2">
                {NAV_GROUPS.map((group) => (
                    <div key={group.title} className="space-y-1">
                        <p className="px-4 text-[10px] uppercase tracking-[0.16em] text-slate-500">{group.title}</p>
                        <div className="space-y-0.5">
                            {group.keys.map((key) => {
                                const nav = navByKey.get(key);
                                return nav ? navButton(nav) : null;
                            })}
                        </div>
                    </div>
                ))}
            </div>
        </nav>
    );
}

function KpiPill({ label, value, color }: { label: string; value: string; color?: string }) {
    return (
        <div className="metric-wow tile-uniform kpi-tile rounded-lg border border-[#2a313b]/80 bg-[#1a1f26d6] px-3 py-2">
            <p className="tile-label text-[10px] uppercase tracking-wide text-slate-400">{label}</p>
            <p className="tile-value text-sm font-semibold" style={color ? { color } : undefined}>{value}</p>
        </div>
    );
}

function MetricCard({ label, value, sub, accent = "text-[#efd7ab]" }: { label: string; value: string; sub?: string; accent?: string }) {
    return (
        <div className="panel-wow tile-uniform metric-card-tile rounded-lg border border-[#2a313b]/80 bg-[#1a1f26d9] p-3 shadow-[0_10px_30px_rgba(12,14,18,0.56)]">
            <p className="tile-label text-xs uppercase tracking-wide text-slate-400">{label}</p>
            <p className={`tile-value mt-1 text-2xl font-semibold ${accent}`}>{value}</p>
            {sub && <p className="tile-sub text-xs text-slate-400">{sub}</p>}
        </div>
    );
}

function Panel({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
    return (
        <div className="panel-wow tile-uniform panel-tile rounded-lg border border-[#2a313b]/80 bg-[#1a1f26d9] p-3 shadow-[0_10px_30px_rgba(12,14,18,0.56)]">
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

// Dedicated, scaled-up single-turbine digital-twin scene with orbit + wheel zoom.
function TurbineTwinScene({ turbine, paused }: { turbine: TurbineTelemetry; paused: boolean }) {
    const hostRef = useRef<HTMLDivElement | null>(null);
    const pausedRef = useRef(paused);
    const turbineRef = useRef(turbine);
    const zoomRef = useRef(1);
    const colorApiRef = useRef<((c: string) => void) | null>(null);
    const labelRefs = useRef<Array<HTMLDivElement | null>>([]);

    useEffect(() => { pausedRef.current = paused; }, [paused]);
    useEffect(() => { turbineRef.current = turbine; }, [turbine]);

    useEffect(() => {
        const host = hostRef.current;
        if (!host) {
            return;
        }

        const testCanvas = document.createElement("canvas");
        const hasWebGL = !!(testCanvas.getContext("webgl2") || testCanvas.getContext("webgl"));
        if (!hasWebGL) {
            host.innerHTML = "<div style='padding:12px;color:#9aa3b2'>WebGL unavailable in this environment.</div>";
            return;
        }

        const scene = new THREE.Scene();
        scene.background = new THREE.Color("#081a2e");
        scene.fog = new THREE.Fog("#081a2e", 42, 130);

        const camera = new THREE.PerspectiveCamera(46, host.clientWidth / host.clientHeight, 0.1, 400);
        camera.position.set(12, 10, 24);
        camera.lookAt(0, 6.5, 0);

        const renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: "high-performance" });
        renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
        renderer.setSize(host.clientWidth, host.clientHeight);
        renderer.shadowMap.enabled = true;
        renderer.shadowMap.type = THREE.PCFSoftShadowMap;
        renderer.outputColorSpace = THREE.SRGBColorSpace;
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.12;
        host.innerHTML = "";
        host.appendChild(renderer.domElement);

        // Soft image-based reflections for the close-up turbine hardware.
        const detailSky = createSkyTexture();
        const pmrem = new THREE.PMREMGenerator(renderer);
        const envRT = pmrem.fromEquirectangular(detailSky);
        scene.environment = envRT.texture;
        pmrem.dispose();
        detailSky.dispose();

        const ambient = new THREE.AmbientLight(0xbcd4ff, 0.5);
        const sun = new THREE.DirectionalLight(0xffffff, 1.22);
        sun.position.set(12, 24, 10);
        sun.castShadow = true;
        sun.shadow.mapSize.set(2048, 2048);
        sun.shadow.camera.near = 1;
        sun.shadow.camera.far = 120;
        sun.shadow.camera.left = -20;
        sun.shadow.camera.right = 20;
        sun.shadow.camera.top = 24;
        sun.shadow.camera.bottom = -8;
        sun.shadow.bias = -0.0004;
        sun.shadow.normalBias = 0.02;
        const rim = new THREE.DirectionalLight(0x4fa3ff, 0.35);
        rim.position.set(-14, 10, -12);
        scene.add(ambient, sun, rim);

        const pad = new THREE.Mesh(
            new THREE.CircleGeometry(9, 48),
            new THREE.MeshStandardMaterial({ color: "#123247", roughness: 0.9, metalness: 0.05 })
        );
        pad.rotation.x = -Math.PI / 2;
        pad.receiveShadow = true;
        scene.add(pad);

        const padRing = new THREE.Mesh(
            new THREE.RingGeometry(8.6, 9, 48),
            new THREE.MeshBasicMaterial({ color: "#2a557f", transparent: true, opacity: 0.6 })
        );
        padRing.rotation.x = -Math.PI / 2;
        padRing.position.y = 0.02;
        scene.add(padRing);

        const grid = new THREE.GridHelper(20, 20, 0x2a557f, 0x1c3a59);
        grid.position.y = 0.01;
        (grid.material as THREE.Material).transparent = true;
        (grid.material as THREE.Material).opacity = 0.22;
        scene.add(grid);

        const group = new THREE.Group();
        scene.add(group);

        const baseMat = new THREE.MeshStandardMaterial({ color: "#7f8b9a", roughness: 0.68, metalness: 0.22 });
        const towerMat = new THREE.MeshStandardMaterial({ color: "#dde4ec", roughness: 0.32, metalness: 0.55 });
        const bladeMat = new THREE.MeshStandardMaterial({ color: "#f6f8fc", roughness: 0.35, metalness: 0.18 });
        const nacelleHousingMat = new THREE.MeshStandardMaterial({ color: "#e8edf3", roughness: 0.4, metalness: 0.5 });
        const hubMat = new THREE.MeshStandardMaterial({ color: "#cdd6e0", roughness: 0.35, metalness: 0.55 });
        const statusMat = new THREE.MeshStandardMaterial({
            color: STATUS_COLORS[turbineRef.current.status],
            emissive: STATUS_COLORS[turbineRef.current.status],
            emissiveIntensity: 0.7,
            roughness: 0.3,
            metalness: 0.2,
        });
        const ringMat = new THREE.MeshBasicMaterial({ color: STATUS_COLORS[turbineRef.current.status], transparent: true, opacity: 0.85 });

        const pedestal = new THREE.Mesh(new THREE.CylinderGeometry(1.0, 1.3, 0.5, 32), baseMat);
        pedestal.position.y = 0.25;
        pedestal.castShadow = true;
        pedestal.receiveShadow = true;
        group.add(pedestal);

        const HUB = 11;
        const tower = new THREE.Mesh(new THREE.CylinderGeometry(0.32, 0.85, HUB, 32), towerMat);
        tower.position.y = HUB / 2 + 0.3;
        tower.castShadow = true;
        group.add(tower);

        const nacelle = new THREE.Mesh(new THREE.BoxGeometry(3.0, 1.0, 1.1), nacelleHousingMat);
        nacelle.position.set(-0.3, HUB + 0.6, 0);
        nacelle.castShadow = true;
        group.add(nacelle);

        const spinner = new THREE.Mesh(new THREE.ConeGeometry(0.48, 1.1, 24), hubMat);
        spinner.rotation.z = -Math.PI / 2;
        spinner.position.set(1.7, HUB + 0.6, 0);
        spinner.castShadow = true;
        group.add(spinner);

        const beacon = new THREE.Mesh(new THREE.SphereGeometry(0.3, 16, 16), statusMat);
        beacon.position.set(-1.55, HUB + 1.25, 0);
        group.add(beacon);

        const bladeGeo = createBladeGeometry();
        const blades = new THREE.Group();
        blades.position.set(1.7, HUB + 0.6, 0);
        for (let i = 0; i < 3; i += 1) {
            const holder = new THREE.Group();
            holder.rotation.x = (i * (Math.PI * 2)) / 3;
            const blade = new THREE.Mesh(bladeGeo, bladeMat);
            blade.scale.set(2.4, 2.4, 2.4);
            blade.rotation.y = 0.22;
            blade.castShadow = true;
            holder.add(blade);
            blades.add(holder);
        }
        group.add(blades);

        const ring = new THREE.Mesh(new THREE.RingGeometry(1.7, 2.2, 48), ringMat);
        ring.rotation.x = -Math.PI / 2;
        ring.position.y = 0.05;
        group.add(ring);

        colorApiRef.current = (c: string) => {
            statusMat.color.set(c);
            statusMat.emissive.set(c);
            ringMat.color.set(c);
        };

        const onWheel = (event: WheelEvent) => {
            event.preventDefault();
            zoomRef.current = THREE.MathUtils.clamp(zoomRef.current + event.deltaY * 0.0009, 0.16, 2.0);
        };
        renderer.domElement.addEventListener("wheel", onWheel, { passive: false });

        const onResize = () => {
            if (host.clientWidth === 0 || host.clientHeight === 0) {
                return;
            }
            camera.aspect = host.clientWidth / host.clientHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(host.clientWidth, host.clientHeight);
        };
        window.addEventListener("resize", onResize);

        let tick = 0;
        const partAnchors = TWIN_PARTS.map((p) => new THREE.Vector3(p.pos[0], p.pos[1], p.pos[2]));
        const projected = new THREE.Vector3();
        const animate = () => {
            if (!pausedRef.current) {
                tick += 0.02;
                blades.rotation.x += 0.03 + Math.min(0.3, turbineRef.current.windMs / 55);
            }
            const z = zoomRef.current;
            camera.position.x = Math.sin(tick * 0.25) * 16 * z;
            camera.position.z = Math.cos(tick * 0.25) * 24 * z;
            camera.position.y = 9 * z + 3;
            // As you zoom in (smaller z), raise the focus toward the nacelle + blade
            // assembly (hub ~11.6) so the turbine's working parts fill the frame.
            const focus = THREE.MathUtils.clamp((z - 0.16) / (1 - 0.16), 0, 1);
            const focusY = THREE.MathUtils.lerp(11.2, 6.5, focus);
            camera.lookAt(0, focusY, 0);
            renderer.render(scene, camera);

            // Project each part anchor to screen space and move its HTML callout.
            const w = host.clientWidth;
            const h = host.clientHeight;
            for (let i = 0; i < partAnchors.length; i += 1) {
                const el = labelRefs.current[i];
                if (!el) {
                    continue;
                }
                projected.copy(partAnchors[i]).project(camera);
                const behind = projected.z > 1;
                const sx = (projected.x * 0.5 + 0.5) * w;
                const sy = (-projected.y * 0.5 + 0.5) * h;
                el.style.transform = `translate(-50%, -50%) translate(${sx}px, ${sy}px)`;
                el.style.opacity = behind ? "0" : "1";
            }
        };
        renderer.setAnimationLoop(animate);

        return () => {
            renderer.setAnimationLoop(null);
            renderer.domElement.removeEventListener("wheel", onWheel);
            window.removeEventListener("resize", onResize);
            colorApiRef.current = null;
            renderer.dispose();
            scene.traverse((obj) => {
                if (obj instanceof THREE.Mesh) {
                    obj.geometry.dispose();
                    const material = Array.isArray(obj.material) ? obj.material : [obj.material];
                    material.forEach((m) => m.dispose());
                }
            });
            envRT.dispose();
        };
    }, []);

    useEffect(() => {
        colorApiRef.current?.(STATUS_COLORS[turbine.status]);
    }, [turbine.status]);

    return (
        <div className="relative h-full w-full">
            <div ref={hostRef} className="h-full w-full" />
            {(() => {
                const readouts = [
                    { text: `${turbine.windMs} m/s`, color: signalColor("wind", turbine.windMs) },
                    { text: `${turbine.nacelleTempC}°C`, color: signalColor("temp", turbine.nacelleTempC) },
                    { text: `${turbine.vibrationMmS} mm/s`, color: signalColor("vibration", turbine.vibrationMmS) },
                    { text: `${turbine.powerKw.toLocaleString()} kW`, color: "#6ee7ff" },
                ];
                return TWIN_PARTS.map((p, i) => (
                    <div
                        key={p.key}
                        ref={(el) => { labelRefs.current[i] = el; }}
                        className="pointer-events-none absolute left-0 top-0 z-10 flex items-center gap-1.5 whitespace-nowrap rounded-md border border-slate-600/60 bg-[#06101fe6] px-2 py-1 text-[11px] shadow-[0_4px_14px_rgba(0,0,0,0.5)] backdrop-blur transition-opacity"
                    >
                        <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: readouts[i].color }} />
                        <span className="text-slate-400">{p.caption}</span>
                        <span className="font-semibold" style={{ color: readouts[i].color }}>{readouts[i].text}</span>
                    </div>
                ));
            })()}
            <div className="absolute bottom-3 right-3 flex flex-col overflow-hidden rounded-lg border border-slate-700/70 bg-[#06101fcc] text-slate-100 backdrop-blur">
                <button type="button" title="Zoom in" onClick={() => { zoomRef.current = THREE.MathUtils.clamp(zoomRef.current - 0.12, 0.16, 2.0); }} className="px-2.5 py-1.5 text-sm hover:bg-slate-700/60">＋</button>
                <button type="button" title="Zoom out" onClick={() => { zoomRef.current = THREE.MathUtils.clamp(zoomRef.current + 0.12, 0.16, 2.0); }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-sm hover:bg-slate-700/60">－</button>
                <button type="button" title="Reset zoom" onClick={() => { zoomRef.current = 1; }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60">⟳</button>
            </div>
        </div>
    );
}

const SIGNAL_DEFS: SignalMetadata[] = SIGNAL_ORDER.map((k) => SIGNAL_METADATA[k]);

function RelationshipGraph({ turbines, sites, selectedId, statusFilter, onSelect }: {
    turbines: TurbineTelemetry[];
    sites: WindSite[];
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
    const matches = (t: TurbineTelemetry) => statusFilter === "all" || t.status === statusFilter;
    const hoveredSiteId = hoveredSite ?? (hovered ? turbines.find((t) => t.id === hovered)?.siteId : undefined);

    // Pre-place every turbine so we can anchor the signal drill-down for the selection.
    const placed = sites.flatMap((s) => {
        const sp = sitePosById.get(s.id);
        if (!sp) {
            return [] as { t: TurbineTelemetry; sp: typeof sitePos[number]; x: number; y: number; a: number }[];
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
                    const comps = TWIN_PARTS;
                    const spread = Math.PI * 0.9;
                    const n = comps.length;
                    const severity = (s: TurbineStatus) => (s === "alarm" ? 2 : s === "warning" ? 1 : 0);
                    return (
                        <g>
                            {comps.map((comp, k) => {
                                const ca = a + (spread * (k - (n - 1) / 2)) / n;
                                const cxp = sx + Math.cos(ca) * 80;
                                const cyp = sy + Math.sin(ca) * 80;
                                const devices = TWIN_COMPONENT_DEVICES[comp.key as TwinPartKey] ?? [];
                                const worst = devices.reduce<TurbineStatus>((acc, d) => (severity(d.status(t)) > severity(acc) ? d.status(t) : acc), "healthy");
                                const compColor = STATUS_COLORS[worst];
                                const labelAbove = cyp < sy;
                                return (
                                    <g key={comp.key}>
                                        <title>{`${comp.caption} — ${devices.length} device(s)`}</title>
                                        <line x1={sx} y1={sy} x2={cxp} y2={cyp} stroke="#2c4a6b" strokeWidth={1.5} />
                                        <circle cx={cxp} cy={cyp} r={6.5} fill="#12324f" stroke={compColor} strokeWidth={1.8} />
                                        <text x={cxp} y={cyp + (labelAbove ? -10 : 16)} textAnchor="middle" fontSize={9} fill="#cbd5e1">{comp.caption}</text>
                                        {devices.map((dev, m) => {
                                            const da = ca + 0.42 * (m - (devices.length - 1) / 2);
                                            const dxp = cxp + Math.cos(da) * 60;
                                            const dyp = cyp + Math.sin(da) * 60;
                                            const st = dev.status(t);
                                            const color = STATUS_COLORS[st];
                                            const devAbove = dyp < cyp;
                                            return (
                                                <g key={dev.key}>
                                                    <title>{`${dev.label} · ${dev.property} = ${dev.value(t)} ${dev.unit} · ${st}`}</title>
                                                    <line x1={cxp} y1={cyp} x2={dxp} y2={dyp} stroke="#24405f" strokeWidth={1} strokeDasharray="3 3" />
                                                    <circle cx={dxp} cy={dyp} r={4.2} fill={color} stroke="#0a1830" strokeWidth={1} />
                                                    <text x={dxp} y={dyp + (devAbove ? -6 : 12)} textAnchor="middle" fontSize={8} fill="#93a7bd">{dev.label}</text>
                                                </g>
                                            );
                                        })}
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
    const [selectedId, setSelectedId] = useState(initialRoute.selectedId ?? "SITE-TX-WT-01");
    const [view, setView] = useState<ViewKey>(initialRoute.view ?? "map");
    const [missionTab, setMissionTab] = useState<"overview" | "risk" | "actions">("overview");
    const [detailOpen, setDetailOpen] = useState(false);
    const [focusedTwinPart, setFocusedTwinPart] = useState<TwinPartKey | null>(null);
    const [focusedTwinDevice, setFocusedTwinDevice] = useState<TwinDeviceKey | null>(null);
    const [twinDeviceGraph, setTwinDeviceGraph] = useState<Record<TwinPartKey, TwinDeviceNode[]>>(TWIN_COMPONENT_DEVICES);
    const [twinDeviceRows, setTwinDeviceRows] = useState<TurbineDeviceRecord[]>([]);
    const [deviceDraft, setDeviceDraft] = useState<TwinDeviceDraft | null>(null);
    const [deviceDraftDirty, setDeviceDraftDirty] = useState(false);
    const [deviceSaveBusy, setDeviceSaveBusy] = useState(false);
    const [deviceSaveMessage, setDeviceSaveMessage] = useState<string | null>(null);

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
    const askCacheRef = useRef<Map<string, { at: number; result: AskResult }>>(new Map());
    const [writebackMessage, setWritebackMessage] = useState<string | null>(null);
    const [ackLog, setAckLog] = useState<Record<string, { at: string; by: string }>>(() => JSON.parse(localStorage.getItem("wind-ack-log") ?? "{}"));
    const [ackMessage, setAckMessage] = useState<string | null>(null);
    const [operatorRole, setOperatorRole] = useState<OperatorRole>(() => normalizeOperatorRole(localStorage.getItem("wind-operator-role")));
    const [showAcked, setShowAcked] = useState(false);
    const [graphFilter, setGraphFilter] = useState<StatusFilter>("all");
    const [graphNonce, setGraphNonce] = useState(0);
    const [autoLogCount, setAutoLogCount] = useState(0);
    const prevStatusRef = useRef<Record<string, TurbineStatus>>({});
    const autoLoggedRef = useRef<Set<string>>(new Set());
    const autoInitRef = useRef(false);
    const anomalyHistRef = useRef<Map<string, number[]>>(new Map());
    const componentStickyRef = useRef<Record<string, WorkOrderComponent>>({});

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
    const [maintenanceOrders, setMaintenanceOrders] = useState<MaintenanceOrderRecord[]>([]);
    const [woAssignee, setWoAssignee] = useState("");
    const [woMessage, setWoMessage] = useState<string | null>(null);
    const [autoPlayRunning, setAutoPlayRunning] = useState(false);
    const [autoPlayStatus, setAutoPlayStatus] = useState<string | null>(null);
    const [demoScriptStep, setDemoScriptStep] = useState<DemoScriptStepId | "idle">("idle");
    const [demoRunLog, setDemoRunLog] = useState<MissionReportEvent[]>([]);
    const [runHistory, setRunHistory] = useState<MissionReport[]>(() => {
        try {
            return JSON.parse(localStorage.getItem("wind-run-history") ?? "[]") as MissionReport[];
        } catch {
            return [];
        }
    });
    const [demoPanelOpen, setDemoPanelOpen] = useState(false);
    const [demoStepIndex, setDemoStepIndex] = useState(0);
    const [demoIntroOpen, setDemoIntroOpen] = useState(false);
    const [techPopupOpen, setTechPopupOpen] = useState(false);
    const [techPopupResponderId, setTechPopupResponderId] = useState<string | null>(null);
    const [selectedEvidenceId, setSelectedEvidenceId] = useState(MOCK_WIND_EVIDENCE[0]?.id ?? "");
    const [responderShiftFilter, setResponderShiftFilter] = useState<"all" | "day" | "swing" | "night">(() => currentShiftByHour(new Date().getHours()));
    const [onCallOnly, setOnCallOnly] = useState(true);
    const [responderLoad, setResponderLoad] = useState<Record<string, number>>(() => JSON.parse(localStorage.getItem("wind-responder-load") ?? "{}"));
    const [escalationStage, setEscalationStage] = useState<EscalationStage>("none");
    const [demoRunCount, setDemoRunCount] = useState(0);
    const [historyWindow, setHistoryWindow] = useState<HistoryWindow>("6h");
    const [wbAction, setWbAction] = useState("Acknowledge");
    const [wbSetpoint, setWbSetpoint] = useState("");
    const [wbNote, setWbNote] = useState("");
    const canWriteback = canManageDispatch(operatorRole);
    const historyLimit = historyPointLimit(historyWindow);

    useEffect(() => {
        localStorage.setItem("wind-operator-role", operatorRole);
    }, [operatorRole]);

    useEffect(() => {
        localStorage.setItem("wind-responder-load", JSON.stringify(responderLoad));
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
                country: s.country,
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
        const seeds = twinDeviceSeeds(TWIN_COMPONENT_DEVICES);
        void (async () => {
            try {
                await ensureTurbineDevices(seeds);
                const rows = await listTurbineDevices();
                if (cancelled) {
                    return;
                }
                if (rows.length > 0) {
                    setTwinDeviceRows(rows);
                    setTwinDeviceGraph(mergeTwinDeviceGraph(rows, TWIN_COMPONENT_DEVICES));
                }
            } catch {
                /* backend unavailable — continue using bundled device graph */
            }
        })();
        return () => {
            cancelled = true;
        };
    }, []);

    useEffect(() => {
        let cancelled = false;
        ensureSignalThresholds(thresholdRows())
            .then(setThresholdsPublished)
            .catch(() => {
                /* backend not ready — publishing thresholds is best-effort */
            })
            .finally(() => {
                // Consume the published bands so operator-tuned thresholds drive
                // classification at runtime. Fallback-safe: an unreachable backend
                // leaves the compiled-in defaults untouched.
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

    useEffect(() => {
        setFocusedTwinPart(null);
        setFocusedTwinDevice(null);
    }, [selectedId]);

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
        setDeviceDraft(draftFromDevice(node, sortOrder));
        setDeviceDraftDirty(false);
    }, [focusedTwinDevice, focusedTwinPart, twinDeviceGraph, twinDeviceRows]);

    const openTurbine = useCallback((id: string) => {
        setSelectedId(id);
        setDetailOpen(true);
    }, []);

    const matchesFilter = useCallback(
        (t: TurbineTelemetry) =>
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

    // Maintain a short rolling window of each turbine's anomaly score so the watch
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

    // Seed the sparkline / forecast window from persisted Eventhouse-backed
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

    const loadOrders = useCallback(async () => {
        try {
            setMaintenanceOrders(await recentMaintenanceOrders(10));
        } catch {
            /* backend not ready — best effort */
        }
    }, []);

    useEffect(() => {
        void loadOrders();
    }, [loadOrders]);

    const acknowledgeAlert = useCallback(async (t: TurbineTelemetry) => {
        if (!canWriteback) {
            setAckMessage("Viewer mode — switch to Operator to acknowledge alarms.");
            return;
        }
        const entry = { at: new Date().toISOString(), by: "operator" };
        setAckLog((prev) => {
            const next = { ...prev, [t.id]: entry };
            localStorage.setItem("wind-ack-log", JSON.stringify(next));
            return next;
        });
        setAckMessage(`Acknowledged ${t.id}…`);
        try {
            await saveDispatchNote({
                turbineId: t.id,
                siteId: t.siteId,
                status: t.status,
                powerKw: t.powerKw,
                note: `[Acknowledge] alert ack (temp ${t.nacelleTempC} C, vib ${t.vibrationMmS} mm/s)`,
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
        const newlyAlarming: TurbineTelemetry[] = newlyAlarmed(prev, turbines)
            .filter((id) => !autoLoggedRef.current.has(id))
            .map((id) => turbines.find((t) => t.id === id))
            .filter((t): t is TurbineTelemetry => Boolean(t));
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
                        note: `[Auto] alarm onset (temp ${t.nacelleTempC} C, vib ${t.vibrationMmS} mm/s)`,
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
                    detail: `temp ${t.nacelleTempC} C, vib ${t.vibrationMmS} mm/s`,
                });
            }
            void loadNotes();
        })();
    }, [turbines, loadNotes]);

    const handleExport = useCallback(() => {
        const header = "id,site,siteId,latitude,longitude,powerKw,windMs,nacelleTempC,vibrationMmS,status";
        const rows = visibleTurbines.map((t) =>
            [t.id, t.siteName, t.siteId, t.latitude.toFixed(3), t.longitude.toFixed(3), t.powerKw, t.windMs, t.nacelleTempC, t.vibrationMmS, t.status].join(","),
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
            const sitePowerMw = local.reduce((sum, t) => sum + t.powerKw, 0) / 1000;
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

    const scenario = simulateScenario({ baselineKw: selected.powerKw, curtailmentPct: simCurtail, downtimeTicks: simDowntime, horizonTicks: simHorizon });

    const selectedForecast = forecastEscalation(anomalyHistRef.current.get(selected.id) ?? []);
    const suggestedPriority = derivePriority(anomalyScore(selected), selectedForecast.etaToAlarmTicks);
    const suggestedComponent = recommendComponentStable(selected.nacelleTempC, selected.vibrationMmS, componentStickyRef.current[selected.id]);
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
    const nextResponders = suggestedResponders.slice(1, 3);
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
        () => MOCK_WIND_EVIDENCE.filter((ev) => ev.component === suggestedComponent),
        [suggestedComponent],
    );
    const selectedEvidence = useMemo(
        () => matchingEvidence.find((ev) => ev.id === selectedEvidenceId) ?? matchingEvidence[0] ?? null,
        [matchingEvidence, selectedEvidenceId],
    );
    const techPopupEvidence = useMemo(
        () => MOCK_WIND_EVIDENCE.find((ev) => ev.component === techPopupFocusComponent) ?? selectedEvidence,
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
        () => maintenanceOrders.find((o) => o.turbineId === selected.id && o.status.toLowerCase() !== "closed"),
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
        const noteBase = `${suggestedComponent} · ${evidenceNote} · plan curtail ${simCurtail}% / downtime ${simDowntime}t · projected ${deltaKwt.toLocaleString()} kW·t`;
        const note = escalationNote ? `${noteBase} · ${escalationNote}` : noteBase;
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
            setWoMessage(`${suggestedPriority} work order raised for ${selected.id} — ${suggestedComponent} · ${assignee}${ref}.`);
            setWoAssignee("");
            void loadOrders();
            return true;
        } catch {
            const fallback = JSON.parse(localStorage.getItem("wind-workorders") ?? "[]") as unknown[];
            fallback.push(order);
            localStorage.setItem("wind-workorders", JSON.stringify(fallback));
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
    }, [canWriteback, raiseWorkOrder, selected.id, setWoMessage]);

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

    const handleAutoRunDemo = useCallback(async () => {
        if (autoPlayRunning) {
            return;
        }
        setAutoPlayRunning(true);
        setDemoScriptStep("story");
        setDemoPanelOpen(true);
        setDemoRunLog([{ step: "story", at: new Date().toISOString(), detail: "Prepared incident story" }]);
        setAutoPlayStatus("Demo script: Step 1/4 - preparing incident story...");
        setView("operations");
        handlePrimeDemoStory();

        const delay = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));
        try {
            await delay(2000);
            setDemoScriptStep("evidence");
            setDemoRunLog((log) => [...log, { step: "evidence", at: new Date().toISOString(), detail: "Selected evidence and assignee" }]);
            setAutoPlayStatus("Demo script: Step 2/4 - selecting evidence and assignee...");
            await delay(2000);
            setDemoScriptStep("dispatch");
            setDemoRunLog((log) => [...log, { step: "dispatch", at: new Date().toISOString(), detail: "Running guided dispatch" }]);
            setAutoPlayStatus("Demo script: Step 3/4 - running guided dispatch...");
            const healed = await handleAutoHealNow();
            await delay(2000);
            setDemoScriptStep("heal");
            setDemoRunLog((log) => [...log, { step: "heal", at: new Date().toISOString(), detail: healed ? "AutoHeal complete, order sent" : "AutoHeal ready, operator confirmation pending" }]);
            setAutoPlayStatus(healed ? "Demo script: Step 4/4 - complete, order sent." : "Demo script: Step 4/4 - ready, switch to Operator to complete.");
            setWoMessage((prev) => `${prev ?? ""} Demo script executed.`.trim());
            setDemoRunCount((count) => count + 1);
        } finally {
            await delay(2000);
            setAutoPlayRunning(false);
            setDemoScriptStep("idle");
        }
    }, [autoPlayRunning, handleAutoHealNow, handlePrimeDemoStory]);

    const handleOpenTechPopupWindow = useCallback(() => {
        const tech = techPopupResponder ?? primaryResponder;
        if (!tech) {
            setWoMessage("No technician profile available for popup.");
            return;
        }
        const evidence = selectedEvidence;
        const popup = window.open("", "wind-tech-popup", "width=680,height=760,noopener,noreferrer");
        if (!popup) {
            setWoMessage("Popup blocked by browser. Allow popups for this app.");
            return;
        }
        const safe = (v: string) => v.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
        const html = `<!doctype html><html><head><meta charset="utf-8"/><title>Technician Popup</title><style>
body{margin:0;font-family:Segoe UI,Arial,sans-serif;background:#0b1220;color:#e5edf7}
.card{max-width:620px;margin:20px auto;padding:16px;border:1px solid #2b3d56;border-radius:14px;background:#111b2c}
.head{display:flex;gap:12px}.head img{width:70px;height:70px;border-radius:10px;border:1px solid #3b4f6a}
.muted{color:#9fb0c4;font-size:12px}.chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}.chip{background:#25344a;padding:4px 8px;border-radius:999px;font-size:11px}
.ev{margin-top:12px}.ev img{width:100%;height:170px;object-fit:cover;border-radius:8px;border:1px solid #33475f}
</style></head><body><div class="card">
<div class="head"><img src="${safe(tech.photo)}" alt="tech"/><div><h2 style="margin:0">${safe(tech.name)}</h2><div class="muted">${safe(tech.role)} · ETA ${tech.etaMin} min · shift ${safe(tech.shift)}</div><div class="muted" style="margin-top:6px">${safe(incidentStory)}</div></div></div>
<div class="chips">${tech.skills.map((s) => `<span class="chip">${safe(s)}</span>`).join("")}</div>
${evidence ? `<div class="ev"><div class="muted">Evidence: ${safe(evidence.label)}</div><img src="${safe(evidence.image)}" alt="evidence"/></div>` : ""}
</div></body></html>`;
        popup.document.open();
        popup.document.write(html);
        popup.document.close();
        popup.focus();
    }, [incidentStory, primaryResponder, selectedEvidence, techPopupResponder]);

    const handleRunDispatchQualityCheck = useCallback(() => {
        const missing = dispatchQuality.checks.filter((c) => !c.ok).map((c) => c.label);
        if (missing.length === 0) {
            setWoMessage(`Dispatch Quality Tool: READY (${dispatchQuality.score}%).`);
            return;
        }
        setWoMessage(`Dispatch Quality Tool: MISSING -> ${missing.join(", ")} (${dispatchQuality.score}%).`);
    }, [dispatchQuality]);

    const demoScriptLead = techPopupResponder ?? primaryResponder ?? suggestedResponders[0] ?? null;
    const demoScriptSteps = useMemo<DemoScriptStep[]>(() => [
        {
            id: "story",
            label: "1. Prepare story",
            detail: "Prime the incident narrative and choose the lead technician.",
            action: () => {
                setDemoScriptStep("story");
                setView("operations");
                handlePrimeDemoStory();
            },
        },
        {
            id: "evidence",
            label: "2. Show evidence",
            detail: "Open the technician card and focus the matching asset image.",
            action: () => {
                setDemoScriptStep("evidence");
                setView("operations");
                setTechPopupOpen(true);
                if (techPopupEvidence) {
                    setSelectedEvidenceId(techPopupEvidence.id);
                }
            },
        },
        {
            id: "dispatch",
            label: "3. Dispatch lead",
            detail: "Send the selected responder and show the order response.",
            action: async () => {
                setDemoScriptStep("dispatch");
                if (!demoScriptLead) {
                    setWoMessage("Demo script: no lead technician available yet.");
                    return;
                }
                setView("operations");
                setTechPopupOpen(false);
                await handleDispatchResponder(demoScriptLead);
            },
        },
        {
            id: "heal",
            label: "4. AutoHeal",
            detail: "Run the guided recovery path and close the loop.",
            action: async () => {
                setDemoScriptStep("heal");
                setView("operations");
                setTechPopupOpen(false);
                await handleAutoHealNow();
            },
        },
    ], [demoScriptLead, handleAutoHealNow, handleDispatchResponder, handlePrimeDemoStory, techPopupEvidence]);

    const handleStartDemoFromIntro = useCallback(() => {
        setDemoIntroOpen(false);
        void handleAutoRunDemo();
    }, [handleAutoRunDemo]);

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
                    nacelleTempC: t.nacelleTempC,
                    vibrationMmS: t.vibrationMmS,
                }));
            const topOutput = [...turbines]
                .sort((a, b) => b.powerKw - a.powerKw)
                .slice(0, 8)
                .map((t) => ({ id: t.id, siteId: t.siteId, powerKw: t.powerKw, windMs: t.windMs }));

            const result = await askFabricIQ(trimmed, {
                selectedTurbineId: selected.id,
                selectedSite: selected.siteName,
                fleet: {
                    turbines: turbines.length,
                    alarms: alarmsNow,
                    warnings: warningsNow,
                    totalMw: Number((fleetKwNow / 1000).toFixed(2)),
                },
                topAlerts,
                topOutput,
                telemetry: turbines.slice(0, 24).map((t) => ({
                    id: t.id,
                    siteId: t.siteId,
                    siteName: t.siteName,
                    powerKw: t.powerKw,
                    windMs: t.windMs,
                    nacelleTempC: t.nacelleTempC,
                    vibrationMmS: t.vibrationMmS,
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

    const runAgentConnectionCheck = useCallback(async () => {
        setAgentCheckLoading(true);
        try {
            const result = await testDataAgentConnection({
                selectedTurbineId: selected.id,
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
                `${wbAction} written: setpoint ${setpointKw.toLocaleString()} kW, forecast ${forecast.toLocaleString()} kW${ref}.`,
            );
            setWbNote("");
            void loadNotes();
        } catch {
            const fallback = JSON.parse(localStorage.getItem("wind-writeback-log") ?? "[]") as unknown[];
            fallback.push({
                turbineId: selected.id,
                siteId: selected.siteId,
                status: selected.status,
                powerKw: setpointKw,
                note: composedNote,
                createdAt: new Date().toISOString(),
            });
            localStorage.setItem("wind-writeback-log", JSON.stringify(fallback));
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

    const fleetRatedMw = SITES.reduce((s, x) => s + x.turbineCount * x.capacityMw, 0);
    const capacityFactor = fleetRatedMw > 0 ? (fleetPower / 1000 / fleetRatedMw) * 100 : 0;
    const avgWind = visibleTurbines.length ? visibleTurbines.reduce((s, t) => s + t.windMs, 0) / visibleTurbines.length : 0;
    const avgTemp = visibleTurbines.length ? visibleTurbines.reduce((s, t) => s + t.nacelleTempC, 0) / visibleTurbines.length : 0;
    const outputBars = sitesSummary.map((s, i) => ({
        label: s.name,
        value: s.sitePowerMw,
        display: `${s.sitePowerMw.toFixed(2)} MW`,
        color: SITE_COLORS[i % SITE_COLORS.length],
    }));
    const topBars = topPerformers.map((t) => ({
        label: t.id,
        value: t.powerKw,
        display: `${t.powerKw.toLocaleString()} kW`,
        color: STATUS_COLORS[t.status],
    }));
    const powerCurve = visibleTurbines.map((t) => ({
        x: t.windMs,
        y: t.powerKw,
        color: STATUS_COLORS[t.status],
        label: `${t.id} · ${t.windMs} m/s · ${t.powerKw.toLocaleString()} kW · ${t.status}`,
    }));
    const curvePowerMax = Math.max(1000, ...powerCurve.map((p) => p.y));

    const selectedSite = sites.find((s) => s.id === selected.siteId);
    const relatedNotes = notes.filter((n) => n.turbineId === selected.id);

    const twinPartLayer = useMemo<TwinPartLayer | null>(() => {
        if (!focusedTwinPart) {
            return null;
        }
        switch (focusedTwinPart) {
            case "rotor": {
                const status = signalState("wind", selected.windMs);
                return {
                    key: "rotor",
                    title: "Rotor assembly",
                    property: SIGNAL_METADATA.wind.ontologyProperty,
                    value: `${selected.windMs.toFixed(1)} m/s`,
                    status,
                    note: `Wind-facing capture zone. Estimated blade tip speed ${(selected.windMs * 7.8).toFixed(1)} m/s based on current wind.`,
                };
            }
            case "nacelle": {
                const status = signalState("temp", selected.nacelleTempC);
                return {
                    key: "nacelle",
                    title: "Nacelle & generator",
                    property: SIGNAL_METADATA.temp.ontologyProperty,
                    value: `${selected.nacelleTempC.toFixed(1)} °C`,
                    status,
                    note: "Thermal envelope of generator and conversion hardware. Threshold driven by ontology Sensor bands.",
                };
            }
            case "drivetrain": {
                const status = signalState("vibration", selected.vibrationMmS);
                return {
                    key: "drivetrain",
                    title: "Drivetrain path",
                    property: SIGNAL_METADATA.vibration.ontologyProperty,
                    value: `${selected.vibrationMmS.toFixed(1)} mm/s`,
                    status,
                    note: "Mechanical stress channel (shaft, gearbox, coupling). Elevated values indicate wear or imbalance trends.",
                };
            }
            case "base": {
                const status: TurbineStatus = selected.status;
                return {
                    key: "base",
                    title: "Tower base / output",
                    property: SIGNAL_METADATA.power.ontologyProperty,
                    value: `${selected.powerKw.toLocaleString()} kW`,
                    status,
                    note: `${relatedNotes.length} dispatch note(s) linked to this turbine. Use this layer for ground-level operations and output checks.`,
                };
            }
            default:
                return null;
        }
    }, [focusedTwinPart, relatedNotes.length, selected.nacelleTempC, selected.powerKw, selected.status, selected.vibrationMmS, selected.windMs]);

    const twinDeviceDefinitions = useMemo(() => (focusedTwinPart ? twinDeviceGraph[focusedTwinPart] : []), [focusedTwinPart, twinDeviceGraph]);
    const twinDeviceLayer = useMemo(() => {
        if (!focusedTwinPart || !focusedTwinDevice) {
            return null;
        }
        const def = twinDeviceDefinitions.find((d) => d.key === focusedTwinDevice) ?? null;
        if (!def) {
            return null;
        }
        return {
            ...def,
            status: def.status(selected),
            value: def.value(selected),
        };
    }, [focusedTwinDevice, focusedTwinPart, selected, twinDeviceDefinitions]);

    const handleTwinDraftChange = useCallback((patch: Partial<TwinDeviceDraft>) => {
        setDeviceDraft((prev) => {
            if (!prev) {
                return prev;
            }
            return { ...prev, ...patch };
        });
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
        setDeviceDraft(draftFromDevice(node, sortOrder));
        setDeviceDraftDirty(false);
        setDeviceSaveMessage("Editor reset to persisted values.");
    }, [focusedTwinDevice, focusedTwinPart, twinDeviceGraph, twinDeviceRows]);

    const persistTwinRows = useCallback((rows: TurbineDeviceRecord[]) => {
        setTwinDeviceRows(rows);
        setTwinDeviceGraph(mergeTwinDeviceGraph(rows, TWIN_COMPONENT_DEVICES));
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
            const normalized = normalizeDraft(deviceDraft, {
                zoom: node.zoom,
                sortOrder: row?.sortOrder ?? componentDevices.findIndex((d) => d.key === focusedTwinDevice),
                anchor: node.anchor,
                lookAt: node.lookAt,
                offset: node.offset,
            });
            const patch: Partial<Omit<TurbineDeviceRecord, "id" | "deviceKey">> = {
                label: normalized.label,
                property: normalized.property,
                unit: normalized.unit,
                note: normalized.note,
                zoom: normalized.zoom,
                sortOrder: normalized.sortOrder,
                anchorX: normalized.anchor[0],
                anchorY: normalized.anchor[1],
                anchorZ: normalized.anchor[2],
                lookAtX: normalized.lookAt[0],
                lookAtY: normalized.lookAt[1],
                lookAtZ: normalized.lookAt[2],
                offsetX: normalized.offset[0],
                offsetY: normalized.offset[1],
                offsetZ: normalized.offset[2],
            };
            await updateTurbineDevice({ id: row?.id, deviceKey: focusedTwinDevice }, patch);
            const rows = await listTurbineDevices();
            if (rows.length > 0) {
                persistTwinRows(rows);
            }
            setDeviceDraftDirty(false);
            setDeviceSaveMessage("Twin graph device saved to backend.");
        } catch {
            setDeviceSaveMessage("Unable to save to backend right now.");
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
            await createTurbineDevice({
                deviceKey: newKey,
                component: focusedTwinPart,
                label: `${base.label} copy`,
                property: base.property,
                unit: base.unit,
                note: `${base.note} (new)`,
                anchorX: base.anchor[0],
                anchorY: base.anchor[1],
                anchorZ: base.anchor[2],
                lookAtX: base.lookAt[0],
                lookAtY: base.lookAt[1],
                lookAtZ: base.lookAt[2],
                offsetX: base.offset[0],
                offsetY: base.offset[1],
                offsetZ: base.offset[2],
                zoom: base.zoom,
                sortOrder: maxSort + 1,
            });
            const rows = await listTurbineDevices();
            if (rows.length > 0) {
                persistTwinRows(rows);
            }
            setDeviceSaveMessage("Sibling device created.");
        } catch {
            setDeviceSaveMessage("Unable to create sibling device.");
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
            setDeviceSaveMessage("At least one child device must remain for this component.");
            return;
        }
        const row = twinDeviceRows.find((r) => r.deviceKey === focusedTwinDevice);
        setDeviceSaveBusy(true);
        setDeviceSaveMessage(null);
        try {
            await deleteTurbineDevice({ id: row?.id, deviceKey: focusedTwinDevice });
            const rows = await listTurbineDevices();
            if (rows.length > 0) {
                persistTwinRows(rows);
            }
            const nextDevice = (rows.find((r) => r.component === focusedTwinPart)?.deviceKey ?? null) as TwinDeviceKey | null;
            setFocusedTwinDevice(nextDevice);
            setDeviceSaveMessage("Device deleted from backend graph.");
        } catch {
            setDeviceSaveMessage("Unable to delete this device.");
        } finally {
            setDeviceSaveBusy(false);
        }
    }, [focusedTwinDevice, focusedTwinPart, persistTwinRows, twinDeviceGraph, twinDeviceRows]);

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
                    placeholder="Find turbine…"
                    aria-label="Find turbine by id"
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

    const activeViewMeta = VIEW_META[view];
    const activeViewAura = VIEW_AURA[view];
    const mapFocusKey = `${selected.id}-${selected.status}-${selected.powerKw}`;
    const incidentMode = alarms >= 3 ? "critical" : alarms > 0 || warnings >= 4 ? "elevated" : "normal";
    const missionState = incidentMode === "critical" ? "Incident" : incidentMode === "elevated" ? "Watch" : "Nominal";

    const recommendedAction = useMemo(() => {
        if (selected.status === "alarm") {
            return `Raise ${suggestedPriority} work order on ${suggestedComponent} and dispatch immediately.`;
        }
        if (selectedForecast.etaToAlarmTicks != null && selectedForecast.etaToAlarmTicks <= 3) {
            return `Escalate to operations and pre-stage ${suggestedComponent} maintenance.`;
        }
        return "Monitor trend and keep the turbine in watch mode.";
    }, [selected.status, selectedForecast.etaToAlarmTicks, suggestedPriority, suggestedComponent]);

    const jurySnapshot = useMemo(() => {
        const eta = selectedForecast.etaToAlarmTicks != null ? `~${selectedForecast.etaToAlarmTicks} ticks` : "unknown";
        return [
            `# Wind Twin Jury Snapshot`,
            `- Generated: ${new Date().toISOString()}`,
            `- Mission state: ${missionState}`,
            `- Fleet output: ${(fleetPower / 1000).toFixed(2)} MW`,
            `- Health mix: Healthy ${healthy} | Warning ${warnings} | Alarm ${alarms}`,
            `- Focus turbine: ${selected.id} (${selected.siteName})`,
            `- Focus status: ${selected.status} | ${selected.powerKw.toLocaleString()} kW | ${selected.windMs} m/s`,
            `- Probable cause: ${suggestedComponent}`,
            `- ETA to alarm: ${eta}`,
            `- Recommended action: ${recommendedAction}`,
        ].join("\n");
    }, [alarms, fleetPower, healthy, missionState, recommendedAction, selected.id, selected.powerKw, selected.siteName, selected.status, selected.windMs, selectedForecast.etaToAlarmTicks, suggestedComponent, warnings]);

    const handleCopyJurySnapshot = useCallback(async () => {
        try {
            await navigator.clipboard.writeText(jurySnapshot);
            setWoMessage("Jury snapshot copied to clipboard.");
        } catch {
            const ta = document.createElement("textarea");
            ta.value = jurySnapshot;
            ta.setAttribute("readonly", "true");
            ta.style.position = "fixed";
            ta.style.opacity = "0";
            document.body.appendChild(ta);
            ta.select();
            document.execCommand("copy");
            document.body.removeChild(ta);
            setWoMessage("Jury snapshot copied (fallback).");
        }
    }, [jurySnapshot]);

    useEffect(() => {
        localStorage.setItem("wind-run-history", JSON.stringify(runHistory.slice(0, 10)));
    }, [runHistory]);

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

    // When the demo panel opens, focus the most critical turbine so the guided story
    // always lands on a real incident. Only auto-switches away from a healthy turbine
    // and never overrides an in-flight run.
    const demoPanelPrevOpenRef = useRef(false);
    useEffect(() => {
        const justOpened = demoPanelOpen && !demoPanelPrevOpenRef.current;
        demoPanelPrevOpenRef.current = demoPanelOpen;
        if (!justOpened || autoPlayRunning) {
            return;
        }
        const worst = activeAlerts[0];
        if (worst && selected.status === "healthy" && worst.id !== selected.id) {
            setSelectedId(worst.id);
            setWoMessage(`Demo focus set to ${worst.id} (${worst.status}).`);
        }
    }, [activeAlerts, autoPlayRunning, demoPanelOpen, selected.id, selected.status]);

    const recordMissionRun = useCallback((report: MissionReport) => {
        setRunHistory((history) => pushMissionRun(history, report, 10));
    }, []);

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

    const handleDownloadMissionReport = useCallback(() => {
        const report = buildMissionReport({
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
        });
        recordMissionRun(report);
        const blob = new Blob([JSON.stringify(report, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `mission-report-${selected.id}-${Date.now()}.json`;
        a.click();
        URL.revokeObjectURL(url);
        setWoMessage(`Mission report exported (${report.stepCount} steps, ${(report.durationMs / 1000).toFixed(1)}s).`);
    }, [demoRunLog, dispatchQuality.score, missionChallenge.score, missionChallenge.verdict, primaryResponder, recordMissionRun, selected.id, selected.siteName, selectedOpenOrder, suggestedComponent, suggestedPriority]);

    return (
        <main className="wow-surface relative flex h-full flex-col overflow-hidden bg-[radial-gradient(circle_at_12%_8%,#202833_0%,#12171f_52%,#0d1016_100%)] text-slate-100">
            <div className="pointer-events-none absolute -left-24 -top-28 h-72 w-72 rounded-full bg-[#b8c1cc]/10 blur-3xl" />
            <div className="pointer-events-none absolute -bottom-24 right-[-5rem] h-80 w-80 rounded-full bg-[#7bc47f]/10 blur-3xl" />

            <header className="header-shell wow-header animate-rise flex flex-wrap items-center gap-3 border-b border-[#2a313b]/70 bg-[#151a20d9] px-3 py-3 backdrop-blur-sm sm:px-5">
                <div className="header-brand mr-2">
                    <p className="text-[11px] uppercase tracking-[0.2em] text-[#b8c1cc]">Fabric Rayfin App</p>
                    <h1 className="header-title text-xl font-semibold leading-tight">Geo Wind Twin Command Center</h1>
                    <div className="mt-0.5 flex flex-wrap items-center gap-2 text-[10px]">
                        <span className={`rounded-full px-2 py-0.5 font-semibold ${missionState === "Incident" ? "bg-red-900/50 text-red-200" : missionState === "Watch" ? "bg-amber-900/50 text-amber-200" : "bg-emerald-900/50 text-emerald-200"}`}>
                            {missionState}
                        </span>
                        <span className="header-context text-slate-400">{activeViewMeta.title}</span>
                    </div>
                </div>

                <div className="view-spotlight header-spotlight tile-uniform hidden min-w-[320px] rounded-xl border border-[#b8c1cc]/35 bg-[#1a2027d9] px-4 py-3 lg:block">
                    <p className="text-[10px] uppercase tracking-[0.18em] text-[#b8c1cc]/95">Now Viewing</p>
                    <p className="text-base font-semibold text-[#e7edf5]">{activeViewMeta.title}</p>
                    <p className="text-xs text-slate-300/90">{activeViewMeta.subtitle}</p>
                </div>

                <div className="header-kpi-grid flex flex-wrap items-center gap-2">
                    <KpiPill label="Fleet" value={`${(fleetPower / 1000).toFixed(2)} MW`} color="#b8c1cc" />
                    <KpiPill label="Alarm" value={String(alarms)} color={STATUS_COLORS.alarm} />
                    <KpiPill label="Capacity" value={`${capacityFactor.toFixed(0)}%`} color="#7bc47f" />
                    <div className="metric-wow tile-uniform kpi-tile rounded-lg border border-[#2a313b]/80 bg-[#1a1f26d6] px-3 py-2">
                        <p className="tile-label text-[10px] uppercase tracking-wide text-slate-400">Health Mix</p>
                        <p className="tile-value text-sm font-semibold text-slate-200">H {healthy} · W {warnings} · T {visibleTurbines.length}/{turbines.length}</p>
                    </div>
                </div>

                <div className="header-controls ml-auto flex w-full flex-wrap items-center justify-end gap-2 text-xs text-slate-300 sm:w-auto">
                    <div className="relative">
                        <button
                            type="button"
                            onClick={() => setDemoPanelOpen((v) => !v)}
                            className={`flex items-center gap-1.5 rounded border px-2.5 py-1.5 font-semibold ${demoPanelOpen || autoPlayRunning ? "border-cyan-400 bg-cyan-600/25 text-cyan-100" : "border-cyan-700/50 bg-[#08182c] text-cyan-200 hover:bg-cyan-900/30"}`}
                            aria-expanded={demoPanelOpen}
                            aria-haspopup="dialog"
                            title="Guided demo script"
                        >
                            <span>▶ Demo</span>
                            {autoPlayRunning && <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-cyan-300" />}
                        </button>
                        {demoPanelOpen && (
                            <>
                                <button type="button" aria-label="Close demo panel" onClick={() => setDemoPanelOpen(false)} className="fixed inset-0 z-[190] cursor-default" />
                                <div role="dialog" aria-modal="true" aria-label="Guided demo script" className="absolute right-0 top-[calc(100%+0.5rem)] z-[200] flex max-w-[92vw] flex-wrap items-center gap-2 rounded-2xl border border-cyan-500/50 bg-[#0a1526] px-2 py-1.5 shadow-[0_12px_40px_rgba(0,0,0,0.7)]">
                                    <span className="pl-1 text-[10px] font-semibold uppercase tracking-wide text-cyan-300">Demo</span>
                                    <button
                                        type="button"
                                        onClick={() => setDemoStepIndex((i) => Math.max(0, i - 1))}
                                        disabled={autoPlayRunning || demoStepIndex === 0}
                                        className="rounded-full border border-slate-700 bg-[#0a1830] px-2 py-0.5 text-xs text-slate-200 hover:border-cyan-500 disabled:cursor-not-allowed disabled:opacity-40"
                                        aria-label="Previous step"
                                        title="Previous step (←)"
                                    >
                                        ◀
                                    </button>
                                    <span className="whitespace-nowrap text-[11px] text-slate-100">
                                        <span className="text-cyan-300/80">{demoStepIndex + 1}/{demoScriptSteps.length}</span>{" "}
                                        <span className="font-semibold">{demoScriptSteps[demoStepIndex]?.label.replace(/^\d+\.\s*/, "")}</span>
                                    </span>
                                    <button
                                        type="button"
                                        onClick={() => setDemoStepIndex((i) => Math.min(demoScriptSteps.length - 1, i + 1))}
                                        disabled={autoPlayRunning || demoStepIndex >= demoScriptSteps.length - 1}
                                        className="rounded-full border border-slate-700 bg-[#0a1830] px-2 py-0.5 text-xs text-slate-200 hover:border-cyan-500 disabled:cursor-not-allowed disabled:opacity-40"
                                        aria-label="Next step"
                                        title="Next step (→)"
                                    >
                                        ▶
                                    </button>
                                    <span className="mx-0.5 h-4 w-px bg-slate-700" />
                                    <button
                                        type="button"
                                        onClick={() => void demoScriptSteps[demoStepIndex]?.action()}
                                        disabled={autoPlayRunning}
                                        title="Run current step (Enter)"
                                        className="rounded-full border border-cyan-600/70 bg-cyan-900/25 px-2.5 py-0.5 text-[11px] font-medium text-cyan-100 hover:bg-cyan-800/40 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        Run step
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => setDemoIntroOpen(true)}
                                        disabled={autoPlayRunning}
                                        className="rounded-full bg-cyan-600 px-3 py-0.5 text-[11px] font-semibold text-white hover:bg-cyan-500 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        {autoPlayRunning ? "Running…" : "Run all"}
                                    </button>
                                    <button type="button" onClick={() => setDemoPanelOpen(false)} className="rounded-full px-1.5 text-slate-400 hover:bg-slate-800 hover:text-slate-100" aria-label="Close demo panel">✕</button>
                                </div>
                            </>
                        )}
                    </div>
                    <button
                        type="button"
                        onClick={() => setLive((v) => !v)}
                        className={`rounded px-2.5 py-1.5 font-medium text-white ${live ? "bg-[#5f915f]" : "bg-[#4a525e]"}`}
                    >
                        {live ? "⏸ Pause" : "▶ Resume"}
                    </button>
                    <select
                        value={refreshMs}
                        onChange={(e) => setRefreshMs(Number(e.target.value))}
                        className="rounded border border-[#2a313b] bg-[#1a1f26] px-2 py-1.5"
                    >
                        <option value={1000}>1s</option>
                        <option value={2500}>2.5s</option>
                        <option value={5000}>5s</option>
                    </select>
                    <div className="hidden text-right sm:block">
                        <div className="text-slate-400">pde_windturbine</div>
                        <div>{new Date().toLocaleTimeString()}</div>
                    </div>
                </div>
            </header>

            {demoIntroOpen && (
                <div className="fixed inset-0 z-[210] flex items-center justify-center bg-black/60 p-4" onClick={() => setDemoIntroOpen(false)}>
                    <div role="dialog" aria-modal="true" aria-label="Guided demo introduction" className="w-full max-w-md rounded-2xl border border-cyan-500/40 bg-[#0a1526] p-5 shadow-[0_24px_70px_rgba(0,0,0,0.7)]" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-start justify-between gap-3">
                            <div>
                                <p className="text-[11px] uppercase tracking-[0.18em] text-cyan-300">Guided Demo</p>
                                <h2 className="mt-0.5 text-lg font-semibold text-slate-100">Incident triage & dispatch walkthrough</h2>
                            </div>
                            <button type="button" onClick={() => setDemoIntroOpen(false)} className="rounded p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100" aria-label="Close intro">✕</button>
                        </div>

                        <div className="mt-3 rounded-lg border border-[#2a313b]/70 bg-[#101925] p-3 text-xs text-slate-300">
                            <p>We will walk through four scripted steps on the most critical turbine:</p>
                            <ol className="mt-2 space-y-1">
                                {demoScriptSteps.map((step, i) => (
                                    <li key={step.id} className="flex gap-2">
                                        <span className="text-cyan-300">{i + 1}.</span>
                                        <span><span className="font-medium text-slate-100">{step.label.replace(/^\d+\.\s*/, "")}</span> — {step.detail}</span>
                                    </li>
                                ))}
                            </ol>
                        </div>

                        <div className="mt-3 grid grid-cols-3 gap-2 text-[11px]">
                            <div className="rounded bg-[#101925] px-2 py-1.5"><p className="text-slate-400">Turbine</p><p className="font-semibold text-slate-100">{selected.id}</p></div>
                            <div className="rounded bg-[#101925] px-2 py-1.5"><p className="text-slate-400">Cause</p><p className="font-semibold text-cyan-200">{suggestedComponent}</p></div>
                            <div className="rounded bg-[#101925] px-2 py-1.5"><p className="text-slate-400">Priority</p><p className="font-semibold text-amber-200">{suggestedPriority}</p></div>
                        </div>
                        {primaryResponder && <p className="mt-2 text-[11px] text-slate-400">Lead technician: <span className="font-medium text-slate-200">{primaryResponder.name}</span> · {primaryResponder.role}</p>}

                        <div className="mt-4 flex gap-2">
                            <button type="button" onClick={() => setDemoIntroOpen(false)} className="flex-1 rounded border border-slate-600 bg-[#0a1830] px-3 py-2 text-sm font-medium text-slate-200 hover:border-slate-400">Cancel</button>
                            <button type="button" onClick={handleStartDemoFromIntro} className="flex-1 rounded bg-cyan-600 px-3 py-2 text-sm font-semibold text-white hover:bg-cyan-500">Start demo</button>
                        </div>
                    </div>
                </div>
            )}

            {demoScriptStep !== "idle" && !demoPanelOpen && (() => {
                const narration = demoNarration(demoScriptStep);
                return (
                    <div className="pointer-events-none fixed inset-x-0 bottom-6 z-[120] flex justify-center px-4">
                        <div role="status" aria-live="polite" className="pointer-events-auto w-full max-w-[640px] rounded-xl border border-cyan-500/40 bg-[#061224f2] px-4 py-3 shadow-[0_18px_50px_rgba(0,0,0,0.55)] backdrop-blur-md">
                            <div className="flex items-center justify-between gap-2">
                                <span className="rounded-full bg-cyan-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-cyan-200">
                                    Demo narration · Step {narration.index} of {narration.total}
                                </span>
                                <div className="flex items-center gap-2">
                                    {autoPlayRunning && <span className="text-[10px] text-cyan-300/80">Auto-playing</span>}
                                    {!autoPlayRunning && (
                                        <button type="button" onClick={() => setDemoScriptStep("idle")} className="rounded px-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100" aria-label="Dismiss narration">✕</button>
                                    )}
                                </div>
                            </div>
                            <p className="mt-1.5 text-sm font-semibold text-slate-100">{narration.title}</p>
                            <p className="mt-0.5 text-xs leading-5 text-slate-300">{narration.caption}</p>
                            <div className="mt-2 flex gap-1">
                                {Array.from({ length: narration.total }).map((_, i) => (
                                    <span key={i} className={`h-1 flex-1 rounded-full ${i < narration.index ? "bg-cyan-400" : "bg-slate-700"}`} />
                                ))}
                            </div>
                        </div>
                    </div>
                );
            })()}

            {incidentMode !== "normal" && (
                <div className={`incident-banner px-4 py-2 text-xs ${incidentMode === "critical" ? "incident-critical" : "incident-elevated"}`}>
                    <div className="mx-auto flex max-w-[1600px] items-center justify-between gap-3">
                        <p>
                            {incidentMode === "critical"
                                ? `Incident mode active: ${alarms} alarms in scope. Focus on immediate triage and dispatch.`
                                : `Watch mode active: ${warnings} warnings in scope. Escalation trend detected.`}
                        </p>
                        <button type="button" onClick={() => setView("alerts")} className="rounded bg-black/25 px-2 py-1 text-[11px] font-semibold text-white hover:bg-black/40">
                            Open Triage
                        </button>
                    </div>
                </div>
            )}

            <div className="flex min-h-0 flex-1 flex-col md:flex-row">
                <NavRail view={view} onChange={setView} badges={{ alerts: unackedAlerts.length }} />

                <section key={view} className="view-stage relative min-h-0 flex-1 animate-rise" style={{ animationDelay: "80ms" }}>
                    <div className="pointer-events-none absolute left-3 right-3 top-2 z-0 h-24 rounded-2xl blur-2xl" style={{ background: activeViewAura }} />
                    <div className="view-spotlight mx-4 mt-3 rounded-xl border border-[#b8c1cc]/30 bg-[#1a2027d2] px-3 py-2 lg:hidden">
                        <p className="text-[10px] uppercase tracking-[0.18em] text-[#b8c1cc]/90">Now Viewing</p>
                        <p className="text-sm font-semibold text-[#e7edf5]">{activeViewMeta.title}</p>
                        <p className="text-[11px] text-slate-300/90">{activeViewMeta.subtitle}</p>
                    </div>

                    {view === "map" && (
                        <div className="relative h-full min-h-[420px] md:min-h-[520px]">
                            <SceneErrorBoundary label="Fleet map">
                                <Suspense fallback={<div className="h-full w-full animate-pulse bg-[#051020]" />}>
                                    <LazyWindFarmScene turbines={turbines} sites={sites} selectedId={selected.id} dimmedIds={dimmedIds} paused={!live} onSelect={openTurbine} />
                                </Suspense>
                            </SceneErrorBoundary>

                            <div className="absolute left-2 right-2 top-2 rounded-lg border border-slate-700/60 bg-[#06101fd9] p-2 backdrop-blur sm:left-3 sm:right-auto sm:max-w-[78%] lg:right-[350px] lg:max-w-[calc(100%-366px)]">
                                {toolbar}
                            </div>

                            <div key={mapFocusKey} className="mission-panel absolute right-3 top-3 hidden w-[330px] rounded-xl border border-cyan-400/30 bg-[#051224e4] p-3 backdrop-blur-md lg:block">
                                <div className="flex items-center justify-between">
                                    <p className="text-[10px] uppercase tracking-[0.16em] text-cyan-300/90">Mission Panel</p>
                                    <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${isLiveTelemetryConfigured() ? "bg-emerald-900/50 text-emerald-200" : "bg-amber-900/50 text-amber-200"}`}>
                                        {isLiveTelemetryConfigured() ? "LIVE" : "SIM"}
                                    </span>
                                </div>
                                <div className="mt-2 flex gap-1 rounded-lg border border-slate-700/70 bg-[#07182f] p-1">
                                    {(["overview", "risk", "actions"] as const).map((tab) => (
                                        <button
                                            key={tab}
                                            type="button"
                                            onClick={() => setMissionTab(tab)}
                                            className={`mission-tab-btn flex-1 rounded px-2 py-1.5 text-xs font-medium capitalize ${missionTab === tab ? "bg-cyan-600 text-white" : "text-slate-300 hover:bg-slate-800/70"}`}
                                        >
                                            {tab}
                                        </button>
                                    ))}
                                </div>

                                {missionTab === "overview" && (
                                    <div className="mt-2 space-y-2">
                                        <div>
                                            <p className="text-sm font-semibold text-slate-100">{selected.id}</p>
                                            <p className="text-xs text-slate-400">{selected.siteName}</p>
                                            <p className="text-xs" style={{ color: STATUS_COLORS[selected.status] }}>
                                                {selected.status} · {selected.powerKw.toLocaleString()} kW · {selected.windMs} m/s
                                            </p>
                                        </div>
                                        <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} />
                                        <div className="grid grid-cols-3 gap-2 text-[11px]">
                                            <div className="mission-mini-tile rounded px-2 py-1.5"><p className="tile-label text-slate-400">Fleet</p><p className="tile-value font-semibold text-cyan-100">{(fleetPower / 1000).toFixed(1)} MW</p></div>
                                            <div className="mission-mini-tile rounded px-2 py-1.5"><p className="tile-label text-slate-400">Capacity</p><p className="tile-value font-semibold text-emerald-200">{capacityFactor.toFixed(0)}%</p></div>
                                            <div className="mission-mini-tile rounded px-2 py-1.5"><p className="tile-label text-slate-400">Alarms</p><p className="tile-value font-semibold text-red-200">{alarms}</p></div>
                                        </div>
                                    </div>
                                )}

                                {missionTab === "risk" && (
                                    <div className="mt-2 space-y-2 text-xs">
                                        <div className="mission-mini-tile rounded px-2 py-1.5 text-slate-300">Unacknowledged alerts: <span className="font-semibold text-red-200">{unackedAlerts.length}</span></div>
                                        <div className="mission-mini-tile rounded px-2 py-1.5 text-slate-300">Watchlist leader: <span className="font-semibold text-amber-200">{anomalyWatch[0]?.t.id ?? "—"}</span></div>
                                        <div className="mission-mini-tile rounded px-2 py-1.5 text-slate-300">Top site output: <span className="font-semibold text-cyan-100">{siteSummaries[0]?.name ?? "—"}</span></div>
                                        <div className="mission-mini-tile mission-story-tile rounded border border-cyan-500/30 px-2 py-1.5 text-[11px]">
                                            <p className="uppercase tracking-wide text-cyan-300">Storyboard</p>
                                            <p className="mt-1 text-slate-200">Cause probable: <span className="font-semibold text-cyan-100">{suggestedComponent}</span></p>
                                            <p className="text-slate-300">ETA alarm: <span className="font-semibold text-amber-200">{selectedForecast.etaToAlarmTicks != null ? `~${selectedForecast.etaToAlarmTicks} ticks` : "unknown"}</span></p>
                                            <p className="text-slate-300">Action: <span className="font-semibold text-emerald-200">{recommendedAction}</span></p>
                                        </div>
                                        <div className="h-1.5 rounded bg-slate-800">
                                            <div className="h-1.5 rounded bg-gradient-to-r from-cyan-400 to-emerald-300" style={{ width: `${Math.min(100, Math.max(0, capacityFactor))}%` }} />
                                        </div>
                                    </div>
                                )}

                                {missionTab === "actions" && (
                                    <div className="mt-2 space-y-2 text-xs">
                                        <div className="mission-mini-tile rounded border border-cyan-500/25 px-2 py-1.5">
                                            <p className="uppercase tracking-wide text-cyan-300">Challenge score</p>
                                            <div className="mt-1 flex items-center justify-between">
                                                <span className="text-xl font-semibold text-slate-100">{missionChallenge.score}%</span>
                                                <span
                                                    className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${missionChallenge.verdict === "ready" ? "bg-emerald-900/60 text-emerald-200" : missionChallenge.verdict === "watch" ? "bg-amber-900/60 text-amber-200" : "bg-rose-900/60 text-rose-200"}`}
                                                >
                                                    {missionChallenge.verdict.toUpperCase()}
                                                </span>
                                            </div>
                                            <div className="mt-1 h-1.5 rounded bg-slate-800">
                                                <div className="h-1.5 rounded bg-gradient-to-r from-cyan-400 to-emerald-300" style={{ width: `${missionChallenge.score}%` }} />
                                            </div>
                                        </div>

                                        <div className="mission-action-grid grid grid-cols-2 gap-2">
                                            <button type="button" onClick={() => setDemoIntroOpen(true)} className="mission-action-btn mission-action-sky col-span-2 rounded px-2 py-1.5 text-xs font-medium">Run Demo Script</button>
                                            <button type="button" onClick={() => void handleAutoHealNow()} className="mission-action-btn mission-action-success rounded px-2 py-1.5 text-xs font-medium">Dispatch Lead</button>
                                            <button type="button" onClick={() => void handleEscalateManager()} className="mission-action-btn mission-action-danger rounded px-2 py-1.5 text-xs font-medium">Escalate L1</button>
                                            <button type="button" onClick={() => setView("operations")} className="mission-action-btn mission-action-primary rounded px-2 py-1.5 text-xs font-medium">Operations</button>
                                            <button type="button" onClick={() => setView("ask")} className="mission-action-btn mission-action-outline rounded px-2 py-1.5 text-xs font-medium">Ask Fabric IQ</button>
                                            <button type="button" onClick={() => void handleCopyJurySnapshot()} className="mission-action-btn mission-action-sky rounded px-2 py-1.5 text-xs font-medium">Copy Snapshot</button>
                                            <button type="button" onClick={handleDownloadMissionReport} className="mission-action-btn mission-action-indigo rounded px-2 py-1.5 text-xs font-medium">Export Report</button>
                                        </div>

                                        {runHistory.length > 0 && (
                                            <div className="rounded border border-slate-700/60 bg-[#07182f] p-2">
                                                <div className="flex items-center justify-between">
                                                    <p className="text-[10px] font-semibold uppercase tracking-wide text-cyan-300/90">Run history</p>
                                                    <button type="button" onClick={() => setRunHistory([])} className="text-[10px] text-slate-400 hover:text-slate-200">Clear</button>
                                                </div>
                                                <ul className="mt-1 space-y-1">
                                                    {runHistory.slice(0, 5).map((r) => {
                                                        const s = summarizeMissionRun(r);
                                                        return (
                                                            <li key={s.id}>
                                                                <button
                                                                    type="button"
                                                                    onClick={() => { setSelectedId(r.turbineId); setView("operations"); setWoMessage(`Replay ${r.turbineId}: ${r.component} · challenge ${r.challengeScore}% (${r.challengeVerdict}) · ${s.durationSec}s · ${r.stepCount} steps.`); }}
                                                                    className="w-full rounded border border-slate-700/60 bg-[#0a1830] px-2 py-1 text-left text-[11px] hover:border-cyan-500 hover:bg-[#0d2140]"
                                                                >
                                                                    <div className="flex items-center justify-between gap-2">
                                                                        <span className="truncate font-medium text-slate-100">{s.turbineId}</span>
                                                                        <span className={s.challengeVerdict === "ready" ? "text-emerald-300" : s.challengeVerdict === "watch" ? "text-amber-300" : "text-rose-300"}>{s.challengeScore}%</span>
                                                                    </div>
                                                                    <div className="flex items-center justify-between text-[9px] text-slate-500">
                                                                        <span>{new Date(s.at).toLocaleTimeString()}</span>
                                                                        <span>{s.component} · {s.durationSec}s</span>
                                                                    </div>
                                                                </button>
                                                            </li>
                                                        );
                                                    })}
                                                </ul>
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>

                            <div className="absolute bottom-3 left-3 hidden rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-1.5 text-xs text-slate-400 sm:block">
                                Click any turbine to open its live detail popup · drag to pan · scroll or use ＋ / － to zoom.
                            </div>
                        </div>
                    )}

                    {view === "twin" && (
                        <div className="h-full overflow-y-auto p-4">
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
                                                <LazyTurbineTwinScene turbine={selected} paused={!live} onPartFocusChange={setFocusedTwinPart} onDeviceFocusChange={setFocusedTwinDevice} deviceGraph={twinDeviceGraph} />
                                            </Suspense>
                                        </SceneErrorBoundary>
                                        <div className="absolute left-3 top-3 rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-2 backdrop-blur">
                                            <p className="text-sm font-semibold">{selected.id}</p>
                                            <p className="text-xs text-slate-400">{selected.siteName} · {selected.latitude.toFixed(2)}, {selected.longitude.toFixed(2)}</p>
                                        </div>
                                    </div>
                                </div>

                                <div className="space-y-3">
                                    <Panel title="Ontology entity">
                                        <dl className="space-y-1 text-sm text-slate-300">
                                            <div className="flex justify-between"><dt className="text-slate-400">Entity type</dt><dd className="text-cyan-200">WindTurbine</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Instance</dt><dd>{selected.id}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">belongsTo → WindSite</dt><dd>{selected.siteName}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Country</dt><dd>{selectedSite?.country ?? "—"}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Coordinates</dt><dd>{selected.latitude.toFixed(3)}, {selected.longitude.toFixed(3)}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Rated capacity</dt><dd>{selectedSite ? `${selectedSite.capacityMw} MW` : "—"}</dd></div>
                                        </dl>
                                    </Panel>

                                    <Panel title="Selected component graph">
                                        {!focusedTwinPart || !twinPartLayer ? (
                                            <p className="text-xs text-slate-400">Click a component in the 3D twin to expand its child devices.</p>
                                        ) : (
                                            <div className="space-y-3 text-sm text-slate-300">
                                                <div className="flex items-center justify-between">
                                                    <span className="font-medium text-slate-200">{twinPartLayer.title}</span>
                                                    <span className="rounded-full px-2 py-0.5 text-xs font-medium" style={{ backgroundColor: `${STATUS_COLORS[twinPartLayer.status]}22`, color: STATUS_COLORS[twinPartLayer.status] }}>
                                                        {twinPartLayer.status.toUpperCase()}
                                                    </span>
                                                </div>

                                                <div className="rounded border border-slate-700/60 bg-[#0a1830] p-2 text-xs">
                                                    <div className="flex justify-between gap-2"><span className="text-slate-400">Ontology property</span><span className="text-cyan-200">{twinPartLayer.property}</span></div>
                                                    <div className="mt-1 flex justify-between gap-2"><span className="text-slate-400">Current value</span><span style={{ color: STATUS_COLORS[twinPartLayer.status] }}>{twinPartLayer.value}</span></div>
                                                </div>
                                                <p className="text-xs text-slate-400">{twinPartLayer.note}</p>

                                                <div className="rounded border border-slate-700/60 bg-[#071526] p-2">
                                                    <p className="mb-2 text-[11px] uppercase tracking-wide text-slate-400">Child devices</p>
                                                    <div className="flex flex-wrap gap-2">
                                                        {twinDeviceDefinitions.map((device) => {
                                                            const selectedDevice = focusedTwinDevice === device.key;
                                                            const status = device.status(selected);
                                                            return (
                                                                <button
                                                                    key={device.key}
                                                                    type="button"
                                                                    onClick={() => setFocusedTwinDevice(device.key)}
                                                                    className={`rounded border px-2 py-1 text-left text-xs transition ${selectedDevice ? "border-cyan-400 bg-cyan-500/15 text-cyan-100" : "border-slate-700 bg-[#0a1830] text-slate-300 hover:border-slate-500"}`}
                                                                >
                                                                    <div className="flex items-center gap-1.5">
                                                                        <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: STATUS_COLORS[status] }} />
                                                                        <span className="font-medium">{device.label}</span>
                                                                    </div>
                                                                    <div className="mt-0.5 text-[11px] text-slate-400">{device.property}</div>
                                                                </button>
                                                            );
                                                        })}
                                                    </div>
                                                </div>

                                                {twinDeviceLayer ? (
                                                    <div className="rounded border border-slate-700/60 bg-[#0a1830] p-2 text-xs">
                                                        <div className="flex items-center justify-between gap-2">
                                                            <span className="font-medium text-slate-200">{twinDeviceLayer.label}</span>
                                                            <span className="rounded-full px-2 py-0.5 font-medium" style={{ backgroundColor: `${STATUS_COLORS[twinDeviceLayer.status]}22`, color: STATUS_COLORS[twinDeviceLayer.status] }}>
                                                                {twinDeviceLayer.status.toUpperCase()}
                                                            </span>
                                                        </div>
                                                        <div className="mt-2 flex justify-between gap-2"><span className="text-slate-400">Property</span><span className="text-cyan-200">{twinDeviceLayer.property}</span></div>
                                                        <div className="mt-1 flex justify-between gap-2"><span className="text-slate-400">Value</span><span style={{ color: STATUS_COLORS[twinDeviceLayer.status] }}>{twinDeviceLayer.value} {twinDeviceLayer.unit}</span></div>
                                                        <p className="mt-2 text-slate-400">{twinDeviceLayer.note}</p>
                                                    </div>
                                                ) : (
                                                    <p className="text-xs text-slate-500">Click a child device above or in the twin scene for a second-level graph focus.</p>
                                                )}
                                            </div>
                                        )}
                                    </Panel>

                                    <Panel
                                        title="Twin graph admin"
                                        action={
                                            <span className="text-[10px] text-slate-500">
                                                {deviceSaveBusy ? "saving…" : "backend editor"}
                                            </span>
                                        }
                                    >
                                        {!focusedTwinDevice || !deviceDraft ? (
                                            <p className="text-xs text-slate-400">Select a child device to edit graph metadata.</p>
                                        ) : (
                                            <div className="space-y-2 text-xs">
                                                <div className="grid grid-cols-2 gap-2">
                                                    <label className="space-y-1">
                                                        <span className="text-slate-400">Label</span>
                                                        <input value={deviceDraft.label} onChange={(e) => handleTwinDraftChange({ label: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                    </label>
                                                    <label className="space-y-1">
                                                        <span className="text-slate-400">Property</span>
                                                        <input value={deviceDraft.property} onChange={(e) => handleTwinDraftChange({ property: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                    </label>
                                                </div>
                                                <div className="grid grid-cols-3 gap-2">
                                                    <label className="space-y-1">
                                                        <span className="text-slate-400">Unit</span>
                                                        <input value={deviceDraft.unit} onChange={(e) => handleTwinDraftChange({ unit: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                    </label>
                                                    <label className="space-y-1">
                                                        <span className="text-slate-400">Zoom</span>
                                                        <input value={deviceDraft.zoom} onChange={(e) => handleTwinDraftChange({ zoom: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                    </label>
                                                    <label className="space-y-1">
                                                        <span className="text-slate-400">Sort</span>
                                                        <input value={deviceDraft.sortOrder} onChange={(e) => handleTwinDraftChange({ sortOrder: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                    </label>
                                                </div>
                                                <label className="space-y-1">
                                                    <span className="text-slate-400">Anchor (x,y,z)</span>
                                                    <input value={deviceDraft.anchor} onChange={(e) => handleTwinDraftChange({ anchor: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                </label>
                                                <label className="space-y-1">
                                                    <span className="text-slate-400">LookAt (x,y,z)</span>
                                                    <input value={deviceDraft.lookAt} onChange={(e) => handleTwinDraftChange({ lookAt: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                </label>
                                                <label className="space-y-1">
                                                    <span className="text-slate-400">Offset (x,y,z)</span>
                                                    <input value={deviceDraft.offset} onChange={(e) => handleTwinDraftChange({ offset: e.target.value })} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                </label>
                                                <label className="space-y-1">
                                                    <span className="text-slate-400">Note</span>
                                                    <textarea value={deviceDraft.note} onChange={(e) => handleTwinDraftChange({ note: e.target.value })} rows={2} className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
                                                </label>
                                                <div className="flex flex-wrap gap-2 pt-1">
                                                    <button type="button" onClick={() => void handleTwinDraftSave()} disabled={deviceSaveBusy || !deviceDraftDirty} className="rounded bg-cyan-600 px-2 py-1 font-medium text-white disabled:cursor-not-allowed disabled:opacity-50">Save</button>
                                                    <button type="button" onClick={handleTwinDraftReset} disabled={deviceSaveBusy || !deviceDraftDirty} className="rounded bg-slate-700 px-2 py-1 text-white disabled:cursor-not-allowed disabled:opacity-50">Reset</button>
                                                    <button type="button" onClick={() => void handleTwinAddSibling()} disabled={deviceSaveBusy} className="rounded bg-emerald-600 px-2 py-1 text-white disabled:cursor-not-allowed disabled:opacity-50">Add sibling</button>
                                                    <button type="button" onClick={() => void handleTwinDeleteDevice()} disabled={deviceSaveBusy} className="rounded bg-red-700 px-2 py-1 text-white disabled:cursor-not-allowed disabled:opacity-50">Delete</button>
                                                </div>
                                                {deviceSaveMessage && <p className="text-[11px] text-cyan-200">{deviceSaveMessage}</p>}
                                            </div>
                                        )}
                                    </Panel>

                                    <Panel title="Live signals (timeseries)">
                                        <div className="mb-2">
                                            <p className="text-xs text-slate-400">Active power</p>
                                            <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} />
                                            <p className="mt-0.5 text-right text-xs text-cyan-200">{selected.powerKw.toLocaleString()} kW</p>
                                        </div>
                                        <div className="space-y-2">
                                            <Meter label="Wind speed" value={selected.windMs} unit="m/s" max={25} warn={SIGNAL_METADATA.wind.warn} alarm={SIGNAL_METADATA.wind.alarm} property={SIGNAL_METADATA.wind.ontologyProperty} />
                                            <Meter label="Nacelle temp" value={selected.nacelleTempC} unit="°C" max={95} warn={SIGNAL_METADATA.temp.warn} alarm={SIGNAL_METADATA.temp.alarm} property={SIGNAL_METADATA.temp.ontologyProperty} />
                                            <Meter label="Vibration" value={selected.vibrationMmS} unit="mm/s" max={10} warn={SIGNAL_METADATA.vibration.warn} alarm={SIGNAL_METADATA.vibration.alarm} property={SIGNAL_METADATA.vibration.ontologyProperty} />
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
                                            <p className="text-xs text-slate-400">No notes linked to this turbine yet.</p>
                                        ) : (
                                            <ul className="space-y-1 text-xs">
                                                {relatedNotes.map((n) => (
                                                    <li key={n.id ?? `${n.turbineId}-${n.createdAt}`} className="rounded bg-[#0b2a20aa] px-2 py-1">
                                                        <span style={{ color: STATUS_COLORS[n.status as TurbineStatus] ?? "#94a3b8" }}>
                                                            {n.status} · {n.powerKw.toLocaleString()} kW
                                                        </span>
                                                        {n.note && <p className="mt-0.5 truncate text-[11px] text-slate-400" title={n.note}>{n.note}</p>}
                                                    </li>
                                                ))}
                                            </ul>
                                        )}
                                        <button type="button" onClick={() => setView("operations")} className="mt-2 w-full rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-500">Dispatch / writeback →</button>
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
                                                        <span>Temp <span style={{ color: signalColor("temp", t.nacelleTempC) }}>{t.nacelleTempC} C</span></span>
                                                        <span>Vib <span style={{ color: signalColor("vibration", t.vibrationMmS) }}>{t.vibrationMmS} mm/s</span></span>
                                                        <span>Power {t.powerKw.toLocaleString()} kW</span>
                                                    </div>
                                                    <div className="mt-2 flex gap-2">
                                                        <button type="button" onClick={() => { setSelectedId(t.id); setView("twin"); }} className="rounded bg-slate-700 px-2 py-1 text-xs text-white">Inspect</button>
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
                                    <p className="mb-2 text-[11px] text-slate-400">Turbines trending toward thresholds, ranked by anomaly score with a slope-based escalation trend and ETA to alarm.</p>
                                    <ul className="space-y-2">
                                        {anomalyWatch.map((a) => (
                                            <li key={a.t.id}>
                                                <div className="flex justify-between text-xs">
                                                    <button type="button" onClick={() => { setSelectedId(a.t.id); setView("twin"); }} className="text-slate-200 hover:text-cyan-200">{a.t.id} · {a.t.siteName}</button>
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
                                <p className="text-xs uppercase tracking-wide text-slate-400">Ontology relationships · Fleet → Site → Turbine → Signal</p>
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
                                    <div className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full bg-[#58d68d]" /> Turbine</div>
                                    <div className="mt-1 flex items-center gap-1.5"><span className="inline-block h-0 w-3 border-t border-dashed border-slate-400" /> Signal (emits)</div>
                                    <p className="mt-1.5 text-slate-500">healthy · <span className="text-[#ffd166]">warn</span> · <span className="text-[#ef476f]">alarm</span></p>
                                    <p className="mt-1 max-w-[160px] text-[10px] leading-tight text-slate-500">Bands from ontology Sensor thresholds — hover a signal for its property &amp; limits.</p>
                                </div>
                                <div className="pointer-events-none absolute bottom-2 left-3 text-[11px] text-slate-500">Scroll to zoom · drag to pan · click a turbine to drill into its signals</div>
                            </div>
                        </div>
                    )}

                    {view === "analytics" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3">{toolbar}</div>
                            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                                <MetricCard label="Fleet output" value={`${(fleetPower / 1000).toFixed(2)} MW`} sub={`${visibleTurbines.length} turbines online`} />
                                <MetricCard label="Capacity factor" value={`${capacityFactor.toFixed(0)}%`} sub={`of ${fleetRatedMw.toFixed(0)} MW rated`} accent="text-emerald-300" />
                                <MetricCard label="Avg wind" value={`${avgWind.toFixed(1)} m/s`} sub="across visible fleet" />
                                <MetricCard label="Avg nacelle temp" value={`${avgTemp.toFixed(1)} °C`} sub="thermal load" accent="text-amber-300" />
                            </div>

                            <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-3">
                                <Panel title="Output share by site"><DonutChart items={outputBars} unit="MW" /></Panel>
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
                                    title="Power curve · wind vs output"
                                    action={
                                        <div className="flex flex-wrap gap-3 text-[10px]">
                                            <span style={{ color: STATUS_COLORS.healthy }}>● Healthy</span>
                                            <span style={{ color: STATUS_COLORS.warning }}>● Warning</span>
                                            <span style={{ color: STATUS_COLORS.alarm }}>● Alarm</span>
                                        </div>
                                    }
                                >
                                    <ScatterPlot points={powerCurve} xLabel="Wind speed (m/s)" yLabel="Power (kW)" xMax={20} yMax={curvePowerMax} />
                                    <p className="mt-1 text-[10px] leading-tight text-slate-500">
                                        Each dot is a visible turbine. Points hugging the upper-right convert wind to power efficiently; low-output dots at high wind are underperformers worth inspecting.
                                    </p>
                                </Panel>
                            </div>
                        </div>
                    )}

                    {view === "sites" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3">{toolbar}</div>
                            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 2xl:grid-cols-3">
                                {siteSummaries.map((s) => {
                                    const local = turbines.filter((t) => t.siteId === s.id);
                                    return (
                                        <Panel
                                            key={s.id}
                                            title={s.name}
                                            action={
                                                <span className="flex gap-1.5 text-[10px]">
                                                    {s.alarms > 0 && <span className="rounded-full bg-red-600/80 px-1.5 font-semibold text-white">{s.alarms} alarm</span>}
                                                    {s.warnings > 0 && <span className="rounded-full bg-amber-500/80 px-1.5 font-semibold text-black">{s.warnings} warn</span>}
                                                </span>
                                            }
                                        >
                                            <div className="grid grid-cols-3 gap-2">
                                                <MetricCard label="Output" value={`${(s.totalKw / 1000).toFixed(2)} MW`} sub={`${s.turbineCount} turbines`} />
                                                <MetricCard label="Capacity" value={`${s.capacityFactor.toFixed(0)}%`} sub={`of ${s.ratedMw.toFixed(0)} MW`} accent="text-emerald-300" />
                                                <MetricCard label="Avg wind" value={`${s.avgWindMs.toFixed(1)} m/s`} sub={`temp ${s.avgNacelleTempC.toFixed(0)}°C`} accent="text-amber-300" />
                                            </div>
                                            <div className="mt-2">
                                                <HealthBar healthy={s.healthy} warning={s.warnings} alarm={s.alarms} />
                                            </div>
                                            <ul className="mt-2 max-h-44 space-y-1 overflow-y-auto pr-1">
                                                {local.map((t) => (
                                                    <li key={t.id}>
                                                        <button type="button" onClick={() => { setSelectedId(t.id); setView("twin"); }} className="flex w-full items-center justify-between rounded bg-[#0a1830] px-2 py-1 text-left text-xs hover:bg-slate-800">
                                                            <span className="flex items-center gap-2">
                                                                <span className="inline-block h-2 w-2 rounded-full" style={{ background: STATUS_COLORS[t.status] }} />
                                                                <span className="text-slate-200">{t.id}</span>
                                                            </span>
                                                            <span className="text-slate-400">{t.powerKw.toLocaleString()} kW · {t.windMs} m/s</span>
                                                        </button>
                                                    </li>
                                                ))}
                                                {local.length === 0 && <li className="text-xs text-slate-500">No turbines at this site.</li>}
                                            </ul>
                                        </Panel>
                                    );
                                })}
                            </div>
                        </div>
                    )}

                    {view === "operations" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                                <Panel title="Selected turbine">
                                    <p className="text-lg font-semibold">{selected.id}</p>
                                    <p className="text-sm text-slate-400">{selected.siteName}</p>
                                    <p className="text-sm" style={{ color: STATUS_COLORS[selected.status] }}>Status: {selected.status}</p>
                                    <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-sm text-slate-300">
                                        <div className="flex justify-between"><dt>Power</dt><dd>{selected.powerKw.toLocaleString()} kW</dd></div>
                                        <div className="flex justify-between"><dt>Wind</dt><dd>{selected.windMs} m/s</dd></div>
                                        <div className="flex justify-between"><dt>Nacelle</dt><dd>{selected.nacelleTempC} °C</dd></div>
                                        <div className="flex justify-between"><dt>Vibration</dt><dd>{selected.vibrationMmS} mm/s</dd></div>
                                    </dl>

                                    <div className="mt-2">
                                        <div className="mb-1 flex items-center justify-between gap-2">
                                            <p className="text-xs text-slate-400">Power trend (live)</p>
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
                                            <span className="font-semibold text-cyan-200">{forecast.toLocaleString()} kW</span>
                                        </div>
                                        <div className="mt-0.5 flex items-center justify-between text-[11px] text-slate-400">
                                            <span>Range {fc.lo.toLocaleString()}–{fc.hi.toLocaleString()} kW</span>
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
                                            {canWriteback ? " — ontology writeback enabled." : " — writeback is read-only, but dispatch/escalation demo actions auto-switch to Operator."}
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
                                                Setpoint (kW)
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
                                    <p className="mb-2 text-[11px] text-slate-400">Model curtailment and a maintenance window on {selected.id} against its current output baseline.</p>
                                    <div className="grid grid-cols-3 gap-2 text-xs text-slate-400">
                                        <label className="flex flex-col gap-1">
                                            Curtail %
                                            <input type="number" min={0} max={100} value={simCurtail} onChange={(e) => setSimCurtail(Number(e.target.value))} aria-label="Curtailment percent" className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100" />
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
                                        <div className="flex justify-between"><dt className="text-slate-400">Projected output</dt><dd>{scenario.projectedKw.toLocaleString()} kW</dd></div>
                                        <div className="flex justify-between"><dt className="text-slate-400">Running</dt><dd>{scenario.runningTicks}/{simHorizon} t</dd></div>
                                        <div className="col-span-2 flex justify-between border-t border-slate-700/60 pt-1"><dt className="text-slate-400">Energy vs baseline</dt><dd className={scenario.energyDeltaKwt < 0 ? "text-red-300" : "text-emerald-300"}>{scenario.energyDeltaKwt >= 0 ? "+" : ""}{scenario.energyDeltaKwt.toLocaleString()} kW·t</dd></div>
                                    </dl>
                                </Panel>

                                <Panel title="Predictive work order">
                                    <p className="mb-2 text-[11px] text-slate-400">Turn the anomaly forecast and simulated intervention into a tracked work order for {selected.id}.</p>
                                    <div className="grid grid-cols-2 gap-2 text-xs">
                                        <div className="rounded bg-[#0a1830] px-2 py-1.5"><span className="text-slate-400">Suspected</span><div className="text-slate-100">{suggestedComponent}</div></div>
                                        <div className="rounded bg-[#0a1830] px-2 py-1.5"><span className="text-slate-400">Priority</span><div className={suggestedPriority === "P1" ? "text-red-300" : suggestedPriority === "P2" ? "text-amber-300" : "text-emerald-300"}>{suggestedPriority}{selectedForecast.etaToAlarmTicks != null ? ` · ETA ~${selectedForecast.etaToAlarmTicks}t` : ""}</div></div>
                                    </div>
                                    <div className="mt-2 rounded border border-[#2a313b]/70 bg-[#101925] p-2">
                                        <p className="text-[10px] uppercase tracking-wide text-[#b8c1cc]">WorkIQ mock · remediation roster</p>
                                        <p className="mt-1 text-[10px] text-slate-400">
                                            Primary: <span className="text-slate-200">{primaryResponder ? `${primaryResponder.name} (${Math.round(primaryResponder.score)}%)` : "none"}</span>
                                            {nextResponders.length > 0 && (
                                                <span> · Next: <span className="text-slate-300">{nextResponders.map((r) => `${r.name.split(" ")[0]} ${Math.round(r.score)}%`).join(" • ")}</span></span>
                                            )}
                                        </p>
                                        <div className="mt-2 rounded border border-cyan-800/50 bg-[#0a1c2f] p-2">
                                            <p className="text-[10px] uppercase tracking-wide text-cyan-300">Incident storytelling card</p>
                                            {primaryResponder ? (
                                                <div className="mt-1 flex items-start gap-2">
                                                    <img src={primaryResponder.photo} alt={`${primaryResponder.name} portrait`} className="h-12 w-12 rounded-md border border-[#3a4657] object-cover" />
                                                    <div className="min-w-0 flex-1 text-[11px]">
                                                        <p className="font-semibold text-slate-100">{primaryResponder.name}</p>
                                                        <p className="text-slate-400">{primaryResponder.role} · ETA {primaryResponder.etaMin} min · shift {primaryResponder.shift}</p>
                                                        <p className="mt-0.5 text-slate-300">{incidentStory}</p>
                                                    </div>
                                                </div>
                                            ) : (
                                                <p className="mt-1 text-[11px] text-slate-400">No primary responder selected.</p>
                                            )}

                                            <div className="mt-2 rounded border border-[#2a313b]/70 bg-[#101925] px-2 py-1.5 text-[10px]">
                                                <p className="text-[#b8c1cc]">Dispatch quality score: <span className="font-semibold text-slate-100">{dispatchQuality.score}%</span></p>
                                                <div className="mt-1 grid grid-cols-2 gap-x-2 gap-y-0.5 text-slate-400">
                                                    {dispatchQuality.checks.map((check) => (
                                                        <span key={check.label} className={check.ok ? "text-emerald-300" : "text-amber-300"}>
                                                            {check.ok ? "OK" : "WARN"} {check.label}
                                                        </span>
                                                    ))}
                                                </div>
                                                <button
                                                    type="button"
                                                    onClick={handleRunDispatchQualityCheck}
                                                    className="mt-1.5 w-full rounded border border-[#b8c1cc]/60 bg-[#233040] px-2 py-1 text-[11px] font-medium text-[#e7edf5] hover:bg-[#2f3c4d]"
                                                >
                                                    Run Dispatch Quality Check
                                                </button>
                                            </div>

                                            <div className="mt-2 grid grid-cols-2 gap-1.5">
                                                {matchingEvidence.map((ev) => {
                                                    const active = selectedEvidence?.id === ev.id;
                                                    return (
                                                        <button
                                                            key={ev.id}
                                                            type="button"
                                                            onClick={() => setSelectedEvidenceId(ev.id)}
                                                            className={`rounded border p-1 text-left ${active ? "border-cyan-500" : "border-slate-700 hover:border-slate-500"}`}
                                                        >
                                                            <img src={ev.image} alt={ev.label} className="h-14 w-full rounded object-cover" />
                                                            <p className="mt-0.5 truncate text-[10px] text-slate-300" title={ev.label}>{ev.label}</p>
                                                        </button>
                                                    );
                                                })}
                                            </div>
                                            {selectedEvidence && <p className="mt-1 text-[10px] text-slate-500">Selected evidence: {selectedEvidence.label}</p>}
                                        </div>
                                        <div className="mt-1 grid grid-cols-2 gap-1.5 text-[10px]">
                                            <label className="rounded border border-[#2a313b]/70 bg-[#151f2b] px-1.5 py-1 text-slate-300">
                                                Shift
                                                <select
                                                    value={responderShiftFilter}
                                                    onChange={(e) => setResponderShiftFilter(e.target.value as "all" | "day" | "swing" | "night")}
                                                    className="mt-0.5 w-full bg-transparent text-slate-100 outline-none"
                                                >
                                                    <option value="all">All</option>
                                                    <option value="day">Day</option>
                                                    <option value="swing">Swing</option>
                                                    <option value="night">Night</option>
                                                </select>
                                            </label>
                                            <button
                                                type="button"
                                                onClick={() => setOnCallOnly((v) => !v)}
                                                className={`rounded border px-1.5 py-1 text-left ${onCallOnly ? "border-[#b8c1cc]/60 bg-[#232a31] text-[#e7edf5]" : "border-[#2a313b]/70 bg-[#151f2b] text-slate-300"}`}
                                            >
                                                On-call only: {onCallOnly ? "ON" : "OFF"}
                                            </button>
                                        </div>
                                        <div className="mt-1.5 flex flex-wrap items-center gap-1 rounded border border-[#2a313b]/70 bg-[#101925] px-2 py-1 text-[10px]" aria-label="Responder availability board">
                                            <span className="font-semibold uppercase tracking-wide text-cyan-300/90">Roster</span>
                                            <span className="text-slate-300">{responderAvailability.total} in scope</span>
                                            <span className="text-emerald-300">· {responderAvailability.onCall} on-call</span>
                                            <span className="text-sky-300">· {responderAvailability.free} free</span>
                                            <span className="ml-auto text-slate-400">D{responderAvailability.byShift.day} · S{responderAvailability.byShift.swing} · N{responderAvailability.byShift.night}</span>
                                        </div>
                                        {needsEscalation && (
                                            <div className="mt-2 rounded border border-[#d85c57]/55 bg-[#32191d] px-2 py-1.5 text-[11px] text-red-200">
                                                <p>No responder above {escalationThreshold}% match for {suggestedPriority}. Escalate to manager.</p>
                                                <button
                                                    type="button"
                                                    onClick={() => void handleEscalateManager()}
                                                    className="mt-1 w-full rounded bg-[#d85c57] px-2 py-1 text-[11px] font-medium text-white hover:bg-[#e06f6a]"
                                                >
                                                    Escalate to Ops Duty Manager
                                                </button>
                                            </div>
                                        )}
                                        <div className={`mt-2 rounded border px-2 py-1.5 text-[11px] ${isSlaOverdue ? "border-[#d85c57]/55 bg-[#32191d] text-red-200" : "border-[#2a313b]/70 bg-[#151f2b] text-slate-300"}`}>
                                            <div className="flex items-center gap-2.5" role="group" aria-label={`SLA ${orderPriority}: ${slaState.label}, ${Math.round(slaState.fraction * 100)}% of window elapsed`}>
                                                <svg viewBox="0 0 36 36" className="h-11 w-11 shrink-0" aria-hidden="true">
                                                    <circle cx="18" cy="18" r="15.5" fill="none" stroke="#2a313b" strokeWidth="4" />
                                                    <circle
                                                        cx="18"
                                                        cy="18"
                                                        r="15.5"
                                                        fill="none"
                                                        stroke={slaState.color}
                                                        strokeWidth="4"
                                                        strokeLinecap="round"
                                                        strokeDasharray={`${(slaState.fraction * 97.4).toFixed(1)} 97.4`}
                                                        transform="rotate(-90 18 18)"
                                                    />
                                                    <text x="18" y="20.5" textAnchor="middle" fontSize="9" fill="#e7edf5" fontWeight="600">{Math.round(slaState.fraction * 100)}%</text>
                                                </svg>
                                                <div className="min-w-0 flex-1">
                                                    <p className="flex items-center justify-between gap-2">
                                                        <span>SLA {orderPriority}</span>
                                                        <span className="font-semibold" style={{ color: slaState.color }}>{slaState.label}</span>
                                                    </p>
                                                    <p className="mt-0.5 text-[10px] text-slate-400">
                                                        {isSlaOverdue ? `Overdue by ${Math.max(0, (orderAgeMin ?? orderSlaMin) - orderSlaMin)} min` : `${slaRemainingMin} min remaining of ${orderSlaMin} min budget`}
                                                    </p>
                                                </div>
                                            </div>
                                            {isSlaOverdue && canEscalateRegional && (
                                                <button
                                                    type="button"
                                                    onClick={() => void handleEscalateRegional()}
                                                    className="mt-1 w-full rounded bg-[#b94455] px-2 py-1 text-[11px] font-medium text-white hover:bg-[#c65665]"
                                                >
                                                    Escalate to Regional Reliability Lead
                                                </button>
                                            )}
                                            {escalationStage !== "none" && <p className="mt-1 text-[10px] text-amber-200">Escalation stage: {escalationStage.toUpperCase()}</p>}
                                            <ul className="mt-2 space-y-1">
                                                {escalationTimeline.map((entry, i) => {
                                                    const dot = entry.state === "done" ? "#5fa27b" : entry.state === "current" ? slaState.color : "#3a4657";
                                                    return (
                                                        <li key={entry.id} className="flex items-start gap-2">
                                                            <div className="flex flex-col items-center">
                                                                <span className="mt-0.5 h-2.5 w-2.5 rounded-full border border-[#0a1830]" style={{ backgroundColor: dot }} />
                                                                {i < escalationTimeline.length - 1 && <span className="h-3 w-px bg-slate-600" />}
                                                            </div>
                                                            <div className="min-w-0 flex-1 leading-tight">
                                                                <p className={`truncate text-[11px] ${entry.state === "pending" ? "text-slate-500" : "text-slate-200"}`}>{entry.label}</p>
                                                                <p className="text-[9px] uppercase tracking-wide" style={{ color: entry.state === "pending" ? "#64748b" : dot }}>{entry.state} · {entry.note}</p>
                                                            </div>
                                                        </li>
                                                    );
                                                })}
                                            </ul>
                                        </div>
                                        <ul className="mt-2 space-y-1.5">
                                            {suggestedResponders.map((person) => (
                                                <li key={person.id} className="rounded border border-[#2a313b]/70 bg-[#151f2b] px-2 py-1.5">
                                                    <div className="flex items-center gap-2">
                                                        <img src={person.photo} alt={`${person.name} portrait`} className="h-8 w-8 rounded-md border border-[#3a4657] object-cover" />
                                                        <div className="min-w-0 flex-1">
                                                            <p className="truncate text-xs font-medium text-slate-100">{person.name}</p>
                                                            <p className="truncate text-[11px] text-slate-400">{person.role}</p>
                                                        </div>
                                                        <div className="text-right">
                                                            <p className="text-[11px] font-semibold text-[#e7edf5]">{Math.round(person.score)}%</p>
                                                            <p className="text-[10px] text-slate-500">match</p>
                                                        </div>
                                                    </div>
                                                    <p className="mt-1 text-[10px] text-slate-400">{person.reason}</p>
                                                    <p className="mt-0.5 text-[10px] text-slate-500">Shift {person.shift} · On-call {person.onCall ? "yes" : "no"} · Active {person.currentLoad}</p>
                                                    <button
                                                        type="button"
                                                        onClick={() => void handleDispatchResponder(person)}
                                                        className="mt-1.5 w-full rounded bg-[#6f7f93] px-2 py-1 text-[11px] font-medium text-white hover:bg-[#8190a3]"
                                                    >
                                                        {`Dispatch ${person.name.split(" ")[0]}`}
                                                    </button>
                                                </li>
                                            ))}
                                            {suggestedResponders.length === 0 && (
                                                <li className="rounded border border-[#2a313b]/70 bg-[#151f2b] px-2 py-1.5 text-[11px] text-slate-400">
                                                    No responders available for selected filter.
                                                </li>
                                            )}
                                        </ul>
                                    </div>
                                    <p className="mt-2 text-[11px] text-slate-400">Plan (from simulator): curtail {simCurtail}% · downtime {simDowntime}t · projected {scenario.energyDeltaKwt.toLocaleString()} kW·t</p>
                                    <input value={woAssignee} onChange={(e) => setWoAssignee(e.target.value)} placeholder="Assign to… (optional)" aria-label="Assign work order to" className="mt-2 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm text-slate-100" />
                                    <button type="button" onClick={handleRaiseWorkOrder} className="mt-2 w-full rounded bg-cyan-600 px-3 py-2 text-sm font-medium text-white hover:bg-cyan-500">{`Raise ${suggestedPriority} work order`}</button>
                                    {woMessage && <p className="mt-2 text-xs text-cyan-200">{woMessage}</p>}
                                    {maintenanceOrders.length > 0 && (
                                        <ul className="mt-3 space-y-1 text-xs">
                                            {maintenanceOrders.slice(0, 5).map((o) => (
                                                <li key={o.id ?? `${o.turbineId}-${o.createdAt}`} className="rounded bg-[#0b1d38aa] px-2 py-1">
                                                    <div className="flex items-center justify-between">
                                                        <span className="text-slate-200">{o.turbineId}</span>
                                                        <span className={o.priority === "P1" ? "text-red-300" : o.priority === "P2" ? "text-amber-300" : "text-emerald-300"}>{o.priority} · {o.component} · {o.status}</span>
                                                    </div>
                                                </li>
                                            ))}
                                        </ul>
                                    )}
                                </Panel>

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
                                                        <span style={{ color: STATUS_COLORS[n.status as TurbineStatus] ?? "#94a3b8" }}>
                                                            {n.status} · {n.powerKw.toLocaleString()} kW
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
                                            The single source of truth for warn / alarm bands. These drive turbine status, the
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
                            </div>
                        </div>
                    )}

                    {view === "ask" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="ask-wow mx-auto max-w-3xl space-y-3 rounded-2xl border border-cyan-500/20 bg-gradient-to-b from-cyan-500/5 via-transparent to-transparent p-3 shadow-[0_20px_45px_rgba(4,20,40,0.45)]">
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
                                        placeholder="Ask about output, alarms, vibration, wind, dispatch notes…  (Ctrl+Enter to send)"
                                        className="mt-2 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1.5 text-sm shadow-inner shadow-black/30"
                                    />
                                    <div className="mt-2 flex flex-wrap gap-1">
                                        {SUGGESTED.map((q) => (
                                            <button
                                                key={q}
                                                type="button"
                                                onClick={() => { setQuestion(q); void runAsk(q); }}
                                                className="rounded-full border border-slate-700 bg-[#0a1830] px-2.5 py-1 text-[11px] text-slate-300 transition-all duration-200 hover:-translate-y-[1px] hover:border-cyan-500 hover:text-cyan-200"
                                            >
                                                {q}
                                            </button>
                                        ))}
                                    </div>
                                    <div className="mt-2 flex gap-2">
                                        <button
                                            type="button"
                                            onClick={() => void runAsk()}
                                            disabled={askLoading}
                                            className="flex-1 rounded bg-cyan-600 px-3 py-2 text-sm font-medium text-white shadow-[0_10px_28px_rgba(6,182,212,0.45)] transition-all duration-200 hover:-translate-y-[1px] hover:bg-cyan-500 disabled:opacity-50"
                                        >
                                            {askLoading ? "Asking…" : "Ask Question"}
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => void runAgentConnectionCheck()}
                                            disabled={agentCheckLoading}
                                            className="rounded border border-slate-600 bg-[#0a1830] px-3 py-2 text-xs font-medium text-slate-200 transition-all duration-200 hover:-translate-y-[1px] hover:border-cyan-500 hover:text-cyan-200 disabled:opacity-50"
                                        >
                                            {agentCheckLoading ? "Testing…" : "Test Data Agent"}
                                        </button>
                                    </div>
                                    {askError && <p className="mt-2 text-xs text-red-300">{askError}</p>}
                                    {agentCheckResult && (
                                        <div className={`mt-2 rounded border p-2 text-xs ${agentCheckResult.ok ? "border-emerald-700/70 bg-emerald-900/20 text-emerald-200" : "border-red-800/70 bg-red-900/20 text-red-200"}`}>
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
                                <p className="text-[11px] uppercase tracking-wide text-cyan-300">Turbine detail</p>
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
                            <div className="flex justify-between"><dt className="text-slate-400">Power</dt><dd>{selected.powerKw.toLocaleString()} kW</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Wind</dt><dd>{selected.windMs} m/s</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Nacelle temp</dt><dd>{selected.nacelleTempC} °C</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Vibration</dt><dd>{selected.vibrationMmS} mm/s</dd></div>
                        </dl>

                        <div className="mt-3">
                            <div className="mb-1 flex items-center justify-between gap-2">
                                <p className="text-xs text-slate-400">Power trend (live)</p>
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
                            <span className="font-semibold text-cyan-200">{forecast.toLocaleString()} kW</span>
                        </div>

                        <div className="mt-4 flex gap-2">
                            <button type="button" onClick={() => { setView("operations"); setDetailOpen(false); }} className="flex-1 rounded bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500">Open in Operations</button>
                            <button type="button" onClick={() => { setView("analytics"); setDetailOpen(false); }} className="flex-1 rounded bg-slate-700 px-3 py-2 text-sm font-medium text-white hover:bg-slate-600">View analytics</button>
                        </div>
                    </div>
                </div>
            )}

            {techPopupOpen && techPopupResponder && (
                <div className="fixed inset-0 z-[200] flex items-center justify-center bg-black/70 p-4" onClick={() => setTechPopupOpen(false)}>
                    <div className="w-[min(92vw,860px)] max-h-[86vh] overflow-y-auto rounded-xl border border-cyan-700/50 bg-[#07142a] p-4 shadow-[0_24px_70px_rgba(0,0,0,0.65)]" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-start justify-between gap-3">
                            <div className="flex items-start gap-3">
                                <img src={techPopupResponder.photo} alt={`${techPopupResponder.name} portrait`} className="h-16 w-16 rounded-lg border border-[#3a4657] object-cover" />
                                <div>
                                    <p className="text-[11px] uppercase tracking-wide text-cyan-300">Technician Profile</p>
                                    <h3 className="text-lg font-semibold text-slate-100">{techPopupResponder.name}</h3>
                                    <p className="text-xs text-slate-400">{techPopupResponder.role} · ETA {techPopupResponder.etaMin} min · shift {techPopupResponder.shift}</p>
                                    <p className="mt-1 text-xs text-slate-300">{incidentStory}</p>
                                </div>
                            </div>
                            <button type="button" onClick={() => setTechPopupOpen(false)} className="rounded p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100">✕</button>
                        </div>

                        <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                            <div className="rounded bg-[#0a1830] px-2 py-1.5"><span className="text-slate-400">Match score</span><div className="font-semibold text-cyan-200">{Math.round(techPopupResponder.score)}%</div></div>
                            <div className="rounded bg-[#0a1830] px-2 py-1.5"><span className="text-slate-400">On-call</span><div className="font-semibold text-slate-100">{techPopupResponder.onCall ? "Yes" : "No"}</div></div>
                        </div>

                        <div className="mt-2 flex flex-wrap gap-1">
                            <span className="rounded bg-cyan-900/40 px-2 py-0.5 text-[11px] text-cyan-200">{techPopupResponder.shift} shift</span>
                            <span className={`rounded px-2 py-0.5 text-[11px] ${techPopupResponder.onCall ? "bg-emerald-900/40 text-emerald-200" : "bg-slate-700/70 text-slate-300"}`}>{techPopupResponder.onCall ? "On-call" : "Standby"}</span>
                            <span className="rounded bg-amber-900/40 px-2 py-0.5 text-[11px] text-amber-200">{techPopupFocusComponent} focus</span>
                            {techPopupResponder.skills.map((skill) => (
                                <span key={`${techPopupResponder.id}-${skill}`} className="rounded bg-slate-700/70 px-2 py-0.5 text-[11px] text-slate-200">{skill}</span>
                            ))}
                        </div>

                        <div className="mt-3">
                            <p className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">Evidence</p>
                            <div className="grid grid-cols-2 gap-2">
                                {matchingEvidence.map((ev) => {
                                    const active = techPopupEvidence?.id === ev.id || selectedEvidence?.id === ev.id;
                                    return (
                                        <button
                                            key={ev.id}
                                            type="button"
                                            onClick={() => setSelectedEvidenceId(ev.id)}
                                            className={`rounded border p-1 text-left ${active ? "border-cyan-500 bg-cyan-500/10" : "border-slate-700 hover:border-slate-500"}`}
                                        >
                                            <img src={ev.image} alt={ev.label} className="h-16 w-full rounded object-cover" />
                                            <p className="mt-0.5 truncate text-[10px] text-slate-300" title={ev.label}>{ev.label}</p>
                                            <p className="text-[9px] uppercase tracking-wide text-slate-500">{ev.component} asset</p>
                                        </button>
                                    );
                                })}
                            </div>
                        </div>

                        <div className="mt-3 flex gap-2">
                            <button type="button" onClick={() => {
                                setView("operations");
                                setTechPopupOpen(false);
                                setSelectedId(selected.id);
                                void handleDispatchResponder(techPopupResponder);
                            }} className="flex-1 rounded bg-cyan-600 px-3 py-2 text-sm font-medium text-white hover:bg-cyan-500">
                                Dispatch now
                            </button>
                            <button type="button" onClick={() => {
                                setView("operations");
                                setTechPopupOpen(false);
                                void handleAutoHealNow();
                            }} className="flex-1 rounded border border-emerald-700/70 bg-emerald-900/20 px-3 py-2 text-sm font-medium text-emerald-200 hover:bg-emerald-900/35">
                                AutoHeal
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </main>
    );
}

export default App;
