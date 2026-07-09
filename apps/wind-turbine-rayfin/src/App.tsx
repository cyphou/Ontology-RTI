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
    ensureOntologySites,
    ensureSignalThresholds,
    listSignalThresholds,
    recentDispatchNotes,
    saveDispatchNote,
    type DispatchNoteRecord,
    type SignalThresholdRecord,
} from "@/services/ontology-data.service";
import { isDataAgentConfigured, queryDataAgent } from "@/services/data-agent.service";
import {
    fetchLiveTelemetry,
    fetchPowerHistory,
    isLiveTelemetryConfigured,
    type LiveTelemetrySnapshot,
} from "@/services/live-telemetry.service";
import { classifyAskIntent, normalizeAskQuestion } from "@/services/ask-routing.service";
import { canManageDispatch, normalizeOperatorRole, type OperatorRole } from "@/services/operator-role.service";
import { HISTORY_WINDOWS, historyPointLimit, type HistoryWindow } from "@/services/history-window.service";
import { SceneErrorBoundary } from "@/components/SceneErrorBoundary";

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

type AskResult = {
    source: "fabriciq" | "ontology" | "local";
    summary: string;
    generatedAt: string;
    queryText?: string;
    confidence?: number;
    evidence?: string[];
    cacheHit?: boolean;
};

const ASK_CACHE_TTL_MS = 30_000;

type TurbineRenderRefs = {
    blades: THREE.Group;
    nacelleMat: THREE.MeshStandardMaterial;
    ringMat: THREE.MeshBasicMaterial;
    ring: THREE.Mesh;
    spin: number;
};

type SceneState = {
    byId: Map<string, TurbineRenderRefs>;
    cleanup: () => void;
};

export const STATUS_COLORS: Record<TurbineStatus, string> = {
    healthy: "#58d68d",
    warning: "#ffd166",
    alarm: "#ef476f",
};

export const SITE_COLORS = ["#6ee7ff", "#9bffb0", "#fbc2eb", "#ffd166", "#b4d4ff", "#a1c4fd"];

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
    ctx.shadowBlur = 26;
    ctx.fillStyle = "rgba(70, 137, 95, 1)";
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.fill();
    });
    ctx.restore();

    // Pass 2 - crisp land with subtle relief gradient + coastline stroke.
    const landGrad = ctx.createLinearGradient(0, 0, 0, h);
    landGrad.addColorStop(0, "#56a06f");
    landGrad.addColorStop(0.5, "#46895f");
    landGrad.addColorStop(1, "#37774f");
    WORLD.forEach((poly) => {
        tracePoly(poly);
        ctx.fillStyle = landGrad;
        ctx.fill();
        ctx.strokeStyle = "rgba(205, 255, 230, 0.85)";
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

        ctx.fillStyle = "#ecfeff";
        ctx.font = "20px sans-serif";
        ctx.fillText(site.country, x + 14, y - 12);
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

    // Fast operational intents are answered deterministically to minimize latency,
    // avoid unnecessary remote calls, and keep triage behavior stable.
    if (intent === "ops-fastpath") {
        return {
            source: "local",
            summary: answerQuestion(question, telemetry),
            generatedAt: new Date().toISOString(),
            queryText: "Deterministic local rule engine",
            confidence: 0.98,
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
            };
        } catch {
            /* fall through to ontology-grounded reasoning */
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
            evidence: ["routing: ontology-grounded"],
        };
    } catch {
        return {
            source: "local",
            summary: `${answerQuestion(question, telemetry)} (offline — ontology backend unreachable)`,
            generatedAt: new Date().toISOString(),
            queryText: "Deterministic local fallback",
            confidence: 0.72,
            evidence: ["ontology backend unreachable", "routing: local fallback"],
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

    useEffect(() => {
        pausedRef.current = paused;
    }, [paused]);

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

        sites.forEach((site, idx) => {
            const marker = new THREE.Mesh(
                new THREE.CylinderGeometry(0.24, 0.24, 0.9, 14),
                new THREE.MeshStandardMaterial({
                    color: SITE_COLORS[idx % SITE_COLORS.length],
                    emissive: SITE_COLORS[idx % SITE_COLORS.length],
                    emissiveIntensity: 0.32,
                })
            );
            marker.position.set(projectLonToX(site.lon), 0.45, projectLatToZ(site.lat));
            scene.add(marker);

            const glow = new THREE.Mesh(
                new THREE.RingGeometry(0.5, 0.95, 24),
                new THREE.MeshBasicMaterial({ color: SITE_COLORS[idx % SITE_COLORS.length], transparent: true, opacity: 0.55 })
            );
            glow.rotation.x = -Math.PI / 2;
            glow.position.set(projectLonToX(site.lon), 0.07, projectLatToZ(site.lat));
            scene.add(glow);
        });

        const byId = new Map<string, TurbineRenderRefs>();
        const pickables: THREE.Mesh[] = [];
        const raycaster = new THREE.Raycaster();
        const pointer = new THREE.Vector2();

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
            camera.position.x = (26 + Math.sin(tick * 0.17) * 9) * z;
            camera.position.y = 36 * z;
            camera.position.z = (61 + Math.cos(tick * 0.13) * 5) * z;
            camera.lookAt(0, 0, 0);
            renderer.render(scene, camera);
        };
        renderer.setAnimationLoop(animate);

        const cleanup = () => {
            renderer.setAnimationLoop(null);
            renderer.domElement.removeEventListener("click", onClick);
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
    }, [selectedId, turbines, dimmedIds]);

    return (
        <div className="relative h-full w-full">
            <div ref={hostRef} className="h-full w-full" />
            <div className="absolute bottom-3 right-3 flex flex-col overflow-hidden rounded-lg border border-slate-700/70 bg-[#06101fcc] text-slate-100 backdrop-blur">
                <button type="button" title="Zoom in" onClick={() => { zoomRef.current = THREE.MathUtils.clamp(zoomRef.current - 0.15, 0.4, 1.8); }} className="px-2.5 py-1.5 text-sm hover:bg-slate-700/60">＋</button>
                <button type="button" title="Zoom out" onClick={() => { zoomRef.current = THREE.MathUtils.clamp(zoomRef.current + 0.15, 0.4, 1.8); }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-sm hover:bg-slate-700/60">－</button>
                <button type="button" title="Reset zoom" onClick={() => { zoomRef.current = 0.62; }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60">⟳</button>
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

// Ids of turbines that transitioned into "alarm" since the previous status snapshot.
export function newlyAlarmed(prev: Record<string, TurbineStatus>, turbines: { id: string; status: TurbineStatus }[]): string[] {
    return turbines.filter((t) => t.status === "alarm" && prev[t.id] !== "alarm").map((t) => t.id);
}

type ViewKey = "map" | "twin" | "sites" | "alerts" | "graph" | "analytics" | "operations" | "ask";

const NAV: { key: ViewKey; label: string; icon: string }[] = [
    { key: "map", label: "Map", icon: "🗺" },
    { key: "twin", label: "Digital Twin", icon: "🌀" },
    { key: "sites", label: "Sites", icon: "🏢" },
    { key: "alerts", label: "Alerts", icon: "🚨" },
    { key: "graph", label: "Graph", icon: "🕸" },
    { key: "analytics", label: "Analytics", icon: "📊" },
    { key: "operations", label: "Operations", icon: "🛠" },
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
    "Which site has the highest output now?",
    "Any turbines in alarm?",
    "Which turbine has the most vibration?",
    "What is the hottest nacelle?",
    "Peak wind right now?",
    "How many turbines total?",
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
                            <span aria-hidden="true" className="ml-auto rounded-full bg-rose-600 px-1.5 text-[10px] font-semibold leading-4 text-white">{badge}</span>
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
    const [selectedId, setSelectedId] = useState(initialRoute.selectedId ?? "SITE-TX-WT-01");
    const [view, setView] = useState<ViewKey>(initialRoute.view ?? "map");
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

    const [powerHistory, setPowerHistory] = useState<number[]>([]);
    const historyKeyRef = useRef(selectedId);

    const [notes, setNotes] = useState<DispatchNoteRecord[]>([]);
    const [notesLoading, setNotesLoading] = useState(false);
    const [thresholdsPublished, setThresholdsPublished] = useState<number | null>(null);
    const [thresholdNonce, setThresholdNonce] = useState(0);

    const [forecastHorizon, setForecastHorizon] = useState(5);
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

    const runAsk = useCallback(async (override?: string) => {
        setAskLoading(true);
        setAskError(null);
        try {
            const trimmed = (override ?? question).trim();
            if (!trimmed) {
                throw new Error("Please enter a question.");
            }

            const alarmsNow = turbines.filter((t) => t.status === "alarm").length;
            const warningsNow = turbines.filter((t) => t.status === "warning").length;
            const fleetKwNow = turbines.reduce((sum, t) => sum + t.powerKw, 0);
            const signature = `${turbines.length}:${alarmsNow}:${warningsNow}:${Math.round(fleetKwNow / 50)}:${selected.id}`;
            const cacheKey = `${normalizeAskQuestion(trimmed)}|${signature}`;
            const now = Date.now();
            const cached = askCacheRef.current.get(cacheKey);
            if (cached && now - cached.at <= ASK_CACHE_TTL_MS) {
                setAskResult({ ...cached.result, cacheHit: true });
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
        } catch (err) {
            setAskError(err instanceof Error ? err.message : String(err));
        } finally {
            setAskLoading(false);
        }
    }, [question, selected.id, selected.siteName, turbines]);

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
                    className="rounded bg-rose-700/70 px-2 py-1 text-white"
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
                    <h1 className="text-xl font-semibold leading-tight">Geo Wind Twin Command Center</h1>
                    <p className="mt-0.5 text-[10px] text-amber-300/80" title="Live fleet telemetry is simulated in-browser; ontology sites and dispatch notes persist to the Fabric Rayfin backend.">
                        ◐ Simulated telemetry · ontology + dispatch notes persisted to Fabric
                    </p>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                    <KpiPill label="Fleet" value={`${(fleetPower / 1000).toFixed(2)} MW`} color="#6ee7ff" />
                    <KpiPill label="Healthy" value={String(healthy)} color={STATUS_COLORS.healthy} />
                    <KpiPill label="Warning" value={String(warnings)} color={STATUS_COLORS.warning} />
                    <KpiPill label="Alarm" value={String(alarms)} color={STATUS_COLORS.alarm} />
                    <KpiPill label="Turbines" value={`${visibleTurbines.length}/${turbines.length}`} />
                </div>

                <div className="ml-auto flex w-full flex-wrap items-center justify-end gap-2 text-xs text-slate-300 sm:w-auto">
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
                        <div className="text-slate-400">pde_windturbine</div>
                        <div>{new Date().toLocaleTimeString()}</div>
                    </div>
                </div>
            </header>

            <div className="flex min-h-0 flex-1 flex-col md:flex-row">
                <NavRail view={view} onChange={setView} badges={{ alerts: unackedAlerts.length }} />

                <section className="relative min-h-0 flex-1">
                    {view === "map" && (
                        <div className="relative h-full min-h-[420px] md:min-h-[520px]">
                            <SceneErrorBoundary label="Fleet map">
                                <Suspense fallback={<div className="h-full w-full animate-pulse bg-[#051020]" />}>
                                    <LazyWindFarmScene turbines={turbines} sites={sites} selectedId={selected.id} dimmedIds={dimmedIds} paused={!live} onSelect={openTurbine} />
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
                                    {selected.status} · {selected.powerKw.toLocaleString()} kW · {selected.windMs} m/s
                                </p>
                                <div className="mt-2"><Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} /></div>
                                <div className="mt-2 flex gap-1">
                                    <button type="button" onClick={() => setDetailOpen(true)} className="flex-1 rounded bg-cyan-600 px-2 py-1 text-xs font-medium text-white">Details</button>
                                    <button type="button" onClick={() => setView("operations")} className="flex-1 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white">Dispatch</button>
                                </div>
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
                                                <LazyTurbineTwinScene turbine={selected} paused={!live} />
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
                                    <p className="mt-2 text-[11px] text-slate-500">Auto-logged {autoLogCount} alarm onset{autoLogCount === 1 ? "" : "s"} to dispatch notes this session.</p>
                                </Panel>

                                <Panel title="Anomaly watch (predictive)">
                                    <p className="mb-2 text-[11px] text-slate-400">Turbines trending toward thresholds, ranked by anomaly score with a slope-based escalation trend and ETA to alarm.</p>
                                    <ul className="space-y-2">
                                        {anomalyWatch.map((a) => (
                                            <li key={a.t.id}>
                                                <div className="flex justify-between text-xs">
                                                    <button type="button" onClick={() => { setSelectedId(a.t.id); setView("twin"); }} className="text-slate-200 hover:text-cyan-200">{a.t.id} · {a.t.siteName}</button>
                                                    <span className="flex items-center gap-2">
                                                        <span className={a.forecast.direction === "rising" ? "text-rose-300" : a.forecast.direction === "falling" ? "text-emerald-300" : "text-slate-500"}>
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
                                                    {s.alarms > 0 && <span className="rounded-full bg-rose-600/80 px-1.5 font-semibold text-white">{s.alarms} alarm</span>}
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
                                        placeholder="Ask about output, alarms, vibration, wind, dispatch notes…  (Ctrl+Enter to send)"
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
                                    {askError && <p className="mt-2 text-xs text-rose-300">{askError}</p>}
                                    {askResult && (
                                        <div className="mt-3 rounded border border-slate-700 bg-[#081226] p-3 text-sm text-slate-200">
                                            <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-400">
                                                <span>Source: {sourceLabel(askResult.source)}</span>
                                                <span className="flex items-center gap-2">
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
        </main>
    );
}

export default App;
