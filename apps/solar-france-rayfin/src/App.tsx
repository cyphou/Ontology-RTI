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
    fetchAnomalyScores,
    fetchPowerHistory,
    isLiveTelemetryConfigured,
    type LiveTelemetrySnapshot,
} from "@/services/live-telemetry.service";
import { classifyAskIntent, normalizeAskQuestion } from "@/services/ask-routing.service";
import { canManageDispatch, normalizeOperatorRole, type OperatorRole } from "@/services/operator-role.service";
import { HISTORY_WINDOWS, historyPointLimit, type HistoryWindow } from "@/services/history-window.service";
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

// Major French photovoltaic plants ("centrales solaires"), positioned by their real
// coordinates across French regions. capacityMw is the peak DC rating (MWc); arrayCount
// is the number of PV array blocks rendered for the plant.
const SITES: SolarPlantSite[] = [
    { id: "CESTAS", name: "Cestas", region: "Nouvelle-Aquitaine", lat: 44.74, lon: -0.68, arrayCount: 8, capacityMw: 300 },
    { id: "MARVILLE", name: "Marville", region: "Grand Est", lat: 49.45, lon: 5.42, arrayCount: 7, capacityMw: 152 },
    { id: "TOUL", name: "Toul-Rosi\u00e8res", region: "Grand Est", lat: 48.78, lon: 5.92, arrayCount: 6, capacityMw: 115 },
    { id: "MEES", name: "Les M\u00e9es", region: "Provence-Alpes-C\u00f4te d'Azur", lat: 44.02, lon: 6.00, arrayCount: 6, capacityMw: 100 },
    { id: "GABARDAN", name: "Gabardan", region: "Nouvelle-Aquitaine", lat: 44.02, lon: -0.08, arrayCount: 5, capacityMw: 76 },
    { id: "MASSANGIS", name: "Massangis", region: "Bourgogne-Franche-Comt\u00e9", lat: 47.62, lon: 4.02, arrayCount: 5, capacityMw: 56 },
];

export function seededRand(seed: number) {
    const s = Math.sin(seed) * 10000;
    return s - Math.floor(s);
}

// Metropolitan France ("l'Hexagone") bounding box. Plant coordinates are projected into
// the 92 x 52 world plane through this box so each centrale lands on its real position
// on the map of France.
const FRANCE_BBOX = { lonMin: -5.2, lonMax: 9.6, latMin: 41.2, latMax: 51.3 };

export function projectLonToX(lon: number) {
    return ((lon - FRANCE_BBOX.lonMin) / (FRANCE_BBOX.lonMax - FRANCE_BBOX.lonMin) - 0.5) * 52;
}

export function projectLatToZ(lat: number) {
    return ((FRANCE_BBOX.latMax - lat) / (FRANCE_BBOX.latMax - FRANCE_BBOX.latMin) - 0.5) * 52;
}

// Simplified outline of metropolitan France ("l'Hexagone") plus Corsica, in [lon, lat].
// Traced so the landmass reads as a recognizable map of France behind the plants.
const WORLD: number[][][] = [
    // Mainland France (l'Hexagone)
    [
        [2.05, 51.05], [4.85, 50.0], [5.9, 49.5], [8.2, 48.9], [7.6, 47.6],
        [7.0, 47.45], [6.85, 46.9], [6.1, 46.25], [6.8, 45.95], [7.05, 45.5],
        [6.65, 45.05], [7.55, 44.1], [7.5, 43.75], [6.0, 43.1], [4.85, 43.35],
        [3.05, 43.05], [3.05, 42.45], [1.9, 42.35], [0.65, 42.7], [-1.4, 43.3],
        [-1.55, 44.25], [-1.25, 45.7], [-1.05, 46.3], [-2.1, 47.0], [-2.55, 47.5],
        [-4.25, 47.8], [-4.78, 48.4], [-3.6, 48.7], [-1.95, 48.65], [-1.55, 49.65],
        [-0.2, 49.3], [0.2, 49.7], [1.6, 50.35], [2.05, 51.05],
    ],
    // Corsica
    [
        [8.6, 42.95], [9.35, 42.7], [9.55, 41.9], [9.25, 41.38], [8.8, 41.55],
        [8.65, 42.25], [8.55, 42.6],
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

// Single source of truth for signal bands, mirrored from the SolarPlant ontology Sensor
// metadata (Unit, MinThreshold/MaxThreshold) and the Data Agent threshold guidance
// (e.g. module temperature: normal <65, warning 65–80, critical >80 °C). Health is
// governed only by condition signals (module temp, inverter load); power and irradiance
// are informational context, so they never drive a plant's alarm status.
const SIGNAL_METADATA: Record<SignalKey, SignalMetadata> = {
    power: { key: "power", label: "Power", ontologyProperty: "AcPowerKW", unit: "kW", warn: Infinity, alarm: Infinity, governsHealth: false, get: (t) => t.powerKw },
    irradiance: { key: "irradiance", label: "Irradiance", ontologyProperty: "IrradianceWM2", unit: "W/m\u00b2", warn: Infinity, alarm: Infinity, governsHealth: false, get: (t) => t.irradianceWm2 },
    moduleTemp: { key: "moduleTemp", label: "Module", ontologyProperty: "ModuleTempC", unit: "\u00b0C", warn: 65, alarm: 80, governsHealth: true, get: (t) => t.moduleTempC },
    inverterLoad: { key: "inverterLoad", label: "Inverter", ontologyProperty: "InverterLoadPct", unit: "%", warn: 90, alarm: 98, governsHealth: true, get: (t) => t.inverterLoadPct },
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
    plantCount: number;
    totalKw: number;
    ratedMw: number;
    capacityFactor: number;
    alarms: number;
    warnings: number;
    healthy: number;
    avgIrradianceWm2: number;
    avgModuleTempC: number;
    avgInverterLoadPct: number;
}

// Pure per-site aggregation for the site drill-down view: rolls each site's
// PV plants into scoped KPIs (output, capacity factor, health counts, averages).
// Sites with no plants report zeros so the UI can render them safely.
export function summarizeSites(
    turbines: { siteId: string; status: PlantStatus; powerKw: number; irradianceWm2: number; moduleTempC: number; inverterLoadPct: number }[],
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
            plantCount: n,
            totalKw,
            ratedMw,
            capacityFactor: ratedMw > 0 ? (totalKw / 1000 / ratedMw) * 100 : 0,
            alarms,
            warnings,
            healthy: n - alarms - warnings,
            avgIrradianceWm2: +avg((t) => t.irradianceWm2).toFixed(0),
            avgModuleTempC: +avg((t) => t.moduleTempC).toFixed(1),
            avgInverterLoadPct: +avg((t) => t.inverterLoadPct).toFixed(0),
        };
    });
}

// Anchor points (in twin-scene world space) for the part value callouts.
export const TWIN_PARTS: { key: string; caption: string; pos: [number, number, number] }[] = [
    { key: "array", caption: "Array", pos: [0, 4.0, -1.4] },
    { key: "inverter", caption: "Inverter", pos: [-3.6, 1.5, 0] },
    { key: "tracker", caption: "Tracker", pos: [2.8, 0.9, 1.4] },
    { key: "output", caption: "Output", pos: [0, 0.7, 2.8] },
];

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
    // Map texture spans the 92 x 52 world plane; project lon/lat through the France
    // bounding box so each centrale lands on its real position over l'Hexagone.
    const toPx = (lon: number, lat: number): [number, number] => {
        const u = (projectLonToX(lon) + 46) / 92;
        const v = (projectLatToZ(lat) + 26) / 52;
        return [u * w, v * h];
    };

    // Transparent overlay: the lit ocean plane shows through; we only paint land.
    ctx.strokeStyle = "rgba(150, 205, 255, 0.10)";
    ctx.lineWidth = 1;
    for (let lon = -6; lon <= 11; lon += 2) {
        const [x] = toPx(lon, FRANCE_BBOX.latMax);
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
        ctx.stroke();
    }
    for (let lat = 41; lat <= 52; lat += 2) {
        const [, y] = toPx(FRANCE_BBOX.lonMin, lat);
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
        const [x, y] = toPx(site.lon, site.lat);
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
        ctx.fillText(site.name, x + 14, y - 12);
    });

    const texture = new THREE.CanvasTexture(canvas);
    texture.anisotropy = 4;
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
        const ratedPerArrayKw = (site.capacityMw * 1000) / Math.max(site.arrayCount, 1);

        for (let i = 0; i < site.arrayCount; i += 1) {
            const idx = siteIdx * 100 + i;
            const col = i % cols;
            const row = Math.floor(i / cols);
            const x = cx + (col - (cols - 1) / 2) * 1.5;
            const z = cz + (row - (totalRows - 1) / 2) * 1.5;

            const id = `${site.id}-PV-${String(i + 1).padStart(2, "0")}`;
            const irradianceWm2 = 250 + seededRand(idx + 3 + seedOffset) * 700;
            const yieldFactor = 0.78 + seededRand(idx + 7 + seedOffset) * 0.16;
            const powerKw = Math.round(ratedPerArrayKw * (irradianceWm2 / 1000) * yieldFactor);
            const moduleTempC = 18 + (irradianceWm2 / 1000) * 45 + seededRand(idx + 11 + seedOffset) * 20;
            const inverterLoadPct = Math.min(100, (irradianceWm2 / 1000) * 78 + seededRand(idx + 15 + seedOffset) * 30);

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
// renders, reusing each plant's real coordinates and the shared status bands so
// real data flows through the identical rendering path as buildFarm().
function assembleLiveView(snapshot: LiveTelemetrySnapshot): { turbines: PlantTelemetry[]; sites: SolarPlantSite[] } {
    const metricsById = new Map(snapshot.metrics.map((m) => [m.arrayId, m]));
    const plantById = new Map(snapshot.plants.map((p) => [p.plantId, p]));
    const arraysByPlant = new Map<string, string[]>();
    for (const a of snapshot.arrays) {
        const list = arraysByPlant.get(a.plantId) ?? [];
        list.push(a.arrayId);
        arraysByPlant.set(a.plantId, list);
    }

    const rows: PlantTelemetry[] = [];
    for (const plant of snapshot.plants) {
        const ids = arraysByPlant.get(plant.plantId) ?? [];
        const cx = projectLonToX(plant.longitude);
        const cz = projectLatToZ(plant.latitude);
        const cols = Math.max(1, Math.ceil(Math.sqrt(ids.length)));
        const totalRows = Math.max(1, Math.ceil(ids.length / cols));
        ids.forEach((arrayId, i) => {
            const col = i % cols;
            const row = Math.floor(i / cols);
            const x = cx + (col - (cols - 1) / 2) * 1.5;
            const z = cz + (row - (totalRows - 1) / 2) * 1.5;
            const m = metricsById.get(arrayId);
            const moduleTempC = +(m?.moduleTempC ?? 0).toFixed(1);
            const inverterLoadPct = +(m?.inverterLoadPct ?? 0).toFixed(0);
            rows.push({
                id: arrayId,
                siteId: plant.plantId,
                siteName: plant.plantName,
                latitude: plant.latitude + (row - (totalRows - 1) / 2) * 0.012,
                longitude: plant.longitude + (col - (cols - 1) / 2) * 0.014,
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

    const sites: SolarPlantSite[] = snapshot.plants.map((plant) => ({
        id: plant.plantId,
        name: plant.plantName,
        region: "",
        lat: plant.latitude,
        lon: plant.longitude,
        arrayCount: (arraysByPlant.get(plant.plantId) ?? []).length,
        capacityMw: plant.capacityMwc,
    }));

    // Ignore arrays whose plant has no coordinates so they cannot collapse to origin.
    return {
        turbines: rows.filter((p) => plantById.has(p.siteId)),
        sites,
    };
}

// React hook: when a live semantic-model connection is configured, poll it and
// return real arrays + sites; otherwise return null so callers fall back to the
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
            if (cancelled || !data || data.arrays.length === 0) {
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

const LazySolarFleetScene = lazyRetry(() => import("@/scenes/SolarFleetScene"));
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

// Predictive anomaly score (0-1) from module temperature and inverter load approaching limits.
export function anomalyScore(t: PlantTelemetry): number {
    const tempScore = (t.moduleTempC - 60) / 20;
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

// Ids of arrays that transitioned into "alarm" since the previous status snapshot.
export function newlyAlarmed(prev: Record<string, PlantStatus>, turbines: { id: string; status: PlantStatus }[]): string[] {
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
    "Which plant has the highest output now?",
    "Any arrays in alarm?",
    "Which array has the highest inverter load?",
    "What is the hottest module temperature?",
    "Peak irradiance right now?",
    "How many PV arrays total?",
];

type SolarRepairSkill = "Inverter" | "PV Module" | "String Combiner" | "Tracker Motor";
type SolarRepairPriority = "P1" | "P2" | "P3";

type MockSolarTechnician = {
    id: string;
    name: string;
    role: string;
    skills: SolarRepairSkill[];
    certifications: string[];
    siteCoverage: string[];
    availability: "available" | "busy" | "off";
    etaMin: number;
    photo: string;
};

function svgTechAvatar(name: string, seed: number): string {
    const initials = name
        .split(" ")
        .map((chunk) => chunk[0] ?? "")
        .join("")
        .slice(0, 2)
        .toUpperCase();
    const hueA = 204 + (seed * 17) % 24;
    const hueB = 214 + (seed * 13) % 28;
    const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 80 80'>
<defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'><stop offset='0%' stop-color='hsl(${hueA} 36% 42%)'/><stop offset='100%' stop-color='hsl(${hueB} 28% 24%)'/></linearGradient></defs>
<rect width='80' height='80' rx='14' fill='url(#g)'/>
<circle cx='40' cy='32' r='15' fill='rgba(255,255,255,0.2)'/>
<path d='M14 74c3-14 12-21 26-21s23 7 26 21' fill='rgba(255,255,255,0.16)'/>
<text x='40' y='50' text-anchor='middle' font-size='22' font-weight='700' fill='#e7edf5' font-family='Segoe UI, sans-serif'>${initials}</text>
</svg>`;
    return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}

function svgMockEvidenceImage(label: string, seed: number): string {
    const hueA = 198 + (seed * 21) % 36;
    const hueB = 218 + (seed * 11) % 40;
    const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 220 132'>
<defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'><stop offset='0%' stop-color='hsl(${hueA} 34% 38%)'/><stop offset='100%' stop-color='hsl(${hueB} 30% 20%)'/></linearGradient></defs>
<rect width='220' height='132' rx='14' fill='url(#g)'/>
<rect x='14' y='12' width='192' height='80' rx='8' fill='rgba(255,255,255,0.08)'/>
<path d='M28 88l28-26 24 18 34-30 38 38' stroke='rgba(255,255,255,0.25)' stroke-width='5' fill='none' stroke-linecap='round' stroke-linejoin='round'/>
<circle cx='168' cy='42' r='9' fill='rgba(255,255,255,0.3)'/>
<text x='14' y='116' font-size='12' fill='#dbe7f3' font-family='Segoe UI, sans-serif'>${label}</text>
<text x='204' y='116' text-anchor='end' font-size='10' fill='rgba(219,231,243,0.75)' font-family='Segoe UI, sans-serif'>mock image</text>
</svg>`;
    return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}

const MOCK_SOLAR_TECHNICIANS: MockSolarTechnician[] = [
    { id: "st-001", name: "Luc Martin", role: "Senior Field Technician", skills: ["Inverter", "String Combiner"], certifications: ["HV", "Arc-Flash"], siteCoverage: ["CESTAS", "TOUL"], availability: "available", etaMin: 25, photo: svgTechAvatar("Luc Martin", 1) },
    { id: "st-002", name: "Nina Dupont", role: "PV Module Specialist", skills: ["PV Module", "Tracker Motor"], certifications: ["IR Thermography", "PV O&M"], siteCoverage: ["MARVILLE", "MASSANGIS"], availability: "available", etaMin: 32, photo: svgTechAvatar("Nina Dupont", 2) },
    { id: "st-003", name: "Hugo Bernard", role: "Inverter Engineer", skills: ["Inverter"], certifications: ["SCADA", "Grid Code"], siteCoverage: ["CESTAS", "GABARDAN", "MEES"], availability: "busy", etaMin: 46, photo: svgTechAvatar("Hugo Bernard", 3) },
    { id: "st-004", name: "Emma Leroy", role: "Electro-Mechanical Technician", skills: ["Tracker Motor", "String Combiner"], certifications: ["Electrical Safety", "Motor Diagnostics"], siteCoverage: ["MEES", "TOUL", "MASSANGIS"], availability: "available", etaMin: 22, photo: svgTechAvatar("Emma Leroy", 4) },
    { id: "st-005", name: "Yanis Moreau", role: "Regional Backup", skills: ["PV Module", "Inverter", "String Combiner"], certifications: ["PV O&M"], siteCoverage: ["CESTAS", "MARVILLE", "TOUL", "MEES", "GABARDAN", "MASSANGIS"], availability: "off", etaMin: 60, photo: svgTechAvatar("Yanis Moreau", 5) },
];

const MOCK_REPAIR_EVIDENCE = [
    { id: "ev-hotspot", label: "Thermal hotspot - module", image: svgMockEvidenceImage("Thermal hotspot - module", 1) },
    { id: "ev-inverter", label: "Inverter cabinet alarm", image: svgMockEvidenceImage("Inverter cabinet alarm", 2) },
    { id: "ev-combiner", label: "String combiner inspection", image: svgMockEvidenceImage("String combiner inspection", 3) },
    { id: "ev-tracker", label: "Tracker motor mechanical issue", image: svgMockEvidenceImage("Tracker motor mechanical issue", 4) },
];

function rankSolarTechnicians(
    technicians: MockSolarTechnician[],
    siteId: string,
    skill: SolarRepairSkill,
    priority: SolarRepairPriority,
) {
    const priorityBoost = priority === "P1" ? 20 : priority === "P2" ? 10 : 0;
    return technicians
        .map((tech) => {
            const hasSkill = tech.skills.includes(skill);
            const siteMatch = tech.siteCoverage.includes(siteId);
            const availabilityScore = tech.availability === "available" ? 24 : tech.availability === "busy" ? 10 : -8;
            const speedScore = Math.max(0, 35 - tech.etaMin);
            const score = (hasSkill ? 34 : 12) + (siteMatch ? 20 : 8) + availabilityScore + speedScore + priorityBoost;
            const reason = [
                hasSkill ? `${skill} certified` : `cross-skilled for ${skill}`,
                siteMatch ? "site familiar" : "regional support",
                tech.availability,
                `ETA ${tech.etaMin} min`,
            ].join(" · ");
            return { ...tech, score, reason };
        })
        .sort((a, b) => b.score - a.score);
}

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
    const [writebackMessage, setWritebackMessage] = useState<string | null>(null);
    const [ackLog, setAckLog] = useState<Record<string, { at: string; by: string }>>(() => JSON.parse(localStorage.getItem("solar-ack-log") ?? "{}"));
    const [ackMessage, setAckMessage] = useState<string | null>(null);
    const [operatorRole, setOperatorRole] = useState<OperatorRole>(() => normalizeOperatorRole(localStorage.getItem("solar-operator-role")));
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
    const [historyWindow, setHistoryWindow] = useState<HistoryWindow>("6h");
    const [wbAction, setWbAction] = useState("Acknowledge");
    const [wbSetpoint, setWbSetpoint] = useState("");
    const [wbNote, setWbNote] = useState("");
    const [repairPriority, setRepairPriority] = useState<SolarRepairPriority>("P2");
    const [repairSkill, setRepairSkill] = useState<SolarRepairSkill>("Inverter");
    const [selectedTechnicianId, setSelectedTechnicianId] = useState(MOCK_SOLAR_TECHNICIANS[0]?.id ?? "");
    const [repairSummary, setRepairSummary] = useState("");
    const [repairEvidenceId, setRepairEvidenceId] = useState(MOCK_REPAIR_EVIDENCE[0]?.id ?? "");
    const [repairOrderMessage, setRepairOrderMessage] = useState<string | null>(null);
    const canWriteback = canManageDispatch(operatorRole);
    const historyLimit = historyPointLimit(historyWindow);

    useEffect(() => {
        localStorage.setItem("solar-operator-role", operatorRole);
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

    const openTurbine = useCallback((id: string) => {
        setSelectedId(id);
        setDetailOpen(true);
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

    // Maintain a short rolling window of each plant's anomaly score so the watch
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

    const acknowledgeAlert = useCallback(async (t: PlantTelemetry) => {
        if (!canWriteback) {
            setAckMessage("Viewer mode — switch to Operator to acknowledge alarms.");
            return;
        }
        const entry = { at: new Date().toISOString(), by: "operator" };
        setAckLog((prev) => {
            const next = { ...prev, [t.id]: entry };
            localStorage.setItem("solar-ack-log", JSON.stringify(next));
            return next;
        });
        setAckMessage(`Acknowledged ${t.id}…`);
        try {
            await saveDispatchNote({
                turbineId: t.id,
                siteId: t.siteId,
                status: t.status,
                powerKw: t.powerKw,
                note: `[Acknowledge] alert ack (module ${t.moduleTempC} C, inverter ${t.inverterLoadPct} %)`,
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
                        note: `[Auto] alarm onset (module ${t.moduleTempC} C, inverter ${t.inverterLoadPct} %)`,
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
                    detail: `module ${t.moduleTempC} C, inverter ${t.inverterLoadPct} %`,
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
                .map((t) => ({ id: t.id, siteId: t.siteId, powerKw: t.powerKw, irradianceWm2: t.irradianceWm2 }));

            const result = await askFabricIQ(trimmed, {
                selectedTurbineId: selected.id,
                selectedSite: selected.siteName,
                fleet: {
                    units: turbines.length,
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
            const fallback = JSON.parse(localStorage.getItem("solar-writeback-log") ?? "[]") as unknown[];
            fallback.push({
                turbineId: selected.id,
                siteId: selected.siteId,
                status: selected.status,
                powerKw: setpointKw,
                note: composedNote,
                createdAt: new Date().toISOString(),
            });
            localStorage.setItem("solar-writeback-log", JSON.stringify(fallback));
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

    const fleetRatedMw = SITES.reduce((s, x) => s + x.arrayCount * x.capacityMw, 0);
    const capacityFactor = fleetRatedMw > 0 ? (fleetPower / 1000 / fleetRatedMw) * 100 : 0;
    const avgWind = visibleTurbines.length ? visibleTurbines.reduce((s, t) => s + t.irradianceWm2, 0) / visibleTurbines.length : 0;
    const avgTemp = visibleTurbines.length ? visibleTurbines.reduce((s, t) => s + t.moduleTempC, 0) / visibleTurbines.length : 0;
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
        x: t.irradianceWm2,
        y: t.powerKw,
        color: STATUS_COLORS[t.status],
        label: `${t.id} \u00b7 ${t.irradianceWm2} W/m\u00b2 \u00b7 ${t.powerKw.toLocaleString()} kW \u00b7 ${t.status}`,
    }));
    const curvePowerMax = Math.max(1000, ...powerCurve.map((p) => p.y));

    const selectedSite = sites.find((s) => s.id === selected.siteId);
    const relatedNotes = notes.filter((n) => n.turbineId === selected.id);

    const rankedTechnicians = useMemo(
        () => rankSolarTechnicians(MOCK_SOLAR_TECHNICIANS, selected.siteId, repairSkill, repairPriority),
        [repairPriority, repairSkill, selected.siteId],
    );

    useEffect(() => {
        if (rankedTechnicians.length === 0) {
            return;
        }
        if (!rankedTechnicians.some((tech) => tech.id === selectedTechnicianId)) {
            setSelectedTechnicianId(rankedTechnicians[0].id);
        }
    }, [rankedTechnicians, selectedTechnicianId]);

    useEffect(() => {
        const suggested = selected.status === "alarm" ? "P1" : selected.status === "warning" ? "P2" : "P3";
        setRepairPriority(suggested);
    }, [selected.status]);

    const selectedTechnician = useMemo(
        () => rankedTechnicians.find((tech) => tech.id === selectedTechnicianId) ?? rankedTechnicians[0],
        [rankedTechnicians, selectedTechnicianId],
    );

    const selectedEvidence = useMemo(
        () => MOCK_REPAIR_EVIDENCE.find((ev) => ev.id === repairEvidenceId) ?? MOCK_REPAIR_EVIDENCE[0],
        [repairEvidenceId],
    );

    const handleSendRepairOrder = useCallback(async () => {
        setRepairOrderMessage(null);
        if (!canWriteback) {
            setRepairOrderMessage("Viewer mode — switch to Operator to send repair orders.");
            return;
        }
        if (!selectedTechnician) {
            setRepairOrderMessage("No technician available for this skill.");
            return;
        }
        const summary = repairSummary.trim() || `Repair required on ${selected.id}`;
        const note = [
            `[RepairOrder ${repairPriority}]`,
            `skill=${repairSkill}`,
            `technician=${selectedTechnician.name} (${selectedTechnician.role})`,
            `eta=${selectedTechnician.etaMin}min`,
            `skills=${selectedTechnician.skills.join("/")}`,
            `certs=${selectedTechnician.certifications.join("/")}`,
            `evidence=${selectedEvidence.label}`,
            `evidenceUrl=${selectedEvidence.image}`,
            `summary=${summary}`,
        ].join(" | ").slice(0, 5000);
        try {
            await saveDispatchNote({
                turbineId: selected.id,
                siteId: selected.siteId,
                status: selected.status,
                powerKw: selected.powerKw,
                note,
                author: "operator",
                createdAt: new Date().toISOString(),
            });
            setRepairOrderMessage(`Repair order sent to ${selectedTechnician.name} (${repairPriority}, ${repairSkill}).`);
            setRepairSummary("");
            void loadNotes();
        } catch {
            const fallback = JSON.parse(localStorage.getItem("solar-repair-orders") ?? "[]") as unknown[];
            fallback.push({
                turbineId: selected.id,
                siteId: selected.siteId,
                status: selected.status,
                powerKw: selected.powerKw,
                note,
                createdAt: new Date().toISOString(),
            });
            localStorage.setItem("solar-repair-orders", JSON.stringify(fallback));
            setRepairOrderMessage(`Backend unreachable. Repair order saved locally (${fallback.length} records).`);
        }
    }, [canWriteback, loadNotes, repairPriority, repairSkill, repairSummary, selected.id, selected.powerKw, selected.siteId, selected.status, selectedEvidence.image, selectedEvidence.label, selectedTechnician]);

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
                    placeholder="Find array…"
                    aria-label="Find array by id"
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
                    <h1 className="text-xl font-semibold leading-tight">Geo Solar Twin Command Center · France</h1>
                    <p className="mt-0.5 text-[10px] text-amber-300/80" title="Live fleet telemetry is simulated in-browser; ontology sites and dispatch notes persist to the Fabric Rayfin backend.">
                        ◐ Simulated telemetry · ontology + dispatch notes persisted to Fabric
                    </p>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                    <KpiPill label="Fleet" value={`${(fleetPower / 1000).toFixed(2)} MW`} color="#6ee7ff" />
                    <KpiPill label="Healthy" value={String(healthy)} color={STATUS_COLORS.healthy} />
                    <KpiPill label="Warning" value={String(warnings)} color={STATUS_COLORS.warning} />
                    <KpiPill label="Alarm" value={String(alarms)} color={STATUS_COLORS.alarm} />
                    <KpiPill label="Arrays" value={`${visibleTurbines.length}/${turbines.length}`} />
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
                        <div className="text-slate-400">pde_solarfrance</div>
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
                                    {selected.status} · {selected.powerKw.toLocaleString()} kW · {selected.irradianceWm2} W/m²
                                </p>
                                <div className="mt-2"><Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} /></div>
                                <div className="mt-2 flex gap-1">
                                    <button type="button" onClick={() => setDetailOpen(true)} className="flex-1 rounded bg-cyan-600 px-2 py-1 text-xs font-medium text-white">Details</button>
                                    <button type="button" onClick={() => setView("operations")} className="flex-1 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white">Dispatch</button>
                                </div>
                            </div>

                            <div className="absolute bottom-3 left-3 hidden rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-1.5 text-xs text-slate-400 sm:block">
                                Click any plant to open its live detail popup · drag to pan · scroll or use ＋ / － to zoom.
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
                                                <LazyPlantTwinScene turbine={selected} paused={!live} />
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
                                            <div className="flex justify-between"><dt className="text-slate-400">Entity type</dt><dd className="text-cyan-200">SolarPlant</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Instance</dt><dd>{selected.id}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">belongsTo → SolarSite</dt><dd>{selected.siteName}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Region</dt><dd>{selectedSite?.region ?? "—"}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Coordinates</dt><dd>{selected.latitude.toFixed(3)}, {selected.longitude.toFixed(3)}</dd></div>
                                            <div className="flex justify-between"><dt className="text-slate-400">Rated capacity</dt><dd>{selectedSite ? `${selectedSite.capacityMw} MWc` : "—"}</dd></div>
                                        </dl>
                                    </Panel>

                                    <Panel title="Live signals (timeseries)">
                                        <div className="mb-2">
                                            <p className="text-xs text-slate-400">Active power</p>
                                            <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} />
                                            <p className="mt-0.5 text-right text-xs text-cyan-200">{selected.powerKw.toLocaleString()} kW</p>
                                        </div>
                                        <div className="space-y-2">
                                            <Meter label="Irradiance" value={selected.irradianceWm2} unit="W/m²" max={1100} warn={SIGNAL_METADATA.irradiance.warn} alarm={SIGNAL_METADATA.irradiance.alarm} property={SIGNAL_METADATA.irradiance.ontologyProperty} />
                                            <Meter label="Module temp" value={selected.moduleTempC} unit="°C" max={95} warn={SIGNAL_METADATA.moduleTemp.warn} alarm={SIGNAL_METADATA.moduleTemp.alarm} property={SIGNAL_METADATA.moduleTemp.ontologyProperty} />
                                            <Meter label="Inverter load" value={selected.inverterLoadPct} unit="%" max={100} warn={SIGNAL_METADATA.inverterLoad.warn} alarm={SIGNAL_METADATA.inverterLoad.alarm} property={SIGNAL_METADATA.inverterLoad.ontologyProperty} />
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
                                                        <span style={{ color: STATUS_COLORS[n.status as PlantStatus] ?? "#94a3b8" }}>
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
                                                        <span>Module <span style={{ color: signalColor("moduleTemp", t.moduleTempC) }}>{t.moduleTempC} C</span></span>
                                                        <span>Inverter <span style={{ color: signalColor("inverterLoad", t.inverterLoadPct) }}>{t.inverterLoadPct} %</span></span>
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
                                    <p className="mb-2 text-[11px] text-slate-400">Arrays trending toward thresholds, ranked by anomaly score with a slope-based escalation trend and ETA to alarm.</p>
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
                                <p className="text-xs uppercase tracking-wide text-slate-400">Ontology relationships · Fleet → Site → Plant → Signal</p>
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
                                    <div className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full bg-[#58d68d]" /> Plant</div>
                                    <div className="mt-1 flex items-center gap-1.5"><span className="inline-block h-0 w-3 border-t border-dashed border-slate-400" /> Signal (emits)</div>
                                    <p className="mt-1.5 text-slate-500">healthy · <span className="text-[#ffd166]">warn</span> · <span className="text-[#ef476f]">alarm</span></p>
                                    <p className="mt-1 max-w-[160px] text-[10px] leading-tight text-slate-500">Bands from ontology Sensor thresholds — hover a signal for its property &amp; limits.</p>
                                </div>
                                <div className="pointer-events-none absolute bottom-2 left-3 text-[11px] text-slate-500">Scroll to zoom · drag to pan · click a plant to drill into its signals</div>
                            </div>
                        </div>
                    )}

                    {view === "analytics" && (
                        <div className="h-full overflow-y-auto p-4">
                            <div className="mb-3">{toolbar}</div>
                            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                                <MetricCard label="Fleet output" value={`${(fleetPower / 1000).toFixed(2)} MW`} sub={`${visibleTurbines.length} arrays online`} />
                                <MetricCard label="Capacity factor" value={`${capacityFactor.toFixed(0)}%`} sub={`of ${fleetRatedMw.toFixed(0)} MWc rated`} accent="text-emerald-300" />
                                <MetricCard label="Avg irradiance" value={`${avgWind.toFixed(0)} W/m²`} sub="across visible fleet" />
                                <MetricCard label="Avg module temp" value={`${avgTemp.toFixed(1)} °C`} sub="thermal load" accent="text-amber-300" />
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
                                    title="Power curve · irradiance vs output"
                                    action={
                                        <div className="flex flex-wrap gap-3 text-[10px]">
                                            <span style={{ color: STATUS_COLORS.healthy }}>● Healthy</span>
                                            <span style={{ color: STATUS_COLORS.warning }}>● Warning</span>
                                            <span style={{ color: STATUS_COLORS.alarm }}>● Alarm</span>
                                        </div>
                                    }
                                >
                                    <ScatterPlot points={powerCurve} xLabel="Irradiance (W/m²)" yLabel="Power (kW)" xMax={1000} yMax={curvePowerMax} />
                                    <p className="mt-1 text-[10px] leading-tight text-slate-500">
                                        Each dot is a visible array. Points hugging the upper-right convert sunlight to power efficiently; low-output dots at high irradiance are underperformers worth inspecting.
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
                                                <MetricCard label="Output" value={`${(s.totalKw / 1000).toFixed(2)} MW`} sub={`${s.plantCount} plants`} />
                                                <MetricCard label="Capacity" value={`${s.capacityFactor.toFixed(0)}%`} sub={`of ${s.ratedMw.toFixed(0)} MW`} accent="text-emerald-300" />
                                                <MetricCard label="Avg module" value={`${s.avgModuleTempC.toFixed(0)} °C`} sub={`load ${s.avgInverterLoadPct.toFixed(0)}%`} accent="text-amber-300" />
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
                                                            <span className="text-slate-400">{t.powerKw.toLocaleString()} kW · {t.inverterLoadPct}%</span>
                                                        </button>
                                                    </li>
                                                ))}
                                                {local.length === 0 && <li className="text-xs text-slate-500">No plants at this site.</li>}
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
                                <Panel title="Selected plant">
                                    <p className="text-lg font-semibold">{selected.id}</p>
                                    <p className="text-sm text-slate-400">{selected.siteName}</p>
                                    <p className="text-sm" style={{ color: STATUS_COLORS[selected.status] }}>Status: {selected.status}</p>
                                    <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-sm text-slate-300">
                                        <div className="flex justify-between"><dt>Power</dt><dd>{selected.powerKw.toLocaleString()} kW</dd></div>
                                        <div className="flex justify-between"><dt>Irradiance</dt><dd>{selected.irradianceWm2} W/m²</dd></div>
                                        <div className="flex justify-between"><dt>Module</dt><dd>{selected.moduleTempC} °C</dd></div>
                                        <div className="flex justify-between"><dt>Inverter</dt><dd>{selected.inverterLoadPct} %</dd></div>
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

                                <Panel title="Repair order dispatch (technicians)">
                                    <p className="mb-2 text-[11px] text-slate-400">Create a repair order by selecting a technician profile, required skill, and mock evidence image.</p>
                                    <div className="grid grid-cols-3 gap-2 text-xs text-slate-400">
                                        <label className="flex flex-col gap-1">
                                            Priority
                                            <select value={repairPriority} onChange={(e) => setRepairPriority(e.target.value as SolarRepairPriority)} className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100">
                                                <option value="P1">P1</option>
                                                <option value="P2">P2</option>
                                                <option value="P3">P3</option>
                                            </select>
                                        </label>
                                        <label className="flex flex-col gap-1">
                                            Skill
                                            <select value={repairSkill} onChange={(e) => setRepairSkill(e.target.value as SolarRepairSkill)} className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100">
                                                <option value="Inverter">Inverter</option>
                                                <option value="PV Module">PV Module</option>
                                                <option value="String Combiner">String Combiner</option>
                                                <option value="Tracker Motor">Tracker Motor</option>
                                            </select>
                                        </label>
                                        <label className="flex flex-col gap-1">
                                            Technician
                                            <select value={selectedTechnicianId} onChange={(e) => setSelectedTechnicianId(e.target.value)} className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100">
                                                {rankedTechnicians.map((tech) => (
                                                    <option key={tech.id} value={tech.id}>{tech.name} · ETA {tech.etaMin}m</option>
                                                ))}
                                            </select>
                                        </label>
                                    </div>

                                    <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
                                        {rankedTechnicians.slice(0, 4).map((tech) => {
                                            const active = tech.id === selectedTechnicianId;
                                            return (
                                                <button
                                                    key={tech.id}
                                                    type="button"
                                                    onClick={() => setSelectedTechnicianId(tech.id)}
                                                    className={`rounded border px-2 py-2 text-left ${active ? "border-cyan-500 bg-[#0a203c]" : "border-slate-700 bg-[#08142a] hover:border-slate-500"}`}
                                                >
                                                    <div className="flex items-start gap-2">
                                                        <img src={tech.photo} alt={`${tech.name} profile`} className="h-12 w-12 rounded-md border border-slate-600/70 object-cover" />
                                                        <div className="min-w-0 flex-1">
                                                            <p className="text-xs font-semibold text-slate-100">{tech.name}</p>
                                                            <p className="text-[11px] text-slate-400">{tech.role}</p>
                                                            <p className="mt-0.5 text-[11px] text-cyan-200">{tech.reason}</p>
                                                        </div>
                                                    </div>
                                                    <div className="mt-1 flex flex-wrap gap-1">
                                                        {tech.skills.map((skill) => (
                                                            <span key={`${tech.id}-${skill}`} className="rounded bg-slate-700/70 px-1.5 py-0.5 text-[10px] text-slate-200">{skill}</span>
                                                        ))}
                                                        {tech.certifications.map((cert) => (
                                                            <span key={`${tech.id}-${cert}`} className="rounded bg-emerald-900/40 px-1.5 py-0.5 text-[10px] text-emerald-200">{cert}</span>
                                                        ))}
                                                    </div>
                                                </button>
                                            );
                                        })}
                                    </div>

                                    <div className="mt-2">
                                        <p className="mb-1 text-[11px] text-slate-400">Mock evidence image</p>
                                        <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
                                            {MOCK_REPAIR_EVIDENCE.map((ev) => {
                                                const active = ev.id === repairEvidenceId;
                                                return (
                                                    <button
                                                        key={ev.id}
                                                        type="button"
                                                        onClick={() => setRepairEvidenceId(ev.id)}
                                                        className={`rounded border p-1 text-left ${active ? "border-cyan-500" : "border-slate-700 hover:border-slate-500"}`}
                                                    >
                                                        <img src={ev.image} alt={ev.label} className="h-16 w-full rounded object-cover" />
                                                        <p className="mt-1 truncate text-[10px] text-slate-300" title={ev.label}>{ev.label}</p>
                                                    </button>
                                                );
                                            })}
                                        </div>
                                    </div>

                                    <textarea
                                        value={repairSummary}
                                        onChange={(e) => setRepairSummary(e.target.value)}
                                        disabled={!canWriteback}
                                        rows={2}
                                        placeholder="Repair summary (fault details, urgency, safety constraints)..."
                                        className="mt-2 w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm text-slate-100"
                                    />

                                    {selectedTechnician && (
                                        <p className="mt-1 text-[11px] text-slate-400">
                                            Selected: <span className="text-slate-200">{selectedTechnician.name}</span> · {selectedTechnician.role} · ETA {selectedTechnician.etaMin} min
                                        </p>
                                    )}

                                    <button
                                        type="button"
                                        onClick={() => void handleSendRepairOrder()}
                                        disabled={!canWriteback || !selectedTechnician}
                                        className="mt-2 w-full rounded bg-cyan-600 px-3 py-2 text-sm font-medium text-white hover:bg-cyan-500 disabled:cursor-not-allowed disabled:opacity-50"
                                    >
                                        {canWriteback ? "Send repair order" : "Viewer mode — repair order disabled"}
                                    </button>
                                    {repairOrderMessage && <p className="mt-1 text-xs text-emerald-300">{repairOrderMessage}</p>}
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
                                                        <span style={{ color: STATUS_COLORS[n.status as PlantStatus] ?? "#94a3b8" }}>
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
                                        placeholder="Ask about output, alarms, inverter load, irradiance, dispatch notes…  (Ctrl+Enter to send)"
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
                                <p className="text-[11px] uppercase tracking-wide text-cyan-300">Plant detail</p>
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
                            <div className="flex justify-between"><dt className="text-slate-400">Irradiance</dt><dd>{selected.irradianceWm2} W/m²</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Module temp</dt><dd>{selected.moduleTempC} °C</dd></div>
                            <div className="flex justify-between"><dt className="text-slate-400">Inverter load</dt><dd>{selected.inverterLoadPct} %</dd></div>
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
