//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode, type PointerEvent as ReactPointerEvent, type WheelEvent as ReactWheelEvent } from "react";
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
};

type PanelRenderRefs = {
    blades: THREE.Group;
    nacelleMat: THREE.MeshStandardMaterial;
    ringMat: THREE.MeshBasicMaterial;
    ring: THREE.Mesh;
    spin: number;
};

type SceneState = {
    byId: Map<string, PanelRenderRefs>;
    cleanup: () => void;
};

const STATUS_COLORS: Record<PlantStatus, string> = {
    healthy: "#58d68d",
    warning: "#ffd166",
    alarm: "#ef476f",
};

const SITE_COLORS = ["#6ee7ff", "#9bffb0", "#fbc2eb", "#ffd166", "#b4d4ff", "#a1c4fd"];

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

function seededRand(seed: number) {
    const s = Math.sin(seed) * 10000;
    return s - Math.floor(s);
}

// Worldwide equirectangular projection onto the 92 x 52 world plane: longitude
// -180..180 maps across X, latitude 90..-90 across Z, so every refinery lands on its
// real position on the world map.
function projectLonToX(lon: number) {
    return ((lon + 180) / 360) * 92 - 46;
}

function projectLatToZ(lat: number) {
    return ((90 - lat) / 180) * 52 - 26;
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

// Anchor points (in twin-scene world space) for the part value callouts.
const TWIN_PARTS: { key: string; caption: string; pos: [number, number, number] }[] = [
    { key: "array", caption: "Column", pos: [0, 4.0, -1.4] },
    { key: "inverter", caption: "Tank", pos: [-3.6, 1.5, 0] },
    { key: "tracker", caption: "Flare", pos: [2.8, 0.9, 1.4] },
    { key: "output", caption: "Throughput", pos: [0, 0.7, 2.8] },
];

function createMapTexture(sites: SolarPlantSite[]) {
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
        const v = (projectLatToZ(lat) + 26) / 52;
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
function createSkyTexture() {
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
function createOceanTexture() {
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
    // Prefer a real deployed Fabric Data Agent when one is configured; fall back
    // to the ontology-grounded local engine if it is absent or unreachable.
    if (isDataAgentConfigured()) {
        try {
            const agent = await queryDataAgent(question, context);
            return {
                source: "fabriciq",
                summary: agent.summary,
                generatedAt: new Date().toISOString(),
                queryText: agent.queryText,
            };
        } catch {
            /* fall through to ontology-grounded reasoning */
        }
    }
    const { summary, queryText } = await askOntology(question, context);
    return {
        source: "ontology",
        summary,
        generatedAt: new Date().toISOString(),
        queryText,
    };
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

function SolarFleetScene({
    turbines,
    sites,
    selectedId,
    dimmedIds,
    paused,
    onSelect,
}: {
    turbines: PlantTelemetry[];
    sites: SolarPlantSite[];
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

        // Image-based lighting from the sky gradient so steel columns and LPG
        // spheres pick up soft, realistic reflections.
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

        // Map plane spans exactly 92 x 52 world units so lon/lat -> X/Z matches
        // projectLonToX / projectLatToZ, pinning every turbine to its real location.
        // Transparent + depthWrite:false so the ocean shows through and turbines stay on top.
        const mapTexture = createMapTexture(sites);
        const mapPlane = new THREE.Mesh(
            new THREE.PlaneGeometry(92, 52, 1, 1),
            new THREE.MeshBasicMaterial({ map: mapTexture, transparent: true, depthWrite: false })
        );
        mapPlane.rotation.x = -Math.PI / 2;
        mapPlane.position.y = 0.06;
        scene.add(mapPlane);

        const grid = new THREE.GridHelper(92, 24, 0x2a557f, 0x2a557f);
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

        const byId = new Map<string, PanelRenderRefs>();
        const pickables: THREE.Mesh[] = [];
        const raycaster = new THREE.Raycaster();
        const pointer = new THREE.Vector2();

        // ---- Shared refinery-unit geometry (created once, instanced per unit) ----
        const padGeo = new THREE.BoxGeometry(2.0, 0.14, 1.4);
        const colBaseGeo = new THREE.CylinderGeometry(0.34, 0.4, 1.2, 18);
        const colMidGeo = new THREE.CylinderGeometry(0.27, 0.32, 1.05, 18);
        const colTopGeo = new THREE.CylinderGeometry(0.2, 0.25, 0.75, 18);
        const colDomeGeo = new THREE.SphereGeometry(0.2, 18, 10, 0, Math.PI * 2, 0, Math.PI / 2);
        const ventGeo = new THREE.CylinderGeometry(0.04, 0.05, 0.5, 8);
        const platformRingGeo = new THREE.TorusGeometry(0.33, 0.035, 8, 22);
        const sphereTankGeo = new THREE.SphereGeometry(0.4, 20, 16);
        const legGeo = new THREE.CylinderGeometry(0.035, 0.035, 0.45, 6);
        const tankGeo = new THREE.CylinderGeometry(0.5, 0.5, 0.6, 22);
        const tankRimGeo = new THREE.TorusGeometry(0.5, 0.04, 8, 24);
        const tankTopGeo = new THREE.CylinderGeometry(0.34, 0.5, 0.18, 22);
        const pipeGeo = new THREE.CylinderGeometry(0.05, 0.05, 1.25, 8);
        const pipeSupportGeo = new THREE.BoxGeometry(0.08, 0.5, 0.08);
        const stackGeo = new THREE.CylinderGeometry(0.07, 0.11, 2.6, 12);
        const flameOuterGeo = new THREE.ConeGeometry(0.2, 0.8, 14);
        const flameInnerGeo = new THREE.ConeGeometry(0.11, 0.5, 12);
        const beaconGeo = new THREE.SphereGeometry(0.15, 14, 14);
        const ringGeoUnit = new THREE.RingGeometry(0.95, 1.25, 36);
        const pickGeo = new THREE.BoxGeometry(2.3, 3.0, 1.7);

        // Refinery hardware materials: brushed-steel column, pale tanks, grey stacks
        // and a layered self-lit flame so units pop against the dark ocean scene.
        const columnMat = new THREE.MeshStandardMaterial({ color: "#c2ccd9", roughness: 0.34, metalness: 0.78 });
        const tankMat = new THREE.MeshStandardMaterial({ color: "#dde3ea", roughness: 0.48, metalness: 0.42 });
        const sphereMat = new THREE.MeshStandardMaterial({ color: "#cfd8e1", roughness: 0.3, metalness: 0.62 });
        const stackMat = new THREE.MeshStandardMaterial({ color: "#7e8794", roughness: 0.58, metalness: 0.5 });
        const trimMat = new THREE.MeshStandardMaterial({ color: "#8b97a6", roughness: 0.5, metalness: 0.6 });
        const pipeMat = new THREE.MeshStandardMaterial({ color: "#9aa6b4", roughness: 0.5, metalness: 0.55 });
        const padMat = new THREE.MeshStandardMaterial({ color: "#4a5462", roughness: 0.78, metalness: 0.15 });
        const flameOuterMat = new THREE.MeshStandardMaterial({ color: "#ff8a3d", emissive: "#ff5a1f", emissiveIntensity: 1.0, transparent: true, opacity: 0.82, roughness: 0.5 });
        const flameInnerMat = new THREE.MeshStandardMaterial({ color: "#ffe08a", emissive: "#ffcc55", emissiveIntensity: 1.25, roughness: 0.4 });

        // A distillation column: stacked steel segments, domed top, vent pipe and
        // catwalk platform rings - the signature refinery silhouette.
        const buildColumn = () => {
            const unit = new THREE.Group();
            const base = new THREE.Mesh(colBaseGeo, columnMat); base.position.y = 0.74; base.castShadow = true; unit.add(base);
            const mid = new THREE.Mesh(colMidGeo, columnMat); mid.position.y = 1.72; mid.castShadow = true; unit.add(mid);
            const top = new THREE.Mesh(colTopGeo, columnMat); top.position.y = 2.55; top.castShadow = true; unit.add(top);
            const dome = new THREE.Mesh(colDomeGeo, columnMat); dome.position.y = 2.92; unit.add(dome);
            const vent = new THREE.Mesh(ventGeo, stackMat); vent.position.y = 3.25; unit.add(vent);
            [1.15, 2.05, 2.7].forEach((y) => {
                const r = new THREE.Mesh(platformRingGeo, trimMat);
                r.rotation.x = Math.PI / 2;
                r.position.y = y;
                unit.add(r);
            });
            return unit;
        };

        // A spherical LPG pressure store carried on four short legs.
        const buildSphereTank = () => {
            const g = new THREE.Group();
            const ball = new THREE.Mesh(sphereTankGeo, sphereMat); ball.position.y = 0.62; ball.castShadow = true; g.add(ball);
            for (let k = 0; k < 4; k += 1) {
                const a = (k / 4) * Math.PI * 2 + Math.PI / 4;
                const leg = new THREE.Mesh(legGeo, trimMat);
                leg.position.set(Math.cos(a) * 0.26, 0.22, Math.sin(a) * 0.26);
                g.add(leg);
            }
            return g;
        };

        turbines.forEach((t) => {
            const group = new THREE.Group();
            group.position.set(t.x, 0, t.z);
            scene.add(group);

            // Concrete pad.
            const pad = new THREE.Mesh(padGeo, padMat);
            pad.position.y = 0.08;
            pad.receiveShadow = true;
            group.add(pad);

            // Distillation column (rear-centre).
            const column = buildColumn();
            column.position.set(-0.1, 0, -0.15);
            group.add(column);

            // Floating-roof storage tank (front-left) with rim + domed roof.
            const tank = new THREE.Mesh(tankGeo, tankMat);
            tank.position.set(-0.66, 0.44, 0.34);
            tank.castShadow = true;
            group.add(tank);
            const tankRim = new THREE.Mesh(tankRimGeo, trimMat);
            tankRim.rotation.x = Math.PI / 2;
            tankRim.position.set(-0.66, 0.74, 0.34);
            group.add(tankRim);
            const tankTop = new THREE.Mesh(tankTopGeo, tankMat);
            tankTop.position.set(-0.66, 0.83, 0.34);
            group.add(tankTop);

            // LPG sphere (front-right).
            const sphere = buildSphereTank();
            sphere.position.set(0.66, 0, -0.42);
            group.add(sphere);

            // Pipe bridge linking the tank and the column.
            const pipe = new THREE.Mesh(pipeGeo, pipeMat);
            pipe.rotation.z = Math.PI / 2;
            pipe.position.set(-0.3, 0.62, 0.34);
            group.add(pipe);
            [-0.66, 0.05].forEach((px) => {
                const sup = new THREE.Mesh(pipeSupportGeo, pipeMat);
                sup.position.set(px, 0.37, 0.34);
                group.add(sup);
            });

            // Flare stack with a layered, flickering flame (animated by telemetry).
            const stack = new THREE.Mesh(stackGeo, stackMat);
            stack.position.set(0.74, 1.3, 0.5);
            group.add(stack);
            const flame = new THREE.Group();
            flame.position.set(0.74, 2.6, 0.5);
            const flameOuter = new THREE.Mesh(flameOuterGeo, flameOuterMat); flameOuter.position.y = 0.4; flame.add(flameOuter);
            const flameInner = new THREE.Mesh(flameInnerGeo, flameInnerMat); flameInner.position.y = 0.3; flame.add(flameInner);
            group.add(flame);

            // Status beacon on the column crown (color driven by live telemetry).
            const nacelleMat = new THREE.MeshStandardMaterial({
                color: STATUS_COLORS[t.status],
                emissive: STATUS_COLORS[t.status],
                emissiveIntensity: 0.6,
                roughness: 0.3,
                metalness: 0.2,
            });
            const beacon = new THREE.Mesh(beaconGeo, nacelleMat);
            beacon.position.set(-0.1, 3.55, -0.15);
            group.add(beacon);

            const pickMesh = new THREE.Mesh(
                pickGeo,
                new THREE.MeshBasicMaterial({ transparent: true, opacity: 0 })
            );
            pickMesh.position.y = 1.4;
            pickMesh.userData.turbineId = t.id;
            pickables.push(pickMesh);
            group.add(pickMesh);

            const ringMat = new THREE.MeshBasicMaterial({
                color: STATUS_COLORS[t.status],
                transparent: true,
                opacity: t.id === selectedId ? 0.9 : 0.55,
            });
            const ring = new THREE.Mesh(ringGeoUnit, ringMat);
            ring.rotation.x = -Math.PI / 2;
            ring.position.y = 0.03;
            group.add(ring);

            byId.set(t.id, {
                nacelleMat,
                ringMat,
                ring,
                blades: flame,
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
                    // Flare flame flicker driven by per-unit phase.
                    refs.blades.scale.y = 1 + Math.sin(tick * 2.2 + refs.spin * 30) * 0.28;
                    refs.blades.scale.x = 1 + Math.sin(tick * 3.1 + refs.spin * 12) * 0.12;
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
            meshRefs.spin = dimmed ? 0.003 : 0.055 + Math.min(0.22, t.irradianceWm2 / 80);
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

// Ids of units that transitioned into "alarm" since the previous status snapshot.
export function newlyAlarmed(prev: Record<string, PlantStatus>, turbines: { id: string; status: PlantStatus }[]): string[] {
    return turbines.filter((t) => t.status === "alarm" && prev[t.id] !== "alarm").map((t) => t.id);
}

type ViewKey = "map" | "twin" | "alerts" | "graph" | "analytics" | "operations" | "ask";

const NAV: { key: ViewKey; label: string; icon: string }[] = [
    { key: "map", label: "Map", icon: "🗺" },
    { key: "twin", label: "Digital Twin", icon: "🌀" },
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
    "Which refinery has the highest throughput now?",
    "Any units in alarm?",
    "Which unit has the highest utilization?",
    "What is the hottest unit temperature?",
    "Peak feed rate right now?",
    "How many process units total?",
];

function NavRail({ view, onChange, badges }: { view: ViewKey; onChange: (v: ViewKey) => void; badges?: Partial<Record<ViewKey, number>> }) {
    return (
        <nav className="flex w-14 flex-col gap-1 border-r border-slate-800/60 bg-[#06101fcc] py-3 md:w-44">
            {NAV.map((n) => {
                const badge = badges?.[n.key] ?? 0;
                return (
                    <button
                        key={n.key}
                        type="button"
                        onClick={() => onChange(n.key)}
                        className={`mx-2 flex items-center gap-3 rounded-lg px-3 py-2 text-left text-sm transition-colors ${view === n.key ? "bg-cyan-600/90 text-white" : "text-slate-300 hover:bg-slate-800/60"}`}
                    >
                        <span className="text-lg leading-none">{n.icon}</span>
                        <span className="hidden md:inline">{n.label}</span>
                        {badge > 0 && (
                            <span className="ml-auto rounded-full bg-rose-600 px-1.5 text-[10px] font-semibold leading-4 text-white">{badge}</span>
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
function PlantTwinScene({ turbine, paused }: { turbine: PlantTelemetry; paused: boolean }) {
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

        // Soft image-based reflections for the close-up steel hardware.
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

        const baseMat = new THREE.MeshStandardMaterial({ color: "#5a6675", roughness: 0.68, metalness: 0.22 });
        const steelMat = new THREE.MeshStandardMaterial({ color: "#c2cad6", roughness: 0.42, metalness: 0.68 });
        const tankMat = new THREE.MeshStandardMaterial({ color: "#dde3ea", roughness: 0.5, metalness: 0.38 });
        const stackMat = new THREE.MeshStandardMaterial({ color: "#8a939f", roughness: 0.6, metalness: 0.5 });
        const flameMat = new THREE.MeshStandardMaterial({ color: "#ff9b3d", emissive: "#ff5a1f", emissiveIntensity: 0.95, roughness: 0.4 });
        const pipeMat = new THREE.MeshStandardMaterial({ color: "#9aa6b4", roughness: 0.5, metalness: 0.55 });
        const statusMat = new THREE.MeshStandardMaterial({
            color: STATUS_COLORS[turbineRef.current.status],
            emissive: STATUS_COLORS[turbineRef.current.status],
            emissiveIntensity: 0.7,
            roughness: 0.3,
            metalness: 0.2,
        });
        const ringMat = new THREE.MeshBasicMaterial({ color: STATUS_COLORS[turbineRef.current.status], transparent: true, opacity: 0.85 });

        // Foundation pad block.
        const pedestal = new THREE.Mesh(new THREE.BoxGeometry(7.5, 0.4, 4.5), baseMat);
        pedestal.position.y = 0.2;
        pedestal.receiveShadow = true;
        group.add(pedestal);

        // Distillation column - the main process unit (caption "Column").
        const column = new THREE.Mesh(new THREE.CylinderGeometry(0.9, 1.1, 6.4, 28), steelMat);
        column.position.set(0, 3.6, 0);
        column.castShadow = true;
        group.add(column);
        [2.0, 3.6, 5.2].forEach((y) => {
            const plat = new THREE.Mesh(new THREE.TorusGeometry(1.05, 0.08, 10, 28), pipeMat);
            plat.rotation.x = Math.PI / 2;
            plat.position.y = y;
            group.add(plat);
        });

        // Storage tank (caption "Tank").
        const tank = new THREE.Mesh(new THREE.CylinderGeometry(1.5, 1.5, 2.4, 28), tankMat);
        tank.position.set(-3.6, 1.4, 0);
        tank.castShadow = true;
        group.add(tank);
        const tankDome = new THREE.Mesh(new THREE.SphereGeometry(1.5, 24, 12, 0, Math.PI * 2, 0, Math.PI / 2), tankMat);
        tankDome.position.set(-3.6, 2.6, 0);
        group.add(tankDome);

        // Flare stack with flame (caption "Flare"). `blades` holds the flame so the
        // animate loop can flicker it with live feed rate.
        const flareStack = new THREE.Mesh(new THREE.CylinderGeometry(0.18, 0.24, 3.6, 16), stackMat);
        flareStack.position.set(2.8, 1.8, 1.4);
        flareStack.castShadow = true;
        group.add(flareStack);
        const blades = new THREE.Group();
        blades.position.set(2.8, 3.9, 1.4);
        const flame = new THREE.Mesh(new THREE.ConeGeometry(0.4, 1.3, 16), flameMat);
        blades.add(flame);
        group.add(blades);

        // Product pipe header (caption "Throughput").
        const pipe = new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.22, 4.2, 16), pipeMat);
        pipe.rotation.z = Math.PI / 2;
        pipe.position.set(0, 0.7, 2.8);
        group.add(pipe);

        // Status beacon above the column.
        const beacon = new THREE.Mesh(new THREE.SphereGeometry(0.3, 16, 16), statusMat);
        beacon.position.set(0, 7.1, 0);
        group.add(beacon);

        const ring = new THREE.Mesh(new THREE.RingGeometry(3.6, 4.2, 48), ringMat);
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
                // Flare flame flicker, amplitude scaled by live feed rate.
                const feed = Math.min(1, turbineRef.current.irradianceWm2 / 200);
                blades.scale.y = 1 + Math.sin(tick * 4) * 0.22 * (0.5 + feed);
                blades.scale.x = 1 + Math.sin(tick * 5.3) * 0.12;
            }
            const z = zoomRef.current;
            camera.position.x = Math.sin(tick * 0.25) * 14 * z;
            camera.position.z = Math.cos(tick * 0.25) * 20 * z;
            camera.position.y = 7 * z + 2.5;
            // As you zoom in (smaller z), lower the focus toward the panel table so the
            // array's working parts fill the frame.
            const focus = THREE.MathUtils.clamp((z - 0.16) / (1 - 0.16), 0, 1);
            const focusY = THREE.MathUtils.lerp(5.0, 2.4, focus);
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
                    { text: `${turbine.irradianceWm2} kbd`, color: signalColor("irradiance", turbine.irradianceWm2) },
                    { text: `${turbine.moduleTempC}°C`, color: signalColor("moduleTemp", turbine.moduleTempC) },
                    { text: `${turbine.inverterLoadPct}%`, color: signalColor("inverterLoad", turbine.inverterLoadPct) },
                    { text: `${turbine.powerKw.toLocaleString()} kbd`, color: "#6ee7ff" },
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
    const [writebackMessage, setWritebackMessage] = useState<string | null>(null);
    const [ackLog, setAckLog] = useState<Record<string, { at: string; by: string }>>(() => JSON.parse(localStorage.getItem("refinery-ack-log") ?? "{}"));
    const [ackMessage, setAckMessage] = useState<string | null>(null);
    const [showAcked, setShowAcked] = useState(false);
    const [graphFilter, setGraphFilter] = useState<StatusFilter>("all");
    const [graphNonce, setGraphNonce] = useState(0);
    const [autoLogCount, setAutoLogCount] = useState(0);
    const prevStatusRef = useRef<Record<string, PlantStatus>>({});
    const autoLoggedRef = useRef<Set<string>>(new Set());
    const autoInitRef = useRef(false);

    const [powerHistory, setPowerHistory] = useState<number[]>([]);
    const historyKeyRef = useRef(selectedId);

    const [notes, setNotes] = useState<DispatchNoteRecord[]>([]);
    const [notesLoading, setNotesLoading] = useState(false);
    const [thresholdsPublished, setThresholdsPublished] = useState<number | null>(null);
    const [thresholdNonce, setThresholdNonce] = useState(0);

    const [forecastHorizon, setForecastHorizon] = useState(5);
    const [wbAction, setWbAction] = useState("Acknowledge");
    const [wbSetpoint, setWbSetpoint] = useState("");
    const [wbNote, setWbNote] = useState("");

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
        () => turbines.map((t) => ({ t, score: anomalyScore(t) })).sort((a, b) => b.score - a.score).slice(0, 6),
        [turbines],
    );
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
            return [...base, selected.powerKw].slice(-40);
        });
    }, [selected.powerKw, selectedId]);

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
            const history = await fetchPowerHistory(selectedId, 40);
            if (cancelled || !history || history.length === 0) {
                return;
            }
            historyKeyRef.current = selectedId;
            setPowerHistory(history.slice(-40));
        })();
        return () => {
            cancelled = true;
        };
    }, [selectedId]);

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
    }, [loadNotes]);

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
                        note: `[Auto] alarm onset (unit ${t.moduleTempC} C, utilization ${t.inverterLoadPct} %)`,
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

    const runAsk = useCallback(async (override?: string) => {
        setAskLoading(true);
        setAskError(null);
        try {
            const trimmed = (override ?? question).trim();
            if (!trimmed) {
                throw new Error("Please enter a question.");
            }

            const result = await askFabricIQ(trimmed, {
                selectedTurbineId: selected.id,
                selectedSite: selected.siteName,
                telemetry: turbines.map((t) => ({
                    id: t.id,
                    siteId: t.siteId,
                    siteName: t.siteName,
                    powerKw: t.powerKw,
                    irradianceWm2: t.irradianceWm2,
                    moduleTempC: t.moduleTempC,
                    inverterLoadPct: t.inverterLoadPct,
                    status: t.status,
                })),
            }).catch(() => ({
                source: "local" as const,
                summary: `${answerQuestion(trimmed, turbines)} (offline — ontology backend unreachable)`,
                generatedAt: new Date().toISOString(),
            }));

            setAskResult(result);
        } catch (err) {
            setAskError(err instanceof Error ? err.message : String(err));
        } finally {
            setAskLoading(false);
        }
    }, [question, selected.id, selected.siteName, turbines]);

    const handleWriteback = useCallback(async () => {
        setWritebackMessage(null);
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
            <select
                value={siteFilter}
                onChange={(e) => setSiteFilter(e.target.value)}
                className="rounded border border-slate-700 bg-[#08142a] px-2 py-1"
            >
                <option value="all">All sites</option>
                {sites.map((s) => (
                    <option key={s.id} value={s.id}>{s.name}</option>
                ))}
            </select>

            <div className="flex overflow-hidden rounded border border-slate-700">
                {(["all", "healthy", "warning", "alarm"] as StatusFilter[]).map((s) => (
                    <button
                        key={s}
                        type="button"
                        onClick={() => setStatusFilter(s)}
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
            <header className="flex flex-wrap items-center gap-3 border-b border-slate-800/60 bg-[#071126cc] px-5 py-3 backdrop-blur-sm">
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

                <div className="ml-auto flex items-center gap-2 text-xs text-slate-300">
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

            <div className="flex min-h-0 flex-1">
                <NavRail view={view} onChange={setView} badges={{ alerts: unackedAlerts.length }} />

                <section className="relative min-h-0 flex-1">
                    {view === "map" && (
                        <div className="relative h-full min-h-[520px]">
                            <SolarFleetScene turbines={turbines} sites={sites} selectedId={selected.id} dimmedIds={dimmedIds} paused={!live} onSelect={openTurbine} />

                            <div className="absolute left-3 top-3 max-w-[78%] rounded-lg border border-slate-700/60 bg-[#06101fd9] p-2 backdrop-blur">
                                {toolbar}
                            </div>

                            <div className="absolute right-3 top-3 w-60 rounded-lg border border-slate-700/60 bg-[#07162de6] p-3 backdrop-blur">
                                <p className="text-[10px] uppercase tracking-wide text-slate-400">Selected</p>
                                <p className="text-sm font-semibold">{selected.id}</p>
                                <p className="text-xs text-slate-400">{selected.siteName}</p>
                                <p className="text-xs" style={{ color: STATUS_COLORS[selected.status] }}>
                                    {selected.status} · {selected.powerKw.toLocaleString()} kbd · {selected.irradianceWm2} kbd feed
                                </p>
                                <div className="mt-2"><Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} /></div>
                                <div className="mt-2 flex gap-1">
                                    <button type="button" onClick={() => setDetailOpen(true)} className="flex-1 rounded bg-cyan-600 px-2 py-1 text-xs font-medium text-white">Details</button>
                                    <button type="button" onClick={() => setView("operations")} className="flex-1 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white">Dispatch</button>
                                </div>
                            </div>

                            <div className="absolute bottom-3 left-3 rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-1.5 text-xs text-slate-400">
                                Click any refinery to open its live detail popup · scroll or use ＋ / － to zoom.
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
                                        <PlantTwinScene turbine={selected} paused={!live} />
                                        <div className="absolute left-3 top-3 rounded-lg border border-slate-700/60 bg-[#06101fcc] px-3 py-2 backdrop-blur">
                                            <p className="text-sm font-semibold">{selected.id}</p>
                                            <p className="text-xs text-slate-400">{selected.siteName} · {selected.latitude.toFixed(2)}, {selected.longitude.toFixed(2)}</p>
                                        </div>
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
                                                        <span>Unit <span style={{ color: signalColor("moduleTemp", t.moduleTempC) }}>{t.moduleTempC} C</span></span>
                                                        <span>Utilization <span style={{ color: signalColor("inverterLoad", t.inverterLoadPct) }}>{t.inverterLoadPct} %</span></span>
                                                        <span>Throughput {t.powerKw.toLocaleString()} kbd</span>
                                                    </div>
                                                    <div className="mt-2 flex gap-2">
                                                        <button type="button" onClick={() => { setSelectedId(t.id); setView("twin"); }} className="rounded bg-slate-700 px-2 py-1 text-xs text-white">Inspect</button>
                                                        {ackLog[t.id] ? (
                                                            <span className="rounded bg-emerald-900/60 px-2 py-1 text-xs text-emerald-300">✓ Ack {new Date(ackLog[t.id].at).toLocaleTimeString()} · {ackLog[t.id].by}</span>
                                                        ) : (
                                                            <button type="button" onClick={() => void acknowledgeAlert(t)} className="rounded bg-emerald-600 px-2 py-1 text-xs text-white hover:bg-emerald-500">Acknowledge</button>
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
                                    <p className="mb-2 text-[11px] text-slate-400">Process units trending toward thresholds, ranked by anomaly score from unit temp &amp; utilization.</p>
                                    <ul className="space-y-2">
                                        {anomalyWatch.map((a) => (
                                            <li key={a.t.id}>
                                                <div className="flex justify-between text-xs">
                                                    <button type="button" onClick={() => { setSelectedId(a.t.id); setView("twin"); }} className="text-slate-200 hover:text-cyan-200">{a.t.id} · {a.t.siteName}</button>
                                                    <span className="text-slate-300">{Math.round(a.score * 100)}%</span>
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

                    {view === "operations" && (
                        <div className="h-full overflow-y-auto p-4">
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
                                        <p className="text-xs text-slate-400">Throughput trend (live)</p>
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
                                        <div className="flex gap-2">
                                            <label className="flex-1 text-xs text-slate-400">
                                                Action
                                                <select
                                                    value={wbAction}
                                                    onChange={(e) => setWbAction(e.target.value)}
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
                                                className="rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-slate-100"
                                            >
                                                <option value={3}>3 ticks</option>
                                                <option value={5}>5 ticks</option>
                                                <option value={10}>10 ticks</option>
                                            </select>
                                        </div>
                                        <input
                                            value={wbNote}
                                            onChange={(e) => setWbNote(e.target.value)}
                                            placeholder="Optional note…"
                                            className="w-full rounded border border-slate-700 bg-[#08142a] px-2 py-1 text-sm text-slate-100"
                                        />
                                        <button
                                            type="button"
                                            onClick={handleWriteback}
                                            className="w-full rounded bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500"
                                        >
                                            Write {wbAction} to ontology
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
                                    {askError && <p className="mt-2 text-xs text-rose-300">{askError}</p>}
                                    {askResult && (
                                        <div className="mt-3 rounded border border-slate-700 bg-[#081226] p-3 text-sm text-slate-200">
                                            <div className="flex items-center justify-between text-xs text-slate-400">
                                                <span>Source: {sourceLabel(askResult.source)}</span>
                                                <span>{new Date(askResult.generatedAt).toLocaleTimeString()}</span>
                                            </div>
                                            <p className="mt-1">{askResult.summary}</p>
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
                            <p className="text-xs text-slate-400">Throughput trend (live)</p>
                            <Sparkline values={powerHistory} color={STATUS_COLORS[selected.status]} forecast={fc} />
                        </div>
                        <div className="mt-2 flex items-center justify-between rounded border border-cyan-900/60 bg-[#06182f] px-3 py-1.5 text-xs">
                            <span className="text-slate-400">Forecast (+{forecastHorizon} ticks)</span>
                            <span className="font-semibold text-cyan-200">{forecast.toLocaleString()} kbd</span>
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
