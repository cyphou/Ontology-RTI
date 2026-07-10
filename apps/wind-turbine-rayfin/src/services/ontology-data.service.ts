//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { getRayfinClient } from "@/lib/rayfin-client";

/** Ontology `Site` reference row stored in the Rayfin data backend. */
export interface OntologySiteRecord {
    id?: string;
    siteId: string;
    name: string;
    country: string;
    latitude: number;
    longitude: number;
    capacityMw: number;
}

/** Operator dispatch note (the writeback target) keyed to ontology ids. */
export interface DispatchNoteRecord {
    id?: string;
    turbineId: string;
    siteId: string;
    status: string;
    powerKw: number;
    note: string;
    author: string;
    createdAt: string;
}

/**
 * Predictive maintenance work order (a structured writeback target). Raised from
 * the anomaly escalation forecast + what-if simulator, it captures the suspected
 * component, priority, the planned curtailment/downtime intervention and its
 * projected energy impact, and a lifecycle status — so a predicted failure turns
 * into a tracked, auditable action rather than a free-text note.
 */
export interface MaintenanceOrderRecord {
    id?: string;
    turbineId: string;
    siteId: string;
    component: string;
    priority: string;
    status: string;
    curtailPct: number;
    downtimeTicks: number;
    projectedDeltaKwt: number;
    assignee: string;
    note: string;
    createdAt: string;
}

/** Schema map consumed by {@link RayfinClient} for typed GraphQL proxies. */
export type OntologyDataSchema = {
    WindSite: OntologySiteRecord;
    DispatchNote: DispatchNoteRecord;
    SensorThreshold: SignalThresholdRecord;
    MaintenanceOrder: MaintenanceOrderRecord;
};

/**
 * Ontology-grounded signal threshold band (warn / alarm) for a measured signal.
 * Mirrors the WindTurbine ontology `Sensor` MinThreshold/MaxThreshold metadata.
 * `warn` / `alarm` may be `Infinity` in the app for informational signals; on the
 * wire that is encoded as the {@link INF_BAND} sentinel so JSON can round-trip it.
 */
export interface SignalThresholdRecord {
    id?: string;
    signalKey: string;
    ontologyProperty: string;
    unit: string;
    warn: number;
    alarm: number;
    governsHealth: boolean;
}

/** Sentinel persisted in place of `Infinity` (which is not JSON-serializable). */
export const INF_BAND = -1;

/** Encode an app-side band value (possibly `Infinity`) for backend storage. */
export function encodeBand(value: number): number {
    return Number.isFinite(value) ? value : INF_BAND;
}

/** Decode a stored band value back into the app-side value (sentinel → `Infinity`). */
export function decodeBand(value: number): number {
    return value === INF_BAND ? Infinity : value;
}

/** List the published ontology signal thresholds. Throws if the backend is unreachable. */
export async function listSignalThresholds(): Promise<SignalThresholdRecord[]> {
    const rows = (await getRayfinClient()
        .data.SensorThreshold.select(["id", "signalKey", "ontologyProperty", "unit", "warn", "alarm", "governsHealth"])
        .execute()) as SignalThresholdRecord[];
    return rows.map((r) => ({ ...r, warn: decodeBand(r.warn), alarm: decodeBand(r.alarm) }));
}

/**
 * Idempotently publish the app's signal threshold bands to the ontology backend.
 * Best-effort: if the backend is unreachable the caller should swallow the
 * rejection. Returns the number of rows now present in the store.
 */
export async function ensureSignalThresholds(thresholds: SignalThresholdRecord[]): Promise<number> {
    const existing = await listSignalThresholds();
    if (existing.length > 0) {
        return existing.length;
    }
    for (const t of thresholds) {
        await getRayfinClient().data.SensorThreshold.create({
            signalKey: t.signalKey,
            ontologyProperty: t.ontologyProperty,
            unit: t.unit,
            warn: encodeBand(t.warn),
            alarm: encodeBand(t.alarm),
            governsHealth: t.governsHealth,
        });
    }
    return thresholds.length;
}

/** Persist a dispatch note to the ontology-backed store. Throws if the backend is unreachable. */
export async function saveDispatchNote(note: DispatchNoteRecord): Promise<DispatchNoteRecord> {
    return getRayfinClient().data.DispatchNote.create({
        turbineId: note.turbineId,
        siteId: note.siteId,
        status: note.status,
        powerKw: note.powerKw,
        note: note.note,
        author: note.author,
        createdAt: new Date(note.createdAt),
    }) as Promise<DispatchNoteRecord>;
}

/** Read the most recent dispatch notes (newest first). Throws if the backend is unreachable. */
export async function recentDispatchNotes(limit = 50): Promise<DispatchNoteRecord[]> {
    const rows = (await getRayfinClient()
        .data.DispatchNote.select(["id", "turbineId", "siteId", "status", "powerKw", "createdAt"])
        .orderBy({ createdAt: "desc" })
        .execute()) as DispatchNoteRecord[];
    return rows.slice(0, limit);
}

/** Persist a maintenance work order to the ontology-backed store. Throws if the backend is unreachable. */
export async function saveMaintenanceOrder(order: MaintenanceOrderRecord): Promise<MaintenanceOrderRecord> {
    return getRayfinClient().data.MaintenanceOrder.create({
        turbineId: order.turbineId,
        siteId: order.siteId,
        component: order.component,
        priority: order.priority,
        status: order.status,
        curtailPct: order.curtailPct,
        downtimeTicks: order.downtimeTicks,
        projectedDeltaKwt: order.projectedDeltaKwt,
        assignee: order.assignee,
        note: order.note,
        createdAt: new Date(order.createdAt),
    }) as Promise<MaintenanceOrderRecord>;
}

/** Read the most recent maintenance work orders (newest first). Throws if the backend is unreachable. */
export async function recentMaintenanceOrders(limit = 20): Promise<MaintenanceOrderRecord[]> {
    const rows = (await getRayfinClient()
        .data.MaintenanceOrder.select(["id", "turbineId", "siteId", "component", "priority", "status", "projectedDeltaKwt", "assignee", "createdAt"])
        .orderBy({ createdAt: "desc" })
        .execute()) as MaintenanceOrderRecord[];
    return rows.slice(0, limit);
}

/** List ontology sites registered in the backend. Throws if the backend is unreachable. */
export async function listOntologySites(): Promise<OntologySiteRecord[]> {
    return (await getRayfinClient()
        .data.WindSite.select(["id", "siteId", "name", "country", "latitude", "longitude", "capacityMw"])
        .execute()) as OntologySiteRecord[];
}

/**
 * Idempotently seed the ontology `Site` reference data. Best-effort: if the
 * backend is unreachable the caller should swallow the rejection.
 */
export async function ensureOntologySites(sites: OntologySiteRecord[]): Promise<void> {
    const existing = await listOntologySites();
    if (existing.length > 0) {
        return;
    }
    for (const site of sites) {
        await getRayfinClient().data.WindSite.create({
            siteId: site.siteId,
            name: site.name,
            country: site.country,
            latitude: site.latitude,
            longitude: site.longitude,
            capacityMw: site.capacityMw,
        });
    }
}

export interface OntologyAnswer {
    summary: string;
    queryText: string;
}

/** Minimal live-telemetry shape the answer engine reasons over. */
export interface TelemetryRow {
    id: string;
    siteId: string;
    siteName: string;
    powerKw: number;
    windMs: number;
    nacelleTempC: number;
    vibrationMmS: number;
    status: string;
}

/**
 * Pure question-answering engine grounded in live telemetry plus (optionally)
 * ontology sites and dispatch-note history. Used by both the online path
 * (`askOntology`) and the offline fallback so answers stay just as useful when
 * the backend is unreachable.
 */
export function answerQuestion(
    question: string,
    telemetry: TelemetryRow[],
    sites: OntologySiteRecord[] = [],
    notes: DispatchNoteRecord[] = [],
): string {
    const q = question.trim().toLowerCase();
    if (telemetry.length === 0) {
        return "No live telemetry is available right now.";
    }

    const fleetKw = telemetry.reduce((sum, t) => sum + t.powerKw, 0);
    const fleetMw = (fleetKw / 1000).toFixed(2);
    const alarms = telemetry.filter((t) => t.status === "alarm");
    const warnings = telemetry.filter((t) => t.status === "warning");

    const bySite = new Map<string, { name: string; kw: number; count: number }>();
    telemetry.forEach((t) => {
        const entry = bySite.get(t.siteId) ?? { name: t.siteName, kw: 0, count: 0 };
        entry.kw += t.powerKw;
        entry.count += 1;
        bySite.set(t.siteId, entry);
    });
    const siteRanked = [...bySite.values()].sort((a, b) => b.kw - a.kw);

    // Specific turbine mentioned by id.
    const turbineHit = telemetry.find((t) => q.includes(t.id.toLowerCase()));
    if (turbineHit) {
        return `${turbineHit.id} (${turbineHit.siteName}): ${turbineHit.powerKw.toLocaleString()} kW, wind ${turbineHit.windMs} m/s, nacelle ${turbineHit.nacelleTempC}°C, vibration ${turbineHit.vibrationMmS} mm/s — status ${turbineHit.status}.`;
    }

    // Specific site mentioned by name or id.
    const siteHit =
        sites.find((s) => q.includes(s.name.toLowerCase()) || q.includes(s.siteId.toLowerCase())) ??
        [...bySite.entries()]
            .map(([id, agg]) => ({ id, ...agg }))
            .find((s) => q.includes(s.name.toLowerCase()) || q.includes(s.id.toLowerCase()));
    if (siteHit) {
        const siteId = "siteId" in siteHit ? siteHit.siteId : siteHit.id;
        const name = siteHit.name;
        const agg = bySite.get(siteId);
        return agg
            ? `${name}: ${(agg.kw / 1000).toFixed(2)} MW from ${agg.count} turbine(s).`
            : `${name} is registered but has no live telemetry.`;
    }

    const asksTurbine = q.includes("turbine");

    if (q.includes("lowest") || q.includes("worst") || q.includes("least") || q.includes("min")) {
        if (asksTurbine) {
            const worst = [...telemetry].sort((a, b) => a.powerKw - b.powerKw)[0];
            return `Lowest-output turbine is ${worst.id} (${worst.siteName}) at ${worst.powerKw.toLocaleString()} kW.`;
        }
        const s = siteRanked[siteRanked.length - 1];
        return `Lowest-output site is ${s.name} at ${(s.kw / 1000).toFixed(2)} MW.`;
    }

    if (
        q.includes("highest") || q.includes("most") || q.includes("top") || q.includes("best") ||
        q.includes("max") || q.includes("which site") || q.includes("output") || q.includes("produc")
    ) {
        if (asksTurbine) {
            const best = [...telemetry].sort((a, b) => b.powerKw - a.powerKw)[0];
            return `Top-producing turbine is ${best.id} (${best.siteName}) at ${best.powerKw.toLocaleString()} kW.`;
        }
        const s = siteRanked[0];
        return `Highest-output site is ${s.name} at ${(s.kw / 1000).toFixed(2)} MW of ${fleetMw} MW fleet total (${bySite.size} sites).`;
    }

    if (q.includes("alarm") || q.includes("critical") || q.includes("fault")) {
        if (alarms.length === 0) {
            return `No turbines are in alarm. ${warnings.length} in warning.`;
        }
        const list = alarms.slice(0, 4).map((t) => `${t.id} (${t.siteName})`).join(", ");
        return `${alarms.length} turbine(s) in alarm: ${list}${alarms.length > 4 ? ", …" : ""}. ${warnings.length} in warning.`;
    }

    if (q.includes("warning")) {
        if (warnings.length === 0) {
            return "No turbines are in warning.";
        }
        const list = warnings.slice(0, 4).map((t) => `${t.id} (${t.siteName})`).join(", ");
        return `${warnings.length} turbine(s) in warning: ${list}${warnings.length > 4 ? ", …" : ""}.`;
    }

    if (q.includes("temp") || q.includes("hot") || q.includes("nacelle") || q.includes("overheat")) {
        const hot = [...telemetry].sort((a, b) => b.nacelleTempC - a.nacelleTempC)[0];
        return `Hottest nacelle is ${hot.id} (${hot.siteName}) at ${hot.nacelleTempC}°C.`;
    }

    if (q.includes("vibrat")) {
        const v = [...telemetry].sort((a, b) => b.vibrationMmS - a.vibrationMmS)[0];
        return `Highest vibration is ${v.id} (${v.siteName}) at ${v.vibrationMmS} mm/s.`;
    }

    if (q.includes("wind")) {
        const w = [...telemetry].sort((a, b) => b.windMs - a.windMs)[0];
        const avg = (telemetry.reduce((sum, t) => sum + t.windMs, 0) / telemetry.length).toFixed(1);
        return `Peak wind is ${w.windMs} m/s at ${w.id} (${w.siteName}); fleet average ${avg} m/s.`;
    }

    if (q.includes("note") || q.includes("dispatch") || q.includes("writeback") || q.includes("history")) {
        if (notes.length === 0) {
            return "No dispatch notes have been written back yet.";
        }
        const n = notes[0];
        return `${notes.length} dispatch note(s) on record. Latest: ${n.turbineId} (${n.status}, ${n.powerKw.toLocaleString()} kW).`;
    }

    if (q.includes("how many") || q.includes("count") || q.includes("total") || q.includes("number of")) {
        return `${telemetry.length} turbines across ${bySite.size} site(s), ${fleetMw} MW total — ${alarms.length} alarm, ${warnings.length} warning.`;
    }

    const top = siteRanked[0];
    return `Fleet output is ${fleetMw} MW across ${bySite.size} ontology site(s). Top site: ${top.name} (${(top.kw / 1000).toFixed(2)} MW). ${alarms.length} alarm, ${warnings.length} warning. ${notes.length} dispatch note(s) on record.`;
}

/**
 * Answer a question grounded in the ontology backend (registered sites +
 * dispatch-note history) combined with the live telemetry `context`.
 * Throws if the backend is unreachable so callers can fall back locally.
 */
export async function askOntology(
    question: string,
    context: Record<string, unknown>,
): Promise<OntologyAnswer> {
    const telemetry = (context.telemetry as TelemetryRow[] | undefined) ?? [];

    // Read both backend collections independently so one transient failure does
    // not knock the whole live link offline — degrade gracefully instead.
    const [sitesResult, notesResult] = await Promise.allSettled([listOntologySites(), recentDispatchNotes(50)]);
    const sites = sitesResult.status === "fulfilled" ? sitesResult.value : [];
    const notes = notesResult.status === "fulfilled" ? notesResult.value : [];

    // Only treat the link as down when BOTH backend reads fail, so the caller's
    // offline fallback is reserved for a genuinely unreachable backend.
    if (sitesResult.status === "rejected" && notesResult.status === "rejected") {
        throw sitesResult.reason instanceof Error ? sitesResult.reason : new Error("Ontology backend unreachable");
    }
    const degraded = sitesResult.status === "rejected" || notesResult.status === "rejected";

    return {
        summary: answerQuestion(question, telemetry, sites, notes),
        queryText: `Rayfin data API · WindSite(${sites.length}) + DispatchNote(${notes.length}) · telemetry(${telemetry.length})${degraded ? " · partial" : ""}`,
    };
}
