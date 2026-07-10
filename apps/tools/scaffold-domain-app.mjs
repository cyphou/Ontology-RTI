//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Multi-domain shell scaffolder.
//
// Reuses an existing Rayfin app as a template to stamp out a starter app for
// another accelerator ontology domain, applying a domain manifest of token
// replacements (app name, project id, entity nouns, primary unit, titles).
//
// The pure functions below (applyManifest / buildTokenPairs) are unit-tested in
// scaffold-domain-app.test.mjs via `node --test`. The CLI clones the template
// directory (skipping build artifacts and dependencies) into a new app folder.
//
// Domain-specific 3D scene geometry, telemetry signal mapping, site coordinates
// and world outlines are intentionally NOT auto-translated — they are authored on
// top of the generated shell, which already carries the domain-correct nav,
// analytics, alerts, sites drill-down, operations, Ask IQ, thresholds and the
// shared services (ask routing, roles, history windows, Teams alerts).
//
// Usage:
//   node apps/tools/scaffold-domain-app.mjs <domainKey> [--template <appDir>] [--dry-run]
//   node apps/tools/scaffold-domain-app.mjs smart-building --dry-run

import { promises as fs } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

// --- Template descriptor (the source app the scaffolder clones from) ----------

export const TEMPLATE = {
    key: "wind-turbine",
    appDir: "wind-turbine-rayfin",
    projectId: "data-app",
    title: "Geo Wind Twin Command Center",
    entityPluralCap: "Turbines",
    entitySingularCap: "Turbine",
    entityPlural: "turbines",
    entitySingular: "turbine",
    unit: "kW",
    ontologyModel: "WindTurbine",
};

// --- Target domain descriptors -----------------------------------------------

export const DOMAINS = {
    "smart-building": {
        appDir: "smart-building-rayfin",
        projectId: "smart-building-app",
        title: "Geo Smart Building Twin Command Center",
        entityPluralCap: "Spaces",
        entitySingularCap: "Space",
        entityPlural: "spaces",
        entitySingular: "space",
        unit: "kWh",
        ontologyModel: "SmartBuilding",
    },
    "manufacturing-plant": {
        appDir: "manufacturing-plant-rayfin",
        projectId: "manufacturing-plant-app",
        title: "Geo Manufacturing Twin Command Center",
        entityPluralCap: "Lines",
        entitySingularCap: "Line",
        entityPlural: "lines",
        entitySingular: "line",
        unit: "units/h",
        ontologyModel: "ManufacturingPlant",
    },
    "it-asset": {
        appDir: "it-asset-rayfin",
        projectId: "it-asset-app",
        title: "Geo IT Asset Twin Command Center",
        entityPluralCap: "Assets",
        entitySingularCap: "Asset",
        entityPlural: "assets",
        entitySingular: "asset",
        unit: "%",
        ontologyModel: "ITAsset",
    },
    healthcare: {
        appDir: "healthcare-rayfin",
        projectId: "healthcare-app",
        title: "Geo Healthcare Twin Command Center",
        entityPluralCap: "Devices",
        entitySingularCap: "Device",
        entityPlural: "devices",
        entitySingular: "device",
        unit: "%",
        ontologyModel: "Healthcare",
    },
};

// --- Pure helpers (unit-tested) ----------------------------------------------

// Build the ordered [from, to] replacement pairs mapping template tokens onto a
// target domain. Pairs are sorted longest-first so a compound token (e.g.
// "wind-turbine-rayfin") is replaced before any substring it contains ("turbine").
export function buildTokenPairs(template, domain) {
    const pairs = [
        [template.appDir, domain.appDir],
        [template.title, domain.title],
        [template.ontologyModel, domain.ontologyModel],
        [template.projectId, domain.projectId],
        [template.entityPluralCap, domain.entityPluralCap],
        [template.entitySingularCap, domain.entitySingularCap],
        [template.entityPlural, domain.entityPlural],
        [template.entitySingular, domain.entitySingular],
        [template.unit, domain.unit],
    ].filter(([from, to]) => from && to && from !== to);
    return pairs.sort((a, b) => b[0].length - a[0].length);
}

// Apply ordered replacement pairs to a string. Each `from` is replaced globally.
export function applyManifest(text, pairs) {
    let out = text;
    for (const [from, to] of pairs) {
        out = out.split(from).join(to);
    }
    return out;
}

const TEXT_EXTENSIONS = new Set([
    ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".json", ".html", ".css",
    ".md", ".yml", ".yaml", ".txt", ".svg",
]);

// True when a file should be treated as text (token-substituted) rather than
// copied verbatim (e.g. images, fonts).
export function isTextFile(filePath) {
    return TEXT_EXTENSIONS.has(path.extname(filePath).toLowerCase());
}

const SKIP_SEGMENTS = new Set(["node_modules", "dist", ".git"]);

// True when a repo-relative path should be skipped entirely (deps/build output).
export function shouldSkip(relPath) {
    const segments = relPath.split(/[\\/]/);
    if (segments.some((s) => SKIP_SEGMENTS.has(s))) {
        return true;
    }
    if (relPath.endsWith(".tsbuildinfo") || relPath.endsWith("package-lock.json")) {
        return true;
    }
    if (segments.includes("rayfin") && segments.includes(".temp")) {
        return true;
    }
    if (segments.includes("rayfin") && segments.at(-1) === ".deployments.json") {
        return true;
    }
    return false;
}

// --- CLI ---------------------------------------------------------------------

async function walk(dir, base, out) {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
        const abs = path.join(dir, entry.name);
        const rel = path.relative(base, abs);
        if (shouldSkip(rel)) {
            continue;
        }
        if (entry.isDirectory()) {
            await walk(abs, base, out);
        } else {
            out.push(rel);
        }
    }
    return out;
}

async function main() {
    const args = process.argv.slice(2);
    const domainKey = args.find((a) => !a.startsWith("--"));
    const dryRun = args.includes("--dry-run");
    const templateArg = args.indexOf("--template");
    const templateDir = templateArg >= 0 ? args[templateArg + 1] : TEMPLATE.appDir;

    if (!domainKey || !DOMAINS[domainKey]) {
        console.error(`Usage: node scaffold-domain-app.mjs <domainKey> [--template <appDir>] [--dry-run]`);
        console.error(`Known domains: ${Object.keys(DOMAINS).join(", ")}`);
        process.exit(1);
    }

    const domain = DOMAINS[domainKey];
    const appsRoot = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
    const src = path.join(appsRoot, templateDir);
    const dest = path.join(appsRoot, domain.appDir);
    const pairs = buildTokenPairs(TEMPLATE, domain);

    const files = await walk(src, src, []);
    console.log(`Scaffolding "${domain.appDir}" from "${templateDir}" (${files.length} files)${dryRun ? " [dry-run]" : ""}`);

    if (dryRun) {
        files.slice(0, 20).forEach((f) => console.log(`  ${f}`));
        if (files.length > 20) {
            console.log(`  … and ${files.length - 20} more`);
        }
        console.log("Token map:");
        pairs.forEach(([from, to]) => console.log(`  "${from}" -> "${to}"`));
        return;
    }

    for (const rel of files) {
        const from = path.join(src, rel);
        const to = path.join(dest, applyManifest(rel, pairs));
        await fs.mkdir(path.dirname(to), { recursive: true });
        if (isTextFile(from)) {
            const text = await fs.readFile(from, "utf8");
            await fs.writeFile(to, applyManifest(text, pairs));
        } else {
            await fs.copyFile(from, to);
        }
    }
    console.log(`Done. Next: cd apps/${domain.appDir} && npm install && author the domain scene/telemetry.`);
}

// Only run the CLI when this module is the process entry point (not when it is
// imported by the test runner). pathToFileURL keeps this correct on Windows.
const isEntryPoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntryPoint) {
    main().catch((err) => {
        console.error(err);
        process.exit(1);
    });
}
