//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Unit tests for the multi-domain shell scaffolder pure logic.
// Run: node --test apps/tools/

import { test } from "node:test";
import assert from "node:assert/strict";
import {
    TEMPLATE,
    DOMAINS,
    buildTokenPairs,
    applyManifest,
    isTextFile,
    shouldSkip,
} from "./scaffold-domain-app.mjs";

test("buildTokenPairs orders compound tokens before their substrings", () => {
    const pairs = buildTokenPairs(TEMPLATE, DOMAINS["smart-building"]);
    const idxApp = pairs.findIndex(([from]) => from === "wind-turbine-rayfin");
    const idxWord = pairs.findIndex(([from]) => from === "turbine");
    assert.ok(idxApp >= 0 && idxWord >= 0);
    assert.ok(idxApp < idxWord, "compound app dir must be replaced before the bare noun");
});

test("applyManifest rewrites the app dir without corrupting it via the bare noun", () => {
    const pairs = buildTokenPairs(TEMPLATE, DOMAINS["smart-building"]);
    assert.equal(applyManifest("wind-turbine-rayfin", pairs), "smart-building-rayfin");
});

test("applyManifest rewrites titles, nouns and units for a domain", () => {
    const pairs = buildTokenPairs(TEMPLATE, DOMAINS["manufacturing-plant"]);
    assert.equal(applyManifest("Geo Wind Twin Command Center", pairs), "Geo Manufacturing Twin Command Center");
    assert.equal(applyManifest("Find turbine by id", pairs), "Find line by id");
    assert.equal(applyManifest("Turbines", pairs), "Lines");
    assert.equal(applyManifest("120 kW", pairs), "120 units/h");
    assert.equal(applyManifest("VITE_LIVE_TELEMETRY_MODEL WindTurbine", pairs), "VITE_LIVE_TELEMETRY_MODEL ManufacturingPlant");
});

test("applyManifest is a no-op when there is nothing to replace", () => {
    const pairs = buildTokenPairs(TEMPLATE, DOMAINS.healthcare);
    assert.equal(applyManifest("plain unrelated text", pairs), "plain unrelated text");
});

test("every known domain produces a non-empty, self-consistent token map", () => {
    for (const [key, domain] of Object.entries(DOMAINS)) {
        const pairs = buildTokenPairs(TEMPLATE, domain);
        assert.ok(pairs.length > 0, `${key} should have replacements`);
        // No pair maps a token onto itself.
        assert.ok(pairs.every(([from, to]) => from !== to), `${key} pairs must all change something`);
        // Descending length ordering holds.
        for (let i = 1; i < pairs.length; i += 1) {
            assert.ok(pairs[i - 1][0].length >= pairs[i][0].length, `${key} pairs must be longest-first`);
        }
    }
});

test("isTextFile recognises source/text but not binary assets", () => {
    assert.equal(isTextFile("src/App.tsx"), true);
    assert.equal(isTextFile("rayfin/rayfin.yml"), true);
    assert.equal(isTextFile("assets/logo.png"), false);
});

test("shouldSkip excludes deps, build output and generated deployment state", () => {
    assert.equal(shouldSkip("node_modules/react/index.js"), true);
    assert.equal(shouldSkip("dist/assets/index.js"), true);
    assert.equal(shouldSkip("rayfin/.temp/dab-config.json"), true);
    assert.equal(shouldSkip("rayfin/.deployments.json"), true);
    assert.equal(shouldSkip("src/App.tsx"), false);
    assert.equal(shouldSkip("rayfin/rayfin.yml"), false);
});
