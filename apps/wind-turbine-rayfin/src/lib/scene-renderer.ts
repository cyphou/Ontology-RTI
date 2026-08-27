//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import * as THREE from "three";

// Shown inside the scene panel when WebGL cannot initialize at all, so the map/twin
// area degrades to a calm message instead of a blank box or a thrown error.
export const WEBGL_UNAVAILABLE_HTML = "<div style='padding:12px;color:#9aa3b2'>3D view unavailable in this environment. Data, alerts and dispatch remain available.</div>";

// Create a WebGL renderer that degrades gracefully in constrained hosts. Some
// environments (sandboxed Fabric portal iframes, headless/software GL) reject the
// default high-performance context or throw during construction; we retry with
// progressively simpler options and return null if none succeed. Returning null
// (instead of throwing) lets the caller show WEBGL_UNAVAILABLE_HTML rather than
// tripping the React scene error boundary.
export function createResilientRenderer(width: number, height: number): THREE.WebGLRenderer | null {
    const attempts: THREE.WebGLRendererParameters[] = [
        { antialias: true, powerPreference: "high-performance", failIfMajorPerformanceCaveat: false },
        { antialias: true, failIfMajorPerformanceCaveat: false },
        { antialias: false, failIfMajorPerformanceCaveat: false },
        { antialias: false },
    ];
    for (const options of attempts) {
        try {
            const renderer = new THREE.WebGLRenderer(options);
            renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
            renderer.setSize(width, height);
            return renderer;
        } catch {
            // Try the next, less demanding configuration.
        }
    }
    return null;
}
