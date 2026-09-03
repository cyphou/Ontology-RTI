//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { useEffect, useMemo, useRef } from "react";
import * as THREE from "three";
import { createResilientRenderer, WEBGL_UNAVAILABLE_HTML } from "@/lib/scene-renderer";
import { createWorldMapTexture } from "@/lib/world-map-texture";
import { SITE_COLORS, STATUS_COLORS, createSkyTexture } from "../App";

type PlantStatus = "healthy" | "warning" | "alarm";
type UnitTelemetry = {
    id: string;
    siteId: string;
    siteName: string;
    latitude: number;
    longitude: number;
    powerKw: number;
    status: PlantStatus;
};
type FleetSite = { id: string; name?: string; region?: string; lat: number; lon: number };

const GLOBE_RADIUS = 10;
const DEG2RAD = Math.PI / 180;

// Map a lon/lat to a point on the globe surface. Uses the same equirectangular
// convention as createMapTexture (seam at lon = 180), so pins land on their country.
function latLonToVec3(lat: number, lon: number, radius: number): THREE.Vector3 {
    const phi = (90 - lat) * DEG2RAD;
    const theta = (lon + 180) * DEG2RAD;
    return new THREE.Vector3(
        -radius * Math.sin(phi) * Math.cos(theta),
        radius * Math.cos(phi),
        radius * Math.sin(phi) * Math.sin(theta),
    );
}

// Camera-facing pill label drawn to a canvas texture, kept on top with depthTest:false.
function createLabelSprite(text: string, color: string): THREE.Sprite {
    const canvas = document.createElement("canvas");
    canvas.width = 512;
    canvas.height = 128;
    const ctx = canvas.getContext("2d");
    const map = new THREE.CanvasTexture(canvas);
    if (ctx) {
        ctx.font = "600 44px 'Segoe UI', system-ui, sans-serif";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        const textWidth = ctx.measureText(text).width;
        const pad = 28;
        const rw = Math.min(canvas.width - 8, textWidth + pad * 2);
        const rx = (canvas.width - rw) / 2;
        const ry = 30;
        const rh = 68;
        ctx.beginPath();
        if (typeof (ctx as unknown as { roundRect?: unknown }).roundRect === "function") {
            (ctx as CanvasRenderingContext2D & { roundRect: (x: number, y: number, w: number, h: number, r: number) => void }).roundRect(rx, ry, rw, rh, 18);
        } else {
            ctx.rect(rx, ry, rw, rh);
        }
        ctx.save();
        ctx.shadowColor = "rgba(0, 0, 0, 0.55)";
        ctx.shadowBlur = 16;
        ctx.shadowOffsetY = 4;
        ctx.fillStyle = "rgba(18, 22, 28, 0.86)";
        ctx.fill();
        ctx.restore();
        ctx.lineWidth = 2;
        ctx.strokeStyle = color;
        ctx.globalAlpha = 0.85;
        ctx.stroke();
        ctx.globalAlpha = 1;
        ctx.fillStyle = "#f3e7ce";
        ctx.fillText(text, canvas.width / 2, ry + rh / 2 + 1);
        map.colorSpace = THREE.SRGBColorSpace;
        map.needsUpdate = true;
    }
    const material = new THREE.SpriteMaterial({ map, transparent: true, depthTest: false, depthWrite: false });
    const sprite = new THREE.Sprite(material);
    sprite.scale.set(4.6, 1.15, 1);
    sprite.renderOrder = 999;
    return sprite;
}

type PinRefs = { group: THREE.Group; flame: THREE.Mesh; flameMat: THREE.MeshStandardMaterial; accentMat: THREE.MeshStandardMaterial };

// Worldwide fleet map rendered as an interactive 3D globe: refinery units are pins at
// their real lat/lon, colored by status. Drag to rotate, wheel to zoom, click a pin to
// open its digital twin. Falls back gracefully when WebGL is unavailable.
export default function GlobeFleetScene({
    turbines,
    sites,
    selectedId,
    dimmedIds,
    paused,
    onSelect,
}: {
    turbines: UnitTelemetry[];
    sites: FleetSite[];
    selectedId: string;
    dimmedIds: Set<string>;
    paused: boolean;
    onSelect: (id: string) => void;
}) {
    const hostRef = useRef<HTMLDivElement | null>(null);
    const pausedRef = useRef(paused);
    const selectedRef = useRef(selectedId);
    const dimmedRef = useRef(dimmedIds);
    const onSelectRef = useRef(onSelect);
    const turbinesRef = useRef(turbines);

    // Rebuild the scene only when the set/positions of units change — NOT on every
    // telemetry tick — so the user's rotation/zoom is never reset. Live status is
    // refreshed each frame from turbinesRef in the animation loop.
    const topology = useMemo(
        () => turbines.map((t) => `${t.id}@${t.latitude.toFixed(3)},${t.longitude.toFixed(3)}`).join("|"),
        [turbines],
    );
    const siteTopology = useMemo(
        () => sites.map((site) => `${site.id}@${site.lat.toFixed(3)},${site.lon.toFixed(3)}:${site.name ?? ""}`).join("|"),
        [sites],
    );

    useEffect(() => { pausedRef.current = paused; }, [paused]);
    useEffect(() => { selectedRef.current = selectedId; }, [selectedId]);
    useEffect(() => { dimmedRef.current = dimmedIds; }, [dimmedIds]);
    useEffect(() => { onSelectRef.current = onSelect; }, [onSelect]);
    useEffect(() => { turbinesRef.current = turbines; }, [turbines]);

    useEffect(() => {
        const host = hostRef.current;
        if (!host) {
            return;
        }

        const scene = new THREE.Scene();
        scene.background = createSkyTexture();

        const camera = new THREE.PerspectiveCamera(46, host.clientWidth / host.clientHeight, 0.1, 2000);
        let camDistance = 32;
        camera.position.set(0, 6, camDistance);
        camera.lookAt(0, 0, 0);

        const renderer = createResilientRenderer(host.clientWidth, host.clientHeight);
        if (!renderer) {
            host.innerHTML = WEBGL_UNAVAILABLE_HTML;
            return;
        }
        renderer.shadowMap.enabled = true;
        renderer.shadowMap.type = THREE.PCFSoftShadowMap;
        renderer.outputColorSpace = THREE.SRGBColorSpace;
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.15;
        host.innerHTML = "";
        host.appendChild(renderer.domElement);
        // Signal the globe is draggable and let touch drags rotate it (not scroll the page).
        renderer.domElement.style.cursor = "grab";
        renderer.domElement.style.touchAction = "none";

        scene.add(new THREE.AmbientLight(0xbdd3ff, 0.55));
        const sun = new THREE.DirectionalLight(0xfff2e0, 1.35);
        sun.position.set(30, 20, 30);
        scene.add(sun);
        const rim = new THREE.DirectionalLight(0x4fa3ff, 0.4);
        rim.position.set(-30, -6, -20);
        scene.add(rim);

        // Rotating group holds the globe and all pins/labels so they spin together.
        const globe = new THREE.Group();
        scene.add(globe);

        // Ocean base sphere.
        const ocean = new THREE.Mesh(
            new THREE.SphereGeometry(GLOBE_RADIUS, 64, 48),
            new THREE.MeshStandardMaterial({ color: "#0b2f4d", roughness: 0.85, metalness: 0.15, emissive: "#04121f", emissiveIntensity: 0.4 }),
        );
        globe.add(ocean);

        // Natural Earth country boundaries and coastlines, rendered locally to an
        // equirectangular texture that aligns to the real lat/lon refinery markers.
        const mapOverlay = new THREE.Mesh(
            new THREE.SphereGeometry(GLOBE_RADIUS * 1.002, 64, 48),
            new THREE.MeshBasicMaterial({ map: createWorldMapTexture(), depthWrite: false }),
        );
        globe.add(mapOverlay);

        // Faint graticule.
        const grat = new THREE.Mesh(
            new THREE.SphereGeometry(GLOBE_RADIUS * 1.004, 24, 16),
            new THREE.MeshBasicMaterial({ color: 0x2a557f, wireframe: true, transparent: true, opacity: 0.08 }),
        );
        globe.add(grat);

        // Atmosphere halo.
        const atmosphere = new THREE.Mesh(
            new THREE.SphereGeometry(GLOBE_RADIUS * 1.09, 48, 32),
            new THREE.MeshBasicMaterial({ color: 0x5fb0ff, transparent: true, opacity: 0.10, side: THREE.BackSide }),
        );
        globe.add(atmosphere);

        // Site labels at each refinery centroid.
        const labels: { sprite: THREE.Sprite; normal: THREE.Vector3 }[] = [];
        sites.forEach((site, idx) => {
            const text = site.name ?? site.region ?? "";
            if (!text) {
                return;
            }
            const pos = latLonToVec3(site.lat, site.lon, GLOBE_RADIUS * 1.32);
            const sprite = createLabelSprite(text, SITE_COLORS[idx % SITE_COLORS.length]);
            sprite.position.copy(pos);
            globe.add(sprite);
            labels.push({ sprite, normal: pos.clone().normalize() });
        });

        // Real 3D refinery markers standing on the globe surface at each unit's location.
        const raycaster = new THREE.Raycaster();
        const pointer = new THREE.Vector2();
        const byId = new Map<string, PinRefs>();
        const pickables: THREE.Mesh[] = [];
        const up = new THREE.Vector3(0, 1, 0);

        // Shared marker geometry/materials (built once) — a detailed compact refinery:
        // twin distillation columns with catwalks, a spherical LPG tank, storage tanks,
        // a cooling tower, a pipe rack, and a status-colored flare stack.
        const padGeo = new THREE.CylinderGeometry(0.22, 0.24, 0.05, 18);
        const colLowerGeo = new THREE.CylinderGeometry(0.055, 0.07, 0.34, 12);
        const colUpperGeo = new THREE.CylinderGeometry(0.04, 0.05, 0.2, 12);
        const domeGeo = new THREE.SphereGeometry(0.05, 12, 8, 0, Math.PI * 2, 0, Math.PI / 2);
        const col2Geo = new THREE.CylinderGeometry(0.045, 0.055, 0.26, 12);
        const dome2Geo = new THREE.SphereGeometry(0.045, 12, 8, 0, Math.PI * 2, 0, Math.PI / 2);
        const ringGeo = new THREE.TorusGeometry(0.052, 0.006, 6, 18);
        const sphereTankGeo = new THREE.SphereGeometry(0.06, 16, 12);
        const legGeo = new THREE.CylinderGeometry(0.008, 0.008, 0.09, 6);
        const tankGeo = new THREE.CylinderGeometry(0.07, 0.07, 0.09, 16);
        const tankTopGeo = new THREE.CylinderGeometry(0.05, 0.07, 0.03, 16);
        const coolGeo = new THREE.CylinderGeometry(0.06, 0.085, 0.16, 18, 1, true);
        const pipeGeo = new THREE.CylinderGeometry(0.008, 0.008, 0.34, 6);
        const pipeSupGeo = new THREE.CylinderGeometry(0.006, 0.006, 0.07, 5);
        const stackGeo = new THREE.CylinderGeometry(0.02, 0.028, 0.5, 8);
        const flameGeo = new THREE.ConeGeometry(0.05, 0.16, 10);
        const bandGeo = new THREE.TorusGeometry(0.066, 0.012, 8, 16);
        const pickGeo = new THREE.BoxGeometry(1.5, 2.5, 1.5);
        const steelMat = new THREE.MeshStandardMaterial({ color: "#c2ccd9", roughness: 0.4, metalness: 0.75 });
        const tankMat = new THREE.MeshStandardMaterial({ color: "#dde3ea", roughness: 0.5, metalness: 0.4 });
        const sphereMat = new THREE.MeshStandardMaterial({ color: "#cfd8e1", roughness: 0.35, metalness: 0.6 });
        const padMat = new THREE.MeshStandardMaterial({ color: "#3a4453", roughness: 0.8, metalness: 0.2 });
        const stackMat = new THREE.MeshStandardMaterial({ color: "#7e8794", roughness: 0.6, metalness: 0.5 });
        const pipeMat = new THREE.MeshStandardMaterial({ color: "#9aa6b4", roughness: 0.5, metalness: 0.55 });
        const coolMat = new THREE.MeshStandardMaterial({ color: "#b7c0cc", roughness: 0.72, metalness: 0.2, side: THREE.DoubleSide });
        const pickMat = new THREE.MeshBasicMaterial({ visible: false });

        turbines.forEach((t) => {
            const dir = latLonToVec3(t.latitude, t.longitude, 1).normalize();
            const color = STATUS_COLORS[t.status];
            const marker = new THREE.Group();

            const pad = new THREE.Mesh(padGeo, padMat); pad.position.y = 0.025; marker.add(pad);

            // Main distillation column with catwalk rings.
            const colLower = new THREE.Mesh(colLowerGeo, steelMat); colLower.position.set(0, 0.22, 0.03); colLower.castShadow = true; marker.add(colLower);
            const colUpper = new THREE.Mesh(colUpperGeo, steelMat); colUpper.position.set(0, 0.49, 0.03); marker.add(colUpper);
            const dome = new THREE.Mesh(domeGeo, steelMat); dome.position.set(0, 0.59, 0.03); marker.add(dome);
            [0.2, 0.34, 0.48].forEach((y) => { const r = new THREE.Mesh(ringGeo, stackMat); r.rotation.x = Math.PI / 2; r.position.set(0, y, 0.03); marker.add(r); });

            // Second, shorter column.
            const col2 = new THREE.Mesh(col2Geo, steelMat); col2.position.set(0.13, 0.18, -0.1); col2.castShadow = true; marker.add(col2);
            const dome2 = new THREE.Mesh(dome2Geo, steelMat); dome2.position.set(0.13, 0.31, -0.1); marker.add(dome2);

            // Spherical LPG pressure tank on four legs.
            const sphere = new THREE.Mesh(sphereTankGeo, sphereMat); sphere.position.set(-0.17, 0.13, -0.05); sphere.castShadow = true; marker.add(sphere);
            for (let k = 0; k < 4; k += 1) {
                const a = (k / 4) * Math.PI * 2 + Math.PI / 4;
                const leg = new THREE.Mesh(legGeo, stackMat);
                leg.position.set(-0.17 + Math.cos(a) * 0.045, 0.06, -0.05 + Math.sin(a) * 0.045);
                marker.add(leg);
            }

            // Two storage tanks with conical roofs.
            const mkTank = (x: number, z: number) => {
                const tk = new THREE.Mesh(tankGeo, tankMat); tk.position.set(x, 0.07, z); tk.castShadow = true; marker.add(tk);
                const top = new THREE.Mesh(tankTopGeo, tankMat); top.position.set(x, 0.125, z); marker.add(top);
            };
            mkTank(-0.12, 0.14); mkTank(0.06, 0.18);

            // Cooling tower.
            const cool = new THREE.Mesh(coolGeo, coolMat); cool.position.set(0.18, 0.11, 0.1); marker.add(cool);

            // Pipe rack linking the units.
            [0.05, 0.09].forEach((y) => { const pipe = new THREE.Mesh(pipeGeo, pipeMat); pipe.rotation.z = Math.PI / 2; pipe.position.set(0, y, 0.03); marker.add(pipe); });
            [-0.15, 0.15].forEach((x) => { const sup = new THREE.Mesh(pipeSupGeo, stackMat); sup.position.set(x, 0.05, 0.03); marker.add(sup); });

            // Flare stack + status-colored accent band and self-lit flame.
            const stack = new THREE.Mesh(stackGeo, stackMat); stack.position.set(0.2, 0.28, -0.17); marker.add(stack);
            const accentMat = new THREE.MeshStandardMaterial({ color, emissive: color, emissiveIntensity: 0.6, roughness: 0.4 });
            const band = new THREE.Mesh(bandGeo, accentMat); band.rotation.x = Math.PI / 2; band.position.set(0, 0.34, 0.03); marker.add(band);
            const flameMat = new THREE.MeshStandardMaterial({ color, emissive: color, emissiveIntensity: 1.2, roughness: 0.4, transparent: true, opacity: 0.9 });
            const flame = new THREE.Mesh(flameGeo, flameMat); flame.position.set(0.2, 0.6, -0.17); marker.add(flame);

            // Sit the base flush on the sphere, structure pointing outward.
            marker.quaternion.setFromUnitVectors(up, dir);
            marker.position.copy(dir.clone().multiplyScalar(GLOBE_RADIUS));
            globe.add(marker);

            // Invisible pick proxy so the whole marker is easy to click.
            const pick = new THREE.Mesh(pickGeo, pickMat);
            pick.position.copy(dir.clone().multiplyScalar(GLOBE_RADIUS + 0.85));
            pick.quaternion.setFromUnitVectors(up, dir);
            pick.userData.id = t.id;
            globe.add(pick);
            pickables.push(pick);

            byId.set(t.id, { group: marker, flame, flameMat, accentMat });
        });

        // Selection halo ring (re-parented onto the selected pin's location each frame).
        const haloMat = new THREE.MeshBasicMaterial({ color: 0x8ee6ff, transparent: true, opacity: 0.9, side: THREE.DoubleSide });
        const halo = new THREE.Mesh(new THREE.RingGeometry(1.25, 1.7, 32), haloMat);
        halo.visible = false;
        globe.add(halo);

        // ---- Interaction: drag to rotate, wheel to zoom, click to select -----
        let dragging = false;
        let lastX = 0;
        let lastY = 0;
        let moved = 0;
        let targetRotY = 0.4;
        let targetRotX = -0.15;

        const onPointerDown = (e: PointerEvent) => {
            dragging = true;
            moved = 0;
            lastX = e.clientX;
            lastY = e.clientY;
            renderer.domElement.style.cursor = "grabbing";
            renderer.domElement.setPointerCapture?.(e.pointerId);
        };
        const onPointerMove = (e: PointerEvent) => {
            if (!dragging) {
                return;
            }
            const dx = e.clientX - lastX;
            const dy = e.clientY - lastY;
            lastX = e.clientX;
            lastY = e.clientY;
            moved += Math.abs(dx) + Math.abs(dy);
            targetRotY += dx * 0.006;
            targetRotX = Math.max(-1.45, Math.min(1.45, targetRotX + dy * 0.006));
        };
        const onPointerUp = () => { dragging = false; renderer.domElement.style.cursor = "grab"; };
        const onWheel = (e: WheelEvent) => {
            e.preventDefault();
            camDistance = Math.max(16, Math.min(52, camDistance + Math.sign(e.deltaY) * 1.6));
        };
        const onClick = (e: MouseEvent) => {
            if (moved > 6) {
                return;
            }
            const rect = renderer.domElement.getBoundingClientRect();
            pointer.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
            pointer.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;
            raycaster.setFromCamera(pointer, camera);
            const hit = raycaster.intersectObjects(pickables, false)[0];
            if (hit) {
                const id = (hit.object.userData as { id?: string }).id;
                if (id) {
                    onSelectRef.current(id);
                }
            }
        };
        renderer.domElement.addEventListener("pointerdown", onPointerDown);
        renderer.domElement.addEventListener("pointermove", onPointerMove);
        window.addEventListener("pointerup", onPointerUp);
        renderer.domElement.addEventListener("wheel", onWheel, { passive: false });
        renderer.domElement.addEventListener("click", onClick);

        const onResize = () => {
            if (!host.clientWidth) {
                return;
            }
            camera.aspect = host.clientWidth / host.clientHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(host.clientWidth, host.clientHeight);
        };
        window.addEventListener("resize", onResize);

        let tick = 0;
        const camDir = new THREE.Vector3();
        renderer.setAnimationLoop(() => {
            tick += 0.016;
            // Keep the fleet view alive by orbiting slowly until the user drags it.
            if (!dragging && !pausedRef.current) {
                targetRotY += 0.0007;
            }
            globe.rotation.y += (targetRotY - globe.rotation.y) * 0.1;
            globe.rotation.x += (targetRotX - globe.rotation.x) * 0.1;
            camera.position.z += (camDistance - camera.position.z) * 0.1;
            camera.lookAt(0, 0, 0);

            // Flare flicker + selection pulse + dimming on the 3D refinery markers.
            const sel = selectedRef.current;
            const dim = dimmedRef.current;
            const latest = new Map(turbinesRef.current.map((t) => [t.id, t.status]));
            byId.forEach((ref, id) => {
                const isSel = id === sel;
                const isDim = dim.has(id);
                // Refresh status colour live without rebuilding the scene.
                const st = latest.get(id);
                if (st) {
                    const c = STATUS_COLORS[st];
                    ref.flameMat.color.set(c); ref.flameMat.emissive.set(c);
                    ref.accentMat.color.set(c); ref.accentMat.emissive.set(c);
                }
                const flicker = 1 + Math.sin(tick * 6 + ref.group.position.x * 2) * 0.28;
                ref.flame.scale.set(1, pausedRef.current ? 1 : flicker, 1);
                ref.flameMat.emissiveIntensity = (isSel ? 1.9 : 1.2) * (isDim ? 0.4 : 1);
                ref.flameMat.opacity = isDim ? 0.3 : 0.9;
                ref.accentMat.emissiveIntensity = isSel ? 1.0 + Math.sin(tick * 4) * 0.3 : (isDim ? 0.25 : 0.6);
                ref.group.scale.setScalar(isSel ? 4.0 : 3.0);
            });
            const selRef = sel ? byId.get(sel) : undefined;
            if (selRef) {
                halo.visible = true;
                const dir = selRef.group.position.clone().normalize();
                halo.position.copy(dir.clone().multiplyScalar(GLOBE_RADIUS + 0.05));
                halo.quaternion.setFromUnitVectors(new THREE.Vector3(0, 0, 1), dir);
            } else {
                halo.visible = false;
            }

            // Hide labels on the far side of the globe.
            camera.getWorldDirection(camDir);
            labels.forEach(({ sprite, normal }) => {
                const worldNormal = normal.clone().applyQuaternion(globe.quaternion);
                sprite.visible = worldNormal.dot(camera.position.clone().normalize()) > 0.15;
            });

            renderer.render(scene, camera);
        });

        return () => {
            renderer.setAnimationLoop(null);
            renderer.domElement.removeEventListener("pointerdown", onPointerDown);
            renderer.domElement.removeEventListener("pointermove", onPointerMove);
            window.removeEventListener("pointerup", onPointerUp);
            renderer.domElement.removeEventListener("wheel", onWheel);
            renderer.domElement.removeEventListener("click", onClick);
            window.removeEventListener("resize", onResize);
            renderer.dispose();
            scene.traverse((obj) => {
                if (obj instanceof THREE.Mesh) {
                    obj.geometry.dispose();
                    const m = obj.material;
                    if (Array.isArray(m)) {
                        m.forEach((mm) => mm.dispose());
                    } else {
                        m.dispose();
                    }
                }
            });
            if (host.contains(renderer.domElement)) {
                host.removeChild(renderer.domElement);
            }
        };
    }, [topology, siteTopology]);

    return <div ref={hostRef} className="h-full w-full" />;
}
