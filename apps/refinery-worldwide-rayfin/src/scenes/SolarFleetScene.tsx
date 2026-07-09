import { useEffect, useRef } from "react";
import * as THREE from "three";
import {
    SITE_COLORS,
    STATUS_COLORS,
    createMapTexture,
    createOceanTexture,
    createSkyTexture,
    seededRand,
} from "../App";

type PlantStatus = "healthy" | "warning" | "alarm";
type PlantTelemetry = {
    id: string;
    x: number;
    z: number;
    status: PlantStatus;
    powerKw: number;
    irradianceWm2: number;
    moduleTempC: number;
    inverterLoadPct: number;
};
type SolarPlantSite = { lon: number; lat: number };
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
export default function SolarFleetScene({
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
    const panRef = useRef<{ x: number; z: number }>({ x: 0, z: 0 });

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

        // Track drag state so a pan gesture never selects a unit, and so the
        // click handler can tell a genuine click from the end of a drag.
        let dragging = false;
        let dragMoved = false;
        let lastPointerX = 0;
        let lastPointerY = 0;
        renderer.domElement.style.cursor = "grab";
        renderer.domElement.style.touchAction = "none";

        const onClick = (event: MouseEvent) => {
            if (dragMoved) {
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

        // Drag anywhere on the canvas to pan the map across the ocean plane. The
        // pan offset shifts both the camera and its look-at target, so zoom and
        // the gentle auto-orbit keep working on top of it.
        const onPointerDown = (event: PointerEvent) => {
            dragging = true;
            dragMoved = false;
            lastPointerX = event.clientX;
            lastPointerY = event.clientY;
            renderer.domElement.style.cursor = "grabbing";
            renderer.domElement.setPointerCapture?.(event.pointerId);
        };
        const onPointerMove = (event: PointerEvent) => {
            if (!dragging) {
                return;
            }
            const dx = event.clientX - lastPointerX;
            const dy = event.clientY - lastPointerY;
            if (Math.abs(dx) + Math.abs(dy) > 3) {
                dragMoved = true;
            }
            lastPointerX = event.clientX;
            lastPointerY = event.clientY;
            const factor = 0.11 * zoomRef.current;
            panRef.current.x = THREE.MathUtils.clamp(panRef.current.x - dx * factor, -70, 70);
            panRef.current.z = THREE.MathUtils.clamp(panRef.current.z - dy * factor, -46, 46);
        };
        const onPointerUp = (event: PointerEvent) => {
            dragging = false;
            renderer.domElement.style.cursor = "grab";
            renderer.domElement.releasePointerCapture?.(event.pointerId);
        };
        renderer.domElement.addEventListener("pointerdown", onPointerDown);
        renderer.domElement.addEventListener("pointermove", onPointerMove);
        renderer.domElement.addEventListener("pointerup", onPointerUp);
        renderer.domElement.addEventListener("pointerleave", onPointerUp);

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
            const px = panRef.current.x;
            const pz = panRef.current.z;
            camera.position.x = (26 + Math.sin(tick * 0.17) * 9) * z + px;
            camera.position.y = 36 * z;
            camera.position.z = (61 + Math.cos(tick * 0.13) * 5) * z + pz;
            camera.lookAt(px, 0, pz);
            renderer.render(scene, camera);
        };
        renderer.setAnimationLoop(animate);

        const cleanup = () => {
            renderer.setAnimationLoop(null);
            renderer.domElement.removeEventListener("click", onClick);
            renderer.domElement.removeEventListener("pointerdown", onPointerDown);
            renderer.domElement.removeEventListener("pointermove", onPointerMove);
            renderer.domElement.removeEventListener("pointerup", onPointerUp);
            renderer.domElement.removeEventListener("pointerleave", onPointerUp);
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
                <button type="button" title="Reset view" onClick={() => { zoomRef.current = 0.62; panRef.current = { x: 0, z: 0 }; }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60">⟳</button>
            </div>
        </div>
    );
}