import { useEffect, useRef, useState } from "react";
import * as THREE from "three";
import {
    SITE_COLORS,
    STATUS_COLORS,
    createBladeGeometry,
    createMapTexture,
    createOceanTexture,
    createSkyTexture,
    projectLatToZ,
    projectLonToX,
    seededRand,
} from "../App";

type TurbineStatus = "healthy" | "warning" | "alarm";

type TurbineTelemetry = {
    id: string;
    x: number;
    z: number;
    powerKw: number;
    windMs: number;
    status: TurbineStatus;
};

type WindSite = {
    lon: number;
    lat: number;
    name?: string;
    country?: string;
};

// Camera-facing pill label drawn to a canvas texture. depthTest:false + a high
// renderOrder keep it on top of the scene, so site labels are never hidden behind
// the tall turbine towers. Positioned in world space, it pans/zooms with the map.
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
        // Soft drop shadow lifts the pill off the scene; a thin accent border and
        // a faint top sheen give it a cleaner, glassier read than a heavy outline.
        ctx.save();
        ctx.shadowColor = "rgba(0, 0, 0, 0.55)";
        ctx.shadowBlur = 16;
        ctx.shadowOffsetY = 4;
        ctx.fillStyle = "rgba(18, 22, 28, 0.86)";
        ctx.fill();
        ctx.restore();
        const sheen = ctx.createLinearGradient(0, ry, 0, ry + rh);
        sheen.addColorStop(0, "rgba(255, 255, 255, 0.08)");
        sheen.addColorStop(0.5, "rgba(255, 255, 255, 0)");
        ctx.fillStyle = sheen;
        ctx.fill();
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
    sprite.scale.set(9.5, 2.4, 1);
    sprite.renderOrder = 999;
    return sprite;
}

// World height for the floating site labels — just above the turbine blade tips.
const LABEL_HEIGHT = 11;

type TurbineRenderRefs = {
    blades: THREE.Group;
    nacelleMat: THREE.MeshStandardMaterial;
    ringMat: THREE.MeshBasicMaterial;
    ring: THREE.Mesh;
    wakeMat: THREE.MeshBasicMaterial;
    wake: THREE.Mesh;
    spin: number;
};

type SceneState = {
    byId: Map<string, TurbineRenderRefs>;
    cleanup: () => void;
};

export default function WindFarmScene({
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
    type LightingMode = "day" | "night";
    type QualityTier = "low" | "med" | "high";

    const hostRef = useRef<HTMLDivElement | null>(null);
    const sceneRef = useRef<SceneState | null>(null);
    const pausedRef = useRef(paused);
    const zoomRef = useRef(0.62);
    const zoomTargetRef = useRef(0.62);
    const tiltDegRef = useRef(40);
    // Default view is centred on the fleet centroid (most sites sit in the northern
    // hemisphere → negative Z), not the map origin, so the farm sits mid-frame.
    const panRef = useRef<{ x: number; z: number }>({ x: -4, z: -9 });
    const autoQualityRef = useRef(true);
    const lightingMode: LightingMode = "day";
    const [qualityTier, setQualityTier] = useState<QualityTier>("high");
    const autoQuality = true;
    const [, setFps] = useState(60);

    const ZOOM_MIN = 0.4;
    const ZOOM_MAX = 1.8;
    const TILT_MIN = 18;
    const TILT_MAX = 56;

    useEffect(() => {
        pausedRef.current = paused;
    }, [paused]);

    useEffect(() => {
        autoQualityRef.current = autoQuality;
    }, [autoQuality]);

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
        const skyTexture = createSkyTexture(lightingMode);
        scene.background = skyTexture;
        // Softer, closer atmospheric fade so distant water melts into the horizon
        // haze without ever swallowing the (much nearer) turbines.
        scene.fog = new THREE.Fog(lightingMode === "day" ? "#2b2822" : "#14161b", 150, 360);

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
        renderer.toneMappingExposure = lightingMode === "day" ? 1.2 : 0.96;
        host.innerHTML = "";
        host.appendChild(renderer.domElement);

        const pmrem = new THREE.PMREMGenerator(renderer);
        const envRT = pmrem.fromEquirectangular(skyTexture);
        scene.environment = envRT.texture;
        pmrem.dispose();

        // Balanced three-point rig: a clean warm key, a cool cyan rim for edge
        // separation (echoes the app's dark-cyan theme), and a sky/ground hemi
        // fill for a richer gradient across the towers.
        const ambient = new THREE.AmbientLight(lightingMode === "day" ? 0xeadfca : 0xb8a88e, lightingMode === "day" ? 0.3 : 0.24);
        const sun = new THREE.DirectionalLight(lightingMode === "day" ? 0xf4ede0 : 0x9ea7b4, lightingMode === "day" ? 1.24 : 0.72);
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
        const rim = new THREE.DirectionalLight(lightingMode === "day" ? 0x8fbdd6 : 0x596271, lightingMode === "day" ? 0.36 : 0.16);
        rim.position.set(-24, 18, -24);
        const hemi = new THREE.HemisphereLight(lightingMode === "day" ? 0xdfcaa4 : 0x8f7f66, 0x121620, lightingMode === "day" ? 0.4 : 0.26);
        scene.add(ambient, sun, rim, hemi);

        const oceanTexture = createOceanTexture();
        const terrain = new THREE.Mesh(
            new THREE.PlaneGeometry(200, 120, 60, 36),
            // Lower roughness + a touch of metalness lets the PMREM sky glaze the
            // water for a softer, richer shimmer instead of a flat matte plane.
            new THREE.MeshStandardMaterial({ map: oceanTexture, roughness: 0.56, metalness: 0.24, envMapIntensity: 0.7 })
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

        const mapTexture = createMapTexture(sites);
        const mapPlane = new THREE.Mesh(
            new THREE.PlaneGeometry(92, 46, 1, 1),
            new THREE.MeshBasicMaterial({ map: mapTexture, transparent: true, depthWrite: false })
        );
        mapPlane.rotation.x = -Math.PI / 2;
        mapPlane.position.y = 0.06;
        scene.add(mapPlane);

        const grid = new THREE.GridHelper(92, 24, 0x4f4655, 0x4f4655);
        grid.scale.z = 46 / 92; // constrain the square grid to the 92 x 46 map footprint
        grid.position.y = 0.02;
        grid.material.transparent = true;
        grid.material.opacity = 0.12;
        scene.add(grid);

        // Fine-grain wind flow particles to reinforce directionality in the scene.
        const flowCount = qualityTier === "low" ? 320 : qualityTier === "med" ? 700 : 1300;
        const flowPositions = new Float32Array(flowCount * 3);
        const flowVel = new Float32Array(flowCount);
        for (let i = 0; i < flowCount; i += 1) {
            const i3 = i * 3;
            flowPositions[i3] = -46 + seededRand(100 + i) * 92;
            flowPositions[i3 + 1] = 0.42 + seededRand(300 + i) * 2.2;
            flowPositions[i3 + 2] = -23 + seededRand(500 + i) * 46;
            flowVel[i] = 0.36 + seededRand(700 + i) * 0.54;
        }
        const flowGeo = new THREE.BufferGeometry();
        flowGeo.setAttribute("position", new THREE.BufferAttribute(flowPositions, 3));
        const flowMat = new THREE.PointsMaterial({
            color: "#a4afbb",
            size: 0.08,
            transparent: true,
            opacity: 0.3,
            depthWrite: false,
            blending: THREE.AdditiveBlending,
        });
        const flowPoints = new THREE.Points(flowGeo, flowMat);
        flowPoints.position.y = 0.06;
        scene.add(flowPoints);

        const labelSprites: THREE.Sprite[] = [];
        const siteBeacons: THREE.Mesh[] = [];
        sites.forEach((site, idx) => {
            const marker = new THREE.Mesh(
                new THREE.CylinderGeometry(0.24, 0.24, 0.9, 14),
                new THREE.MeshStandardMaterial({
                    color: SITE_COLORS[idx % SITE_COLORS.length],
                    emissive: SITE_COLORS[idx % SITE_COLORS.length],
                    emissiveIntensity: 0.42,
                    roughness: 0.4,
                    metalness: 0.3,
                    envMapIntensity: 1.1,
                })
            );
            marker.position.set(projectLonToX(site.lon), 0.45, projectLatToZ(site.lat));
            scene.add(marker);

            // Single clean additive halo (no stacked rings) keeps the marker
            // legible without adding visual noise across dense clusters.
            const glow = new THREE.Mesh(
                new THREE.RingGeometry(0.52, 0.98, 32),
                new THREE.MeshBasicMaterial({
                    color: SITE_COLORS[idx % SITE_COLORS.length],
                    transparent: true,
                    opacity: 0.4,
                    depthWrite: false,
                    blending: THREE.AdditiveBlending,
                })
            );
            glow.rotation.x = -Math.PI / 2;
            glow.position.set(projectLonToX(site.lon), 0.07, projectLatToZ(site.lat));
            scene.add(glow);

            const beacon = new THREE.Mesh(
                new THREE.CylinderGeometry(0.055, 0.055, 3.8, 16, 1, true),
                new THREE.MeshBasicMaterial({ color: SITE_COLORS[idx % SITE_COLORS.length], transparent: true, opacity: 0.2, side: THREE.DoubleSide, depthWrite: false, blending: THREE.AdditiveBlending })
            );
            beacon.position.set(projectLonToX(site.lon), 1.95, projectLatToZ(site.lat));
            scene.add(beacon);
            siteBeacons.push(beacon);

            const labelText = site.name ?? site.country ?? "";
            if (labelText) {
                const sprite = createLabelSprite(labelText, SITE_COLORS[idx % SITE_COLORS.length]);
                sprite.position.set(projectLonToX(site.lon), LABEL_HEIGHT + (idx % 2) * 1.7, projectLatToZ(site.lat));
                scene.add(sprite);
                labelSprites.push(sprite);
            }
        });

        const byId = new Map<string, TurbineRenderRefs>();
        const pickables: THREE.Object3D[] = [];
        const raycaster = new THREE.Raycaster();
        const pointer = new THREE.Vector2();

        const hubY = 7.4;
        const towerGeo = new THREE.CylinderGeometry(0.12, 0.4, 7.2, 24);
        const bladeGeo = createBladeGeometry();
        const baseGeo = new THREE.CylinderGeometry(0.62, 0.78, 0.3, 24);
        const nacelleGeo = new THREE.BoxGeometry(1.7, 0.55, 0.6);
        const spinnerGeo = new THREE.ConeGeometry(0.26, 0.6, 16);

        const towerMat = new THREE.MeshStandardMaterial({ color: "#e0e7ef", roughness: 0.28, metalness: 0.6, envMapIntensity: 1.25 });
        const bladeMat = new THREE.MeshStandardMaterial({ color: "#f7f9fd", roughness: 0.3, metalness: 0.14, envMapIntensity: 1.1 });
        const baseMat = new THREE.MeshStandardMaterial({ color: "#7f8b9a", roughness: 0.66, metalness: 0.24, envMapIntensity: 0.9 });
        const nacelleHousingMat = new THREE.MeshStandardMaterial({ color: "#e9eef4", roughness: 0.36, metalness: 0.54, envMapIntensity: 1.2 });
        const hubMat = new THREE.MeshStandardMaterial({ color: "#cfd8e2", roughness: 0.32, metalness: 0.58, envMapIntensity: 1.2 });

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

            // Lightweight wake ribbon behind turbines to communicate wind intensity.
            const wakeMat = new THREE.MeshBasicMaterial({
                color: 0xd6bc7f,
                transparent: true,
                opacity: 0.08,
                side: THREE.DoubleSide,
                depthWrite: false,
            });
            const wake = new THREE.Mesh(new THREE.PlaneGeometry(2.8, 0.62), wakeMat);
            wake.rotation.x = -Math.PI / 2;
            wake.rotation.z = (seededRand(t.id.length * 3.7) - 0.5) * 0.35;
            wake.position.set(-2.1, 0.085, 0);
            group.add(wake);

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
                wakeMat,
                wake,
                spin: 0.08 + seededRand(t.id.length + t.powerKw * 0.001) * 0.12,
            });
        });

        // Track drag state so a pan gesture never selects a turbine, and so the
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
                const obj = hits[0].object;
                const id = obj.userData.turbineId as string;
                if (id) {
                    onSelect(id);
                }
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
                renderer.domElement.style.cursor = "grab";
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
            // Normalize wheel/trackpad delta so zoom speed stays predictable
            // across mice, touchpads, and browser delta modes.
            const raw = event.deltaMode === 1
                ? event.deltaY * 16
                : event.deltaMode === 2
                    ? event.deltaY * 120
                    : event.deltaY;
            const norm = Math.sign(raw) * Math.min(140, Math.abs(raw));

            if (event.shiftKey) {
                tiltDegRef.current = THREE.MathUtils.clamp(tiltDegRef.current + norm * 0.02, TILT_MIN, TILT_MAX);
                return;
            }
            zoomTargetRef.current = THREE.MathUtils.clamp(zoomTargetRef.current + norm * 0.0009, ZOOM_MIN, ZOOM_MAX);
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
        let fpsFrames = 0;
        let fpsSampleStart = performance.now();
        let lastAutoAdjustAt = performance.now();
        const animate = () => {
            if (!pausedRef.current) {
                tick += 0.03;
                byId.forEach((refs) => {
                    refs.blades.rotation.x += refs.spin;
                });
            }

            const flowPos = flowGeo.getAttribute("position") as THREE.BufferAttribute;
            for (let i = 0; i < flowCount; i += 1) {
                let x = flowPos.getX(i) + flowVel[i] * 0.02;
                let y = flowPos.getY(i) + Math.sin((tick + i) * 0.04) * 0.002;
                if (x > 46) {
                    x = -46;
                    y = 0.42 + seededRand(900 + i + tick) * 2.2;
                    flowPos.setZ(i, -23 + seededRand(1200 + i + tick) * 46);
                }
                flowPos.setX(i, x);
                flowPos.setY(i, y);
            }
            flowPos.needsUpdate = true;

            siteBeacons.forEach((b, idx) => {
                const pulse = 0.84 + (Math.sin(tick * 1.7 + idx * 0.9) + 1) * 0.16;
                b.scale.y = pulse;
                const mat = b.material as THREE.MeshBasicMaterial;
                const base = qualityTier === "low" ? 0.09 : qualityTier === "med" ? 0.12 : 0.15;
                mat.opacity = base + pulse * 0.16;
            });

            oceanTexture.offset.x = tick * 0.0015;
            oceanTexture.offset.y = Math.sin(tick * 0.05) * 0.01;

            // Damp toward target zoom to avoid jumpy wheel behavior.
            zoomRef.current = THREE.MathUtils.lerp(zoomRef.current, zoomTargetRef.current, 0.16);
            const z = zoomRef.current;
            const px = panRef.current.x;
            const pz = panRef.current.z;

            const orbitX = (26 + Math.sin(tick * 0.17) * 9) * z;
            const orbitZ = (61 + Math.cos(tick * 0.13) * 5) * z;
            const planarDist = Math.sqrt(orbitX * orbitX + orbitZ * orbitZ);
            const tiltRad = THREE.MathUtils.degToRad(tiltDegRef.current);

            camera.position.x = orbitX + px;
            camera.position.y = Math.max(8, Math.tan(tiltRad) * planarDist);
            camera.position.z = orbitZ + pz;
            camera.lookAt(px, 0, pz);
            renderer.render(scene, camera);

            fpsFrames += 1;
            const now = performance.now();
            const sampleElapsed = now - fpsSampleStart;
            if (sampleElapsed >= 500) {
                const measuredFps = Math.round((fpsFrames * 1000) / sampleElapsed);
                setFps(measuredFps);
                fpsFrames = 0;
                fpsSampleStart = now;

                if (autoQualityRef.current && now - lastAutoAdjustAt > 4200) {
                    if (measuredFps < 28) {
                        setQualityTier((prev) => (prev === "high" ? "med" : "low"));
                        lastAutoAdjustAt = now;
                    } else if (measuredFps > 52) {
                        setQualityTier((prev) => (prev === "low" ? "med" : "high"));
                        lastAutoAdjustAt = now;
                    }
                }
            }

            // Screen-space label de-confliction: nearest label wins; hide any whose
            // projected box overlaps a label already shown this frame so labels never stack.
            const shownBoxes: { x: number; y: number; hw: number; hh: number }[] = [];
            const camRight = new THREE.Vector3().setFromMatrixColumn(camera.matrixWorld, 0);
            const camUp = new THREE.Vector3().setFromMatrixColumn(camera.matrixWorld, 1);
            labelSprites
                .map((s) => ({ s, d: camera.position.distanceToSquared(s.position) }))
                .sort((a, b) => a.d - b.d)
                .forEach(({ s }) => {
                    const c = s.position.clone().project(camera);
                    const eX = s.position.clone().addScaledVector(camRight, s.scale.x / 2).project(camera);
                    const eY = s.position.clone().addScaledVector(camUp, s.scale.y / 2).project(camera);
                    const hw = Math.abs(eX.x - c.x);
                    const hh = Math.abs(eY.y - c.y);
                    const overlaps = shownBoxes.some((b) => Math.abs(b.x - c.x) < b.hw + hw && Math.abs(b.y - c.y) < b.hh + hh);
                    if (overlaps) {
                        s.visible = false;
                    } else {
                        s.visible = true;
                        shownBoxes.push({ x: c.x, y: c.y, hw, hh });
                    }
                });
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
                if (obj instanceof THREE.Mesh || obj instanceof THREE.Points) {
                    obj.geometry.dispose();
                    const material = Array.isArray(obj.material) ? obj.material : [obj.material];
                    material.forEach((m) => m.dispose());
                }
            });
            mapTexture.dispose();
            oceanTexture.dispose();
            skyTexture.dispose();
            envRT.dispose();
            labelSprites.forEach((s) => {
                s.material.map?.dispose();
                s.material.dispose();
            });
        };

        sceneRef.current = {
            byId,
            cleanup,
        };

        return () => {
            sceneRef.current = null;
            cleanup();
        };
    }, [onSelect, sites, lightingMode, qualityTier]);

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

            const wakePower = dimmed ? 0.03 : Math.min(0.34, Math.max(0, (t.windMs - 7) * 0.03));
            meshRefs.wakeMat.opacity = wakePower;
            meshRefs.wake.scale.x = 0.86 + Math.min(2.2, t.windMs / 7.2);
            meshRefs.wake.visible = t.windMs >= 8;
        });
    }, [selectedId, turbines, dimmedIds]);

    return (
        <div className="relative h-full w-full">
            <div ref={hostRef} className="h-full w-full" />
        </div>
    );
}
