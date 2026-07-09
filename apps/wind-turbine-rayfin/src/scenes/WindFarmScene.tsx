import { useEffect, useRef } from "react";
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
            (ctx as CanvasRenderingContext2D & { roundRect: (x: number, y: number, w: number, h: number, r: number) => void }).roundRect(rx, ry, rw, rh, 16);
        } else {
            ctx.rect(rx, ry, rw, rh);
        }
        ctx.fillStyle = "rgba(6, 16, 31, 0.78)";
        ctx.fill();
        ctx.lineWidth = 3;
        ctx.strokeStyle = color;
        ctx.stroke();
        ctx.fillStyle = "#eefcff";
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

        const mapTexture = createMapTexture(sites as any);
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

        const labelSprites: THREE.Sprite[] = [];
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

            const labelText = site.name ?? site.country ?? "";
            if (labelText) {
                const sprite = createLabelSprite(labelText, SITE_COLORS[idx % SITE_COLORS.length]);
                sprite.position.set(projectLonToX(site.lon), LABEL_HEIGHT + (idx % 2) * 1.7, projectLatToZ(site.lat));
                scene.add(sprite);
                labelSprites.push(sprite);
            }
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
                    refs.blades.rotation.x += refs.spin;
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
                <button type="button" title="Reset view" onClick={() => { zoomRef.current = 0.62; panRef.current = { x: 0, z: 0 }; }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60">⟳</button>
            </div>
        </div>
    );
}
