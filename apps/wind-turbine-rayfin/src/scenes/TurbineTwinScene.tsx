import { useEffect, useRef } from "react";
import * as THREE from "three";
import { STATUS_COLORS, TWIN_COMPONENT_DEVICES, TWIN_PARTS, createBladeGeometry, createSkyTexture, signalColor, type TwinDeviceKey, type TwinDeviceNode } from "../App";

export type TwinPartKey = "rotor" | "nacelle" | "drivetrain" | "base";

type TurbineStatus = "healthy" | "warning" | "alarm";

type TurbineTelemetry = {
    windMs: number;
    nacelleTempC: number;
    vibrationMmS: number;
    powerKw: number;
    status: TurbineStatus;
};

export default function TurbineTwinScene({
    turbine,
    paused,
    onPartFocusChange,
    onDeviceFocusChange,
    deviceGraph,
}: {
    turbine: TurbineTelemetry;
    paused: boolean;
    onPartFocusChange?: (part: TwinPartKey | null) => void;
    onDeviceFocusChange?: (device: TwinDeviceKey | null) => void;
    deviceGraph?: Record<TwinPartKey, TwinDeviceNode[]>;
}) {
    const hostRef = useRef<HTMLDivElement | null>(null);
    const pausedRef = useRef(paused);
    const turbineRef = useRef(turbine);
    const zoomRef = useRef(1);
    const colorApiRef = useRef<((c: string) => void) | null>(null);
    const labelRefs = useRef<Array<HTMLDivElement | null>>([]);
    const focusedPartRef = useRef<TwinPartKey | null>(null);
    const focusedDeviceRef = useRef<TwinDeviceKey | null>(null);
    const deviceGraphRef = useRef<Record<TwinPartKey, TwinDeviceNode[]>>(deviceGraph ?? TWIN_COMPONENT_DEVICES);

    useEffect(() => { pausedRef.current = paused; }, [paused]);
    useEffect(() => { turbineRef.current = turbine; }, [turbine]);
    useEffect(() => { deviceGraphRef.current = deviceGraph ?? TWIN_COMPONENT_DEVICES; }, [deviceGraph]);

    useEffect(() => {
        focusedPartRef.current = null;
        focusedDeviceRef.current = null;
        onPartFocusChange?.(null);
        onDeviceFocusChange?.(null);
        // Only reset the focused component when the *turbine identity* changes, not on
        // every live-telemetry tick (the turbine object is rebuilt each refresh). This
        // keeps a clicked component selected across refreshes so the graph stays reachable.
    }, [onDeviceFocusChange, onPartFocusChange, turbine.id]);

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

        const detailSky = createSkyTexture();
        const pmrem = new THREE.PMREMGenerator(renderer);
        const envRT = pmrem.fromEquirectangular(detailSky);
        scene.environment = envRT.texture;
        pmrem.dispose();
        detailSky.dispose();

        const ambient = new THREE.AmbientLight(0xbcd4ff, 0.44);
        const sun = new THREE.DirectionalLight(0xf4f8ff, 1.32);
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
        const rim = new THREE.DirectionalLight(0x4fa3ff, 0.48);
        rim.position.set(-14, 10, -12);
        // Sky/ground hemisphere fill lifts shadowed component undersides so the
        // drivetrain and generator read clearly without flattening the key light.
        const hemi = new THREE.HemisphereLight(0x8fbfff, 0x0a1a2c, 0.32);
        scene.add(ambient, sun, rim, hemi);

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

        const baseMat = new THREE.MeshStandardMaterial({ color: "#7f8b9a", roughness: 0.66, metalness: 0.24, envMapIntensity: 0.95 });
        const towerMat = new THREE.MeshStandardMaterial({ color: "#e0e7ef", roughness: 0.28, metalness: 0.58, envMapIntensity: 1.25 });
        const bladeMat = new THREE.MeshStandardMaterial({ color: "#f7f9fd", roughness: 0.3, metalness: 0.14, envMapIntensity: 1.1 });
        const nacelleHousingMat = new THREE.MeshStandardMaterial({ color: "#e9eef4", roughness: 0.36, metalness: 0.52, envMapIntensity: 1.2 });
        const hubMat = new THREE.MeshStandardMaterial({ color: "#cfd8e2", roughness: 0.32, metalness: 0.58, envMapIntensity: 1.2 });
        const statusMat = new THREE.MeshStandardMaterial({
            color: STATUS_COLORS[turbineRef.current.status],
            emissive: STATUS_COLORS[turbineRef.current.status],
            emissiveIntensity: 0.7,
            roughness: 0.3,
            metalness: 0.2,
        });
        const ringMat = new THREE.MeshBasicMaterial({ color: STATUS_COLORS[turbineRef.current.status], transparent: true, opacity: 0.85 });

        const pedestal = new THREE.Mesh(new THREE.CylinderGeometry(1.0, 1.3, 0.5, 32), baseMat);
        pedestal.position.y = 0.25;
        pedestal.castShadow = true;
        pedestal.receiveShadow = true;
        group.add(pedestal);

        const HUB = 11;
        const tower = new THREE.Mesh(new THREE.CylinderGeometry(0.32, 0.85, HUB, 32), towerMat);
        tower.position.y = HUB / 2 + 0.3;
        tower.castShadow = true;
        group.add(tower);

        const nacelle = new THREE.Mesh(new THREE.BoxGeometry(3.0, 1.0, 1.1), nacelleHousingMat);
        nacelle.position.set(-0.3, HUB + 0.6, 0);
        nacelle.castShadow = true;
        group.add(nacelle);

        const spinner = new THREE.Mesh(new THREE.ConeGeometry(0.48, 1.1, 24), hubMat);
        spinner.rotation.z = -Math.PI / 2;
        spinner.position.set(1.7, HUB + 0.6, 0);
        spinner.castShadow = true;
        group.add(spinner);

        const beacon = new THREE.Mesh(new THREE.SphereGeometry(0.3, 16, 16), statusMat);
        beacon.position.set(-1.55, HUB + 1.25, 0);
        group.add(beacon);

        const bladeGeo = createBladeGeometry();
        const blades = new THREE.Group();
        blades.position.set(1.7, HUB + 0.6, 0);
        for (let i = 0; i < 3; i += 1) {
            const holder = new THREE.Group();
            holder.rotation.x = (i * (Math.PI * 2)) / 3;
            const blade = new THREE.Mesh(bladeGeo, bladeMat);
            blade.scale.set(2.4, 2.4, 2.4);
            blade.rotation.y = 0.22;
            blade.castShadow = true;
            holder.add(blade);
            blades.add(holder);
        }
        group.add(blades);

        // --- Higher-fidelity mechanical detail (drivetrain / generator "motor" parts) ---
        const steelMat = new THREE.MeshStandardMaterial({ color: "#9aa6b4", roughness: 0.42, metalness: 0.72, envMapIntensity: 1.15 });
        const copperMat = new THREE.MeshStandardMaterial({ color: "#c07b46", roughness: 0.38, metalness: 0.78, envMapIntensity: 1.15 });
        const darkMat = new THREE.MeshStandardMaterial({ color: "#3a434f", roughness: 0.58, metalness: 0.52, envMapIntensity: 1.0 });

        // Low-speed shaft: hub → gearbox, along the nacelle axis (X).
        const shaft = new THREE.Mesh(new THREE.CylinderGeometry(0.16, 0.16, 1.9, 20), steelMat);
        shaft.rotation.z = Math.PI / 2;
        shaft.position.set(0.9, HUB + 0.6, 0);
        shaft.castShadow = true;
        group.add(shaft);

        // Gearbox block (drivetrain) mid-nacelle.
        const gearbox = new THREE.Mesh(new THREE.BoxGeometry(0.9, 0.8, 0.8), darkMat);
        gearbox.position.set(0.1, HUB + 0.55, 0);
        gearbox.castShadow = true;
        group.add(gearbox);

        // Generator drum with radial cooling fins at the nacelle rear — the "motor".
        const generator = new THREE.Mesh(new THREE.CylinderGeometry(0.42, 0.42, 1.1, 24), copperMat);
        generator.rotation.z = Math.PI / 2;
        generator.position.set(-1.15, HUB + 0.6, 0);
        generator.castShadow = true;
        group.add(generator);
        const finGeo = new THREE.BoxGeometry(1.0, 0.5, 0.05);
        for (let i = 0; i < 8; i += 1) {
            const fin = new THREE.Mesh(finGeo, steelMat);
            fin.position.set(-1.15, HUB + 0.6, 0);
            fin.rotation.x = (i * Math.PI) / 8;
            group.add(fin);
        }
        // Generator end bell / bearing cap.
        const endBell = new THREE.Mesh(new THREE.CylinderGeometry(0.3, 0.44, 0.22, 24), steelMat);
        endBell.rotation.z = Math.PI / 2;
        endBell.position.set(-1.78, HUB + 0.6, 0);
        group.add(endBell);

        // Nacelle top cooling fins.
        const topFinGeo = new THREE.BoxGeometry(0.08, 0.22, 0.9);
        for (let i = 0; i < 6; i += 1) {
            const fin = new THREE.Mesh(topFinGeo, steelMat);
            fin.position.set(-1.1 + i * 0.32, HUB + 1.22, 0);
            group.add(fin);
        }

        // Yaw bearing ring at the tower top.
        const yawRing = new THREE.Mesh(new THREE.TorusGeometry(0.6, 0.1, 12, 32), steelMat);
        yawRing.rotation.x = Math.PI / 2;
        yawRing.position.set(-0.3, HUB + 0.05, 0);
        group.add(yawRing);

        // Blade-root hubs at the rotor for a beefier hub read.
        const rootGeo = new THREE.CylinderGeometry(0.22, 0.26, 0.5, 16);
        for (let i = 0; i < 3; i += 1) {
            const root = new THREE.Mesh(rootGeo, hubMat);
            const ang = (i * (Math.PI * 2)) / 3;
            root.position.set(1.9, HUB + 0.6 + Math.sin(ang) * 0.55, Math.cos(ang) * 0.55);
            root.rotation.x = ang;
            group.add(root);
        }

        const ring = new THREE.Mesh(new THREE.RingGeometry(1.7, 2.2, 48), ringMat);
        ring.rotation.x = -Math.PI / 2;
        ring.position.y = 0.05;
        group.add(ring);

        const partFocus = {
            rotor: { lookAt: new THREE.Vector3(1.7, HUB + 0.65, 0), offset: new THREE.Vector3(6.5, 1.8, 4.8), zoom: 0.62 },
            nacelle: { lookAt: new THREE.Vector3(-0.4, HUB + 0.8, 0), offset: new THREE.Vector3(5.2, 2.0, 5.8), zoom: 0.68 },
            drivetrain: { lookAt: new THREE.Vector3(0.0, HUB * 0.58, 0), offset: new THREE.Vector3(6.2, 1.6, 6.8), zoom: 0.74 },
            base: { lookAt: new THREE.Vector3(0, 0.45, 0), offset: new THREE.Vector3(7.5, 2.4, 8.0), zoom: 0.84 },
        } satisfies Record<TwinPartKey, { lookAt: THREE.Vector3; offset: THREE.Vector3; zoom: number }>;

        const deviceRoot = new THREE.Group();
        scene.add(deviceRoot);

        const pickables: THREE.Object3D[] = [];
        const bindPart = (obj: THREE.Object3D, part: TwinPartKey) => {
            obj.userData.partKey = part;
            pickables.push(obj);
        };

        const deviceRefs = new Map<TwinDeviceKey, { mesh: THREE.Mesh; line: THREE.Line; statusMat: THREE.MeshStandardMaterial; lineMat: THREE.LineBasicMaterial; def: TwinDeviceNode }>();
        (Object.keys(deviceGraphRef.current) as TwinPartKey[]).forEach((component) => {
            const componentDevices = deviceGraphRef.current[component];
            const parentAnchor = partFocus[component].lookAt;
            componentDevices.forEach((def) => {
                const statusMat = new THREE.MeshStandardMaterial({
                    color: STATUS_COLORS[turbineRef.current.status],
                    emissive: STATUS_COLORS[turbineRef.current.status],
                    emissiveIntensity: 0.22,
                    roughness: 0.35,
                    metalness: 0.12,
                });
                const mesh = new THREE.Mesh(new THREE.SphereGeometry(0.17, 16, 16), statusMat);
                mesh.position.set(def.anchor[0], def.anchor[1], def.anchor[2]);
                mesh.userData.deviceKey = def.key;
                mesh.userData.partKey = component;
                mesh.userData.deviceComponent = component;
                mesh.castShadow = true;
                deviceRoot.add(mesh);
                pickables.push(mesh);

                const lineMat = new THREE.LineBasicMaterial({ color: STATUS_COLORS[turbineRef.current.status], transparent: true, opacity: 0.55 });
                const lineGeo = new THREE.BufferGeometry().setFromPoints([parentAnchor.clone(), mesh.position.clone()]);
                const line = new THREE.Line(lineGeo, lineMat);
                deviceRoot.add(line);

                deviceRefs.set(def.key, { mesh, line, statusMat, lineMat, def });
            });
        });

        bindPart(spinner, "rotor");
        bindPart(blades, "rotor");
        blades.traverse((obj) => {
            if (obj !== blades && obj instanceof THREE.Mesh) {
                bindPart(obj, "rotor");
            }
        });

        bindPart(nacelle, "nacelle");
        bindPart(beacon, "nacelle");
        bindPart(tower, "drivetrain");
        bindPart(pedestal, "base");
        bindPart(ring, "base");

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

        const raycaster = new THREE.Raycaster();
        const pointer = new THREE.Vector2();
        const onClick = (event: MouseEvent) => {
            const rect = renderer.domElement.getBoundingClientRect();
            pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
            pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
            raycaster.setFromCamera(pointer, camera);
            const hits = raycaster.intersectObjects(pickables, true);
            const hitDevice = hits.find((h) => Boolean(h.object.userData.deviceKey))?.object.userData.deviceKey as TwinDeviceKey | undefined;
            if (hitDevice) {
                const def = deviceRefs.get(hitDevice)?.def;
                if (def) {
                    focusedPartRef.current = def.component;
                    focusedDeviceRef.current = hitDevice;
                    zoomRef.current = def.zoom;
                    onPartFocusChange?.(def.component);
                    onDeviceFocusChange?.(hitDevice);
                }
                return;
            }

            const hitPart = hits.find((h) => Boolean(h.object.userData.partKey))?.object.userData.partKey as TwinPartKey | undefined;
            const next = hitPart ?? null;
            focusedPartRef.current = next;
            focusedDeviceRef.current = null;
            if (next) {
                zoomRef.current = partFocus[next].zoom;
            }
            onPartFocusChange?.(next);
            onDeviceFocusChange?.(null);
        };
        renderer.domElement.addEventListener("click", onClick);

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
        const currentLookAt = new THREE.Vector3(0, 6.5, 0);
        const targetLookAt = new THREE.Vector3(0, 6.5, 0);
        const currentOffset = new THREE.Vector3(12, 10, 24);
        const targetOffset = new THREE.Vector3(12, 10, 24);
        const animate = () => {
            if (!pausedRef.current) {
                tick += 0.02;
                blades.rotation.x += 0.03 + Math.min(0.3, turbineRef.current.windMs / 55);
            }
            const z = zoomRef.current;
            const focusedDevice = focusedDeviceRef.current;
            const focusedPart = focusedPartRef.current;
            const currentDevice = focusedDevice ? deviceRefs.get(focusedDevice)?.def : null;
            if (currentDevice) {
                targetLookAt.set(currentDevice.lookAt[0], currentDevice.lookAt[1], currentDevice.lookAt[2]);
                targetOffset.set(currentDevice.offset[0], currentDevice.offset[1], currentDevice.offset[2]).multiplyScalar(z);
            } else if (focusedPart) {
                targetLookAt.copy(partFocus[focusedPart].lookAt);
                targetOffset.copy(partFocus[focusedPart].offset).multiplyScalar(z);
            } else {
                targetOffset.set(Math.sin(tick * 0.25) * 16 * z, 9 * z + 3, Math.cos(tick * 0.25) * 24 * z);
                const focus = THREE.MathUtils.clamp((z - 0.16) / (1 - 0.16), 0, 1);
                targetLookAt.set(0, THREE.MathUtils.lerp(11.2, 6.5, focus), 0);
            }
            currentLookAt.lerp(targetLookAt, 0.14);
            currentOffset.lerp(targetOffset, 0.14);
            camera.position.set(currentLookAt.x + currentOffset.x, currentLookAt.y + currentOffset.y, currentLookAt.z + currentOffset.z);
            camera.lookAt(currentLookAt);

            const focusedColor = focusedPart ? signalColor(
                focusedPart === "rotor" ? "wind" : focusedPart === "nacelle" ? "temp" : focusedPart === "drivetrain" ? "vibration" : "power",
                focusedPart === "rotor" ? turbineRef.current.windMs : focusedPart === "nacelle" ? turbineRef.current.nacelleTempC : focusedPart === "drivetrain" ? turbineRef.current.vibrationMmS : turbineRef.current.powerKw,
            ) : STATUS_COLORS[turbineRef.current.status];
            ringMat.color.set(focusedColor);
            ringMat.opacity = focusedPart ? 0.97 : 0.85;
            ring.scale.setScalar(focusedPart ? 1.28 : 1);

            const activeComponent = focusedPart ?? null;
            deviceRefs.forEach((refs) => {
                const visible = activeComponent === refs.def.component;
                refs.mesh.visible = visible;
                refs.line.visible = visible;
                refs.mesh.position.set(refs.def.anchor[0], refs.def.anchor[1], refs.def.anchor[2]);
                (refs.line.geometry as THREE.BufferGeometry).setFromPoints([
                    partFocus[refs.def.component].lookAt.clone(),
                    refs.mesh.position.clone(),
                ]);
                const statusColor = refs.def.status(turbineRef.current);
                refs.statusMat.color.set(statusColor);
                refs.statusMat.emissive.set(statusColor);
                refs.statusMat.emissiveIntensity = focusedDevice === refs.def.key ? 0.72 : 0.22;
                refs.lineMat.color.set(statusColor);
                refs.lineMat.opacity = focusedDevice === refs.def.key ? 0.85 : 0.55;
                refs.mesh.scale.setScalar(focusedDevice === refs.def.key ? 1.55 : 1.0);
            });

            renderer.render(scene, camera);

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
            renderer.domElement.removeEventListener("click", onClick);
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
            deviceRoot.clear();
            envRT.dispose();
        };
    }, [deviceGraph]);

    useEffect(() => {
        colorApiRef.current?.(STATUS_COLORS[turbine.status]);
    }, [turbine.status]);

    return (
        <div className="relative h-full w-full">
            <div ref={hostRef} className="h-full w-full" />
            {(() => {
                const readouts = [
                    { text: `${turbine.windMs} m/s`, color: signalColor("wind", turbine.windMs) },
                    { text: `${turbine.nacelleTempC}°C`, color: signalColor("temp", turbine.nacelleTempC) },
                    { text: `${turbine.vibrationMmS} mm/s`, color: signalColor("vibration", turbine.vibrationMmS) },
                    { text: `${turbine.powerKw.toLocaleString()} kW`, color: "#6ee7ff" },
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
                <button type="button" title="Reset view" onClick={() => { zoomRef.current = 1; focusedPartRef.current = null; focusedDeviceRef.current = null; onPartFocusChange?.(null); onDeviceFocusChange?.(null); }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60">⟳</button>
            </div>
            <div className="pointer-events-none absolute bottom-3 left-3 rounded border border-slate-700/70 bg-[#06101fcc] px-2 py-1 text-[11px] text-slate-300 backdrop-blur">
                Click a component to open its child devices. Click a device for the second-level graph.
            </div>
        </div>
    );
}
