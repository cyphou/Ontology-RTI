import { useEffect, useRef } from "react";
import * as THREE from "three";
import { STATUS_COLORS, TWIN_PARTS, signalColor } from "../App";

type PlantStatus = "healthy" | "warning" | "alarm";
type PlantTelemetry = {
    status: PlantStatus;
    powerKw: number;
    irradianceWm2: number;
    moduleTempC: number;
    inverterLoadPct: number;
};
export default function PlantTwinScene({ turbine, paused }: { turbine: PlantTelemetry; paused: boolean }) {
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
        host.innerHTML = "";
        host.appendChild(renderer.domElement);

        const ambient = new THREE.AmbientLight(0xbcd4ff, 0.55);
        const sun = new THREE.DirectionalLight(0xffffff, 1.15);
        sun.position.set(12, 24, 10);
        sun.castShadow = true;
        sun.shadow.mapSize.set(1024, 1024);
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
        const frameMat = new THREE.MeshStandardMaterial({ color: "#cfd7e2", roughness: 0.45, metalness: 0.5 });
        const panelMat = new THREE.MeshStandardMaterial({ color: "#10325c", roughness: 0.25, metalness: 0.55, emissive: "#0a1c3a", emissiveIntensity: 0.3 });
        const cellMat = new THREE.MeshBasicMaterial({ color: "#1d4a86" });
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

        // Tracker post + drive motor under the table.
        const post = new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.3, 3.0, 20), frameMat);
        post.position.set(0, 1.7, 0);
        post.castShadow = true;
        group.add(post);

        const motor = new THREE.Mesh(new THREE.BoxGeometry(0.9, 0.7, 0.7), frameMat);
        motor.position.set(2.8, 0.9, 1.4);
        motor.castShadow = true;
        group.add(motor);

        // Inverter / transformer cabinet beside the array.
        const inverter = new THREE.Mesh(new THREE.BoxGeometry(1.4, 2.0, 1.0), frameMat);
        inverter.position.set(-3.6, 1.2, 0);
        inverter.castShadow = true;
        group.add(inverter);

        // Tilting PV panel table: a glass surface split into a grid of cells.
        const blades = new THREE.Group();
        blades.position.set(0, 3.2, 0);
        const panelW = 6.0;
        const panelD = 3.2;
        const table = new THREE.Mesh(new THREE.BoxGeometry(panelW, 0.12, panelD), panelMat);
        table.castShadow = true;
        blades.add(table);
        for (let c = -2; c <= 2; c += 1) {
            const vbar = new THREE.Mesh(new THREE.BoxGeometry(0.05, 0.14, panelD), cellMat);
            vbar.position.set((c * panelW) / 5, 0, 0);
            blades.add(vbar);
        }
        for (let r = -1; r <= 1; r += 1) {
            const hbar = new THREE.Mesh(new THREE.BoxGeometry(panelW, 0.14, 0.05), cellMat);
            hbar.position.set(0, 0, (r * panelD) / 3);
            blades.add(hbar);
        }
        blades.rotation.x = -0.6;
        group.add(blades);

        // Status beacon above the array.
        const beacon = new THREE.Mesh(new THREE.SphereGeometry(0.3, 16, 16), statusMat);
        beacon.position.set(0, 4.0, -1.4);
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
                // Gentle sun-tracking sweep, amplitude scaled by live irradiance.
                blades.rotation.x = -0.6 + Math.sin(tick * 0.6) * 0.22 * Math.min(1, turbineRef.current.irradianceWm2 / 1000);
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
                    { text: `${turbine.irradianceWm2} W/m²`, color: signalColor("irradiance", turbine.irradianceWm2) },
                    { text: `${turbine.moduleTempC}°C`, color: signalColor("moduleTemp", turbine.moduleTempC) },
                    { text: `${turbine.inverterLoadPct}%`, color: signalColor("inverterLoad", turbine.inverterLoadPct) },
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
                <button type="button" title="Reset zoom" onClick={() => { zoomRef.current = 1; }} className="border-t border-slate-700/60 px-2.5 py-1.5 text-xs hover:bg-slate-700/60">⟳</button>
            </div>
        </div>
    );
}