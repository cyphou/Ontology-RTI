import { useEffect, useRef } from "react";
import * as THREE from "three";
import { STATUS_COLORS, TWIN_PARTS, createSkyTexture, signalColor } from "../App";

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
        renderer.outputColorSpace = THREE.SRGBColorSpace;
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.12;
        host.innerHTML = "";
        host.appendChild(renderer.domElement);

        // Soft image-based reflections for the close-up steel hardware.
        const detailSky = createSkyTexture();
        const pmrem = new THREE.PMREMGenerator(renderer);
        const envRT = pmrem.fromEquirectangular(detailSky);
        scene.environment = envRT.texture;
        pmrem.dispose();
        detailSky.dispose();

        const ambient = new THREE.AmbientLight(0xbcd4ff, 0.5);
        const sun = new THREE.DirectionalLight(0xffffff, 1.22);
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
        const steelMat = new THREE.MeshStandardMaterial({ color: "#c2cad6", roughness: 0.42, metalness: 0.68 });
        const tankMat = new THREE.MeshStandardMaterial({ color: "#dde3ea", roughness: 0.5, metalness: 0.38 });
        const stackMat = new THREE.MeshStandardMaterial({ color: "#8a939f", roughness: 0.6, metalness: 0.5 });
        const flameMat = new THREE.MeshStandardMaterial({ color: "#ff9b3d", emissive: "#ff5a1f", emissiveIntensity: 0.95, roughness: 0.4 });
        const pipeMat = new THREE.MeshStandardMaterial({ color: "#9aa6b4", roughness: 0.5, metalness: 0.55 });
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

        // Distillation column - the main process unit (caption "Column").
        const column = new THREE.Mesh(new THREE.CylinderGeometry(0.9, 1.1, 6.4, 28), steelMat);
        column.position.set(0, 3.6, 0);
        column.castShadow = true;
        group.add(column);
        [2.0, 3.6, 5.2].forEach((y) => {
            const plat = new THREE.Mesh(new THREE.TorusGeometry(1.05, 0.08, 10, 28), pipeMat);
            plat.rotation.x = Math.PI / 2;
            plat.position.y = y;
            group.add(plat);
        });

        // Storage tank (caption "Tank").
        const tank = new THREE.Mesh(new THREE.CylinderGeometry(1.5, 1.5, 2.4, 28), tankMat);
        tank.position.set(-3.6, 1.4, 0);
        tank.castShadow = true;
        group.add(tank);
        const tankDome = new THREE.Mesh(new THREE.SphereGeometry(1.5, 24, 12, 0, Math.PI * 2, 0, Math.PI / 2), tankMat);
        tankDome.position.set(-3.6, 2.6, 0);
        group.add(tankDome);

        // Flare stack with flame (caption "Flare"). `blades` holds the flame so the
        // animate loop can flicker it with live feed rate.
        const flareStack = new THREE.Mesh(new THREE.CylinderGeometry(0.18, 0.24, 3.6, 16), stackMat);
        flareStack.position.set(2.8, 1.8, 1.4);
        flareStack.castShadow = true;
        group.add(flareStack);
        const blades = new THREE.Group();
        blades.position.set(2.8, 3.9, 1.4);
        const flame = new THREE.Mesh(new THREE.ConeGeometry(0.4, 1.3, 16), flameMat);
        blades.add(flame);
        group.add(blades);

        // Product pipe header (caption "Throughput").
        const pipe = new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.22, 4.2, 16), pipeMat);
        pipe.rotation.z = Math.PI / 2;
        pipe.position.set(0, 0.7, 2.8);
        group.add(pipe);

        // Status beacon above the column.
        const beacon = new THREE.Mesh(new THREE.SphereGeometry(0.3, 16, 16), statusMat);
        beacon.position.set(0, 7.1, 0);
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
                // Flare flame flicker, amplitude scaled by live feed rate.
                const feed = Math.min(1, turbineRef.current.irradianceWm2 / 200);
                blades.scale.y = 1 + Math.sin(tick * 4) * 0.22 * (0.5 + feed);
                blades.scale.x = 1 + Math.sin(tick * 5.3) * 0.12;
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
            envRT.dispose();
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
                    { text: `${turbine.irradianceWm2} kbd`, color: signalColor("irradiance", turbine.irradianceWm2) },
                    { text: `${turbine.moduleTempC}°C`, color: signalColor("moduleTemp", turbine.moduleTempC) },
                    { text: `${turbine.inverterLoadPct}%`, color: signalColor("inverterLoad", turbine.inverterLoadPct) },
                    { text: `${turbine.powerKw.toLocaleString()} kbd`, color: "#6ee7ff" },
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