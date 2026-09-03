import { type MouseEvent } from "react";

type UnitStatus = "healthy" | "warning" | "alarm";

export interface RefineryProcessSchematicProps {
    unitId: string;
    siteName: string;
    status: UnitStatus;
    feedKbd: number;
    temperatureC: number;
    utilizationPct: number;
    throughputKbd: number;
    onCreateWorkOrder: () => void;
}

const STATUS = {
    healthy: { stroke: "#58d68d", fill: "#062b23", label: "NORMAL" },
    warning: { stroke: "#f5c75f", fill: "#33280d", label: "WATCH" },
    alarm: { stroke: "#fb4f75", fill: "#35101c", label: "ALARM" },
} as const;

function ValueTag({ x, y, label, value, alarm }: { x: number; y: number; label: string; value: string; alarm?: boolean }) {
    return (
        <g transform={`translate(${x} ${y})`}>
            <rect width="102" height="34" rx="3" fill="#07162d" stroke={alarm ? "#fb4f75" : "#355171"} />
            <text x="7" y="13" fill="#91a6be" fontSize="8" fontFamily="ui-sans-serif, system-ui">{label}</text>
            <text x="7" y="26" fill={alarm ? "#fb4f75" : "#e5f0ff"} fontWeight="700" fontSize="12" fontFamily="ui-sans-serif, system-ui">{value}</text>
        </g>
    );
}

// A compact P&ID-style process overview. It is intentionally a schematic rather
// than a photorealistic scene: the operator can scan flow direction, live signals,
// and alarm state without navigating the 3D model.
export function RefineryProcessSchematic({ unitId, siteName, status, feedKbd, temperatureC, utilizationPct, throughputKbd, onCreateWorkOrder }: RefineryProcessSchematicProps) {
    const state = STATUS[status];
    const criticalTemp = temperatureC >= 430;
    const criticalLoad = utilizationPct >= 98;
    const createOrder = (event: MouseEvent<HTMLButtonElement>) => {
        event.stopPropagation();
        onCreateWorkOrder();
    };

    return (
        <div className="relative h-full w-full overflow-hidden bg-[#061527]">
            <div className="absolute left-3 top-3 z-10 rounded border border-slate-700/70 bg-[#07162de6] px-3 py-2 text-xs backdrop-blur">
                <p className="font-semibold text-slate-100">{unitId} · Process schematic</p>
                <p className="text-[11px] text-slate-400">{siteName} · live operating view</p>
            </div>
            <div className="absolute right-3 top-3 z-10 flex items-center gap-2 rounded border px-2 py-1 text-[11px] font-semibold" style={{ borderColor: state.stroke, color: state.stroke, backgroundColor: state.fill }}>
                <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: state.stroke }} /> {state.label}
            </div>
            <svg viewBox="0 0 1000 510" role="img" aria-label={`Process schematic for ${unitId}`} className="h-full w-full min-w-[660px]">
                <defs>
                    <linearGradient id="pipe" x1="0" x2="1"><stop stopColor="#397ba8" /><stop offset="1" stopColor="#69c5e1" /></linearGradient>
                    <linearGradient id="column" x1="0" x2="1"><stop stopColor="#63788e" /><stop offset="0.5" stopColor="#c4d3df" /><stop offset="1" stopColor="#506479" /></linearGradient>
                    <filter id="glow"><feGaussianBlur stdDeviation="3" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
                    <pattern id="grid" width="25" height="25" patternUnits="userSpaceOnUse"><path d="M 25 0 L 0 0 0 25" fill="none" stroke="#17314b" strokeWidth="1" /></pattern>
                </defs>
                <rect width="1000" height="510" fill="url(#grid)" />
                <text x="34" y="92" fill="#8ca4bd" fontSize="12" fontFamily="ui-sans-serif, system-ui">CRUDE FEED</text>
                <path d="M 48 238 H 245 V 350 H 390" fill="none" stroke="url(#pipe)" strokeWidth="13" strokeLinejoin="round" />
                <path d="M 48 238 H 245 V 350 H 390" fill="none" stroke="#b7eafa" strokeOpacity=".35" strokeWidth="3" />
                <polygon points="390,350 370,337 370,363" fill="#6ee7ff" />
                <rect x="174" y="196" width="92" height="82" rx="8" fill="#152d43" stroke="#6ba0c6" strokeWidth="3" />
                <circle cx="220" cy="237" r="24" fill="#3c5269" stroke="#d4e4ef" strokeWidth="3" />
                <path d="M 205 237 H 235 M 220 222 V 252" stroke="#d4e4ef" strokeWidth="3" />
                <text x="181" y="303" fill="#d7e6f5" fontSize="11" fontFamily="ui-sans-serif, system-ui">FEED PUMP</text>

                <rect x="382" y="105" width="114" height="294" rx="44" fill="url(#column)" stroke={state.stroke} strokeWidth="4" filter={status === "alarm" ? "url(#glow)" : undefined} />
                {[155, 208, 261, 314].map((y) => <path key={y} d={`M 386 ${y} H 492`} stroke="#34495d" strokeWidth="4" />)}
                <ellipse cx="439" cy="105" rx="40" ry="13" fill="#d4e1ea" stroke="#506479" strokeWidth="3" />
                <text x="402" y="425" fill="#d7e6f5" fontSize="12" fontFamily="ui-sans-serif, system-ui">DISTILLATION</text>
                <text x="420" y="440" fill="#8ca4bd" fontSize="10" fontFamily="ui-sans-serif, system-ui">COLUMN</text>

                <path d="M 496 151 H 620 V 113 H 760" fill="none" stroke="url(#pipe)" strokeWidth="11" />
                <path d="M 496 250 H 680" fill="none" stroke="url(#pipe)" strokeWidth="11" />
                <path d="M 496 349 H 620 V 406 H 760" fill="none" stroke="url(#pipe)" strokeWidth="13" />
                <polygon points="760,113 740,100 740,126" fill="#6ee7ff" /><polygon points="680,250 660,237 660,263" fill="#6ee7ff" /><polygon points="760,406 740,393 740,419" fill="#6ee7ff" />
                <text x="766" y="116" fill="#a7d8e6" fontSize="12" fontFamily="ui-sans-serif, system-ui">LIGHT PRODUCTS</text>
                <text x="688" y="254" fill="#a7d8e6" fontSize="12" fontFamily="ui-sans-serif, system-ui">MIDDLE DISTILLATE</text>
                <text x="766" y="410" fill="#a7d8e6" fontSize="12" fontFamily="ui-sans-serif, system-ui">HEAVY PRODUCTS</text>

                <rect x="585" y="190" width="90" height="72" rx="7" fill="#284052" stroke="#b8ccd9" strokeWidth="3" />
                <path d="M 593 208 H 667 M 593 226 H 667 M 593 244 H 667" stroke="#6ee7ff" strokeWidth="3" />
                <text x="594" y="282" fill="#d7e6f5" fontSize="10" fontFamily="ui-sans-serif, system-ui">HEAT EXCHANGER</text>

                <ellipse cx="812" cy="383" rx="48" ry="22" fill="#4f6475" stroke="#c5d6e1" strokeWidth="3" />
                <rect x="764" y="337" width="96" height="47" fill="#7d94a4" stroke="#c5d6e1" strokeWidth="3" />
                <ellipse cx="812" cy="337" rx="48" ry="22" fill="#aabcc8" stroke="#c5d6e1" strokeWidth="3" />
                <text x="778" y="429" fill="#d7e6f5" fontSize="11" fontFamily="ui-sans-serif, system-ui">PRODUCT TANK</text>

                <ValueTag x={62} y={175} label="FEED RATE" value={`${feedKbd.toLocaleString()} kbd`} />
                <ValueTag x={300} y={182} label="COLUMN TEMP" value={`${temperatureC.toFixed(1)} °C`} alarm={criticalTemp} />
                <ValueTag x={514} y={288} label="UTILIZATION" value={`${utilizationPct.toFixed(0)} %`} alarm={criticalLoad} />
                <ValueTag x={718} y={175} label="THROUGHPUT" value={`${throughputKbd.toLocaleString()} kbd`} />
                {status !== "healthy" && <g><circle cx="532" cy="88" r="11" fill={state.stroke} filter="url(#glow)" /><text x="553" y="92" fill={state.stroke} fontSize="12" fontWeight="700" fontFamily="ui-sans-serif, system-ui">ACTIVE ALARM</text></g>}
            </svg>
            <button type="button" onClick={createOrder} className="absolute bottom-3 right-3 rounded bg-emerald-600 px-3 py-2 text-xs font-semibold text-white shadow hover:bg-emerald-500">Create work order</button>
        </div>
    );
}
