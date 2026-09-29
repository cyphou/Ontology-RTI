import type { MouseEvent } from "react";

type ArrayStatus = "healthy" | "warning" | "alarm";

export interface SolarArraySchematicProps {
    arrayId: string;
    siteName: string;
    status: ArrayStatus;
    irradianceWm2: number;
    moduleTempC: number;
    inverterLoadPct: number;
    powerKw: number;
    onCreateRepairOrder: () => void;
}

const STATUS = {
    healthy: { stroke: "#58d68d", fill: "#062b23", label: "NORMAL" },
    warning: { stroke: "#f5c75f", fill: "#33280d", label: "WATCH" },
    alarm: { stroke: "#fb4f75", fill: "#35101c", label: "ALARM" },
} as const;

function ValueTag({ x, y, label, value, alarm }: { x: number; y: number; label: string; value: string; alarm?: boolean }) {
    return <g transform={`translate(${x} ${y})`}><rect width="112" height="34" rx="3" fill="#07162d" stroke={alarm ? "#fb4f75" : "#355171"} /><text x="7" y="13" fill="#91a6be" fontSize="8">{label}</text><text x="7" y="26" fill={alarm ? "#fb4f75" : "#e5f0ff"} fontWeight="700" fontSize="12">{value}</text></g>;
}

export function SolarArraySchematic({ arrayId, siteName, status, irradianceWm2, moduleTempC, inverterLoadPct, powerKw, onCreateRepairOrder }: SolarArraySchematicProps) {
    const state = STATUS[status];
    const createOrder = (event: MouseEvent<HTMLButtonElement>) => { event.stopPropagation(); onCreateRepairOrder(); };
    return (
        <div className="relative h-full w-full overflow-hidden bg-[#061527]">
            <div className="absolute left-3 top-3 z-10 rounded border border-slate-700/70 bg-[#07162de6] px-3 py-2 text-xs backdrop-blur"><p className="font-semibold text-slate-100">{arrayId} · PV array schematic</p><p className="text-[11px] text-slate-400">{siteName} · live generation path</p></div>
            <div className="absolute right-3 top-3 z-10 rounded border px-2 py-1 text-[11px] font-semibold" style={{ borderColor: state.stroke, color: state.stroke, backgroundColor: state.fill }}>{state.label}</div>
            <svg viewBox="0 0 1000 510" role="img" aria-label={`PV array schematic for ${arrayId}`} className="h-full w-full min-w-[660px]">
                <defs><pattern id="grid" width="25" height="25" patternUnits="userSpaceOnUse"><path d="M 25 0 L 0 0 0 25" fill="none" stroke="#17314b" /></pattern><filter id="glow"><feGaussianBlur stdDeviation="3" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter></defs>
                <rect width="1000" height="510" fill="url(#grid)" />
                <text x="40" y="95" fill="#f5c75f" fontSize="13">SOLAR IRRADIANCE</text><path d="M 170 110 V 180 M 245 110 V 180 M 320 110 V 180" stroke="#f5c75f" strokeWidth="3" strokeDasharray="7 8" />
                {[0, 1, 2, 3].map((row) => <g key={row}>{[0, 1, 2, 3, 4].map((column) => <rect key={column} x={70 + column * 56} y={190 + row * 38} width="48" height="30" rx="2" fill="#174b78" stroke={state.stroke} strokeWidth="1.5" />)}</g>)}
                <text x="132" y="370" fill="#d7e6f5" fontSize="12">PV MODULE STRINGS</text>
                <path d="M 354 266 H 445 M 555 266 H 665 M 780 266 H 910" fill="none" stroke="#69c5e1" strokeWidth="12" /><polygon points="445,266 425,253 425,279" fill="#6ee7ff" /><polygon points="665,266 645,253 645,279" fill="#6ee7ff" /><polygon points="910,266 890,253 890,279" fill="#6ee7ff" />
                <rect x="445" y="212" width="110" height="108" rx="8" fill="#284052" stroke="#b8ccd9" strokeWidth="3" /><path d="M 463 240 H 537 M 463 266 H 537 M 463 292 H 537" stroke="#6ee7ff" strokeWidth="3" /><text x="460" y="345" fill="#d7e6f5" fontSize="11">COMBINER BOX</text>
                <rect x="665" y="185" width="115" height="162" rx="12" fill="#193c52" stroke={state.stroke} strokeWidth="4" filter={status === "alarm" ? "url(#glow)" : undefined} /><circle cx="722" cy="245" r="27" fill="#0d2236" stroke="#6ee7ff" strokeWidth="3" /><path d="M 708 245 H 736 M 722 231 V 259" stroke="#d4e4ef" strokeWidth="3" /><text x="692" y="373" fill="#d7e6f5" fontSize="12">INVERTER</text>
                <rect x="910" y="214" width="55" height="104" rx="4" fill="#4f6475" stroke="#c5d6e1" strokeWidth="3" /><text x="892" y="345" fill="#d7e6f5" fontSize="11">GRID EXPORT</text>
                <ValueTag x={42} y={395} label="IRRADIANCE" value={`${irradianceWm2.toFixed(0)} W/m²`} /><ValueTag x={330} y={145} label="MODULE TEMP" value={`${moduleTempC.toFixed(1)} °C`} alarm={moduleTempC >= 80} /><ValueTag x={585} y={395} label="INVERTER LOAD" value={`${inverterLoadPct.toFixed(0)} %`} alarm={inverterLoadPct >= 98} /><ValueTag x={834} y={145} label="AC POWER" value={`${powerKw.toLocaleString()} kW`} />
            </svg>
            <button type="button" onClick={createOrder} className="absolute bottom-3 right-3 rounded bg-emerald-600 px-3 py-2 text-xs font-semibold text-white shadow hover:bg-emerald-500">Create repair order</button>
        </div>
    );
}