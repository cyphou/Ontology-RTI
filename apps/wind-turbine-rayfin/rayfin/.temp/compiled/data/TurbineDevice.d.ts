/**
 * Ontology graph leaf for digital twin component hierarchy.
 *
 * Stores second-level device nodes under each turbine component so the twin view
 * can render and inspect a backend-driven component -> device graph instead of
 * a hard-coded client-only structure.
 */
export declare class TurbineDevice {
    id: string;
    deviceKey: string;
    component: string;
    label: string;
    property: string;
    unit: string;
    note: string;
    anchorX: number;
    anchorY: number;
    anchorZ: number;
    lookAtX: number;
    lookAtY: number;
    lookAtZ: number;
    offsetX: number;
    offsetY: number;
    offsetZ: number;
    zoom: number;
    sortOrder: number;
}
