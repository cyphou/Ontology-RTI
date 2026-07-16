import { entity, authenticated, uuid, text, decimal, int } from "@microsoft/rayfin-core";

/**
 * Ontology graph leaf for digital twin component hierarchy.
 *
 * Stores second-level device nodes under each turbine component so the twin view
 * can render and inspect a backend-driven component -> device graph instead of
 * a hard-coded client-only structure.
 */
@entity()
@authenticated("*")
export class TurbineDevice {
    @uuid() id!: string;
    @text({ max: 64, unique: true }) deviceKey!: string;
    @text({ max: 24 }) component!: string;
    @text({ max: 64 }) label!: string;
    @text({ max: 64 }) property!: string;
    @text({ max: 16 }) unit!: string;
    @text({ max: 260 }) note!: string;

    @decimal() anchorX!: number;
    @decimal() anchorY!: number;
    @decimal() anchorZ!: number;

    @decimal() lookAtX!: number;
    @decimal() lookAtY!: number;
    @decimal() lookAtZ!: number;

    @decimal() offsetX!: number;
    @decimal() offsetY!: number;
    @decimal() offsetZ!: number;

    @decimal() zoom!: number;
    @int() sortOrder!: number;
}
