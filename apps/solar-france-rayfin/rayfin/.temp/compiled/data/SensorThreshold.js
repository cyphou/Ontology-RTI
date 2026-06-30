var __esDecorate = (this && this.__esDecorate) || function (ctor, descriptorIn, decorators, contextIn, initializers, extraInitializers) {
    function accept(f) { if (f !== void 0 && typeof f !== "function") throw new TypeError("Function expected"); return f; }
    var kind = contextIn.kind, key = kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
    var target = !descriptorIn && ctor ? contextIn["static"] ? ctor : ctor.prototype : null;
    var descriptor = descriptorIn || (target ? Object.getOwnPropertyDescriptor(target, contextIn.name) : {});
    var _, done = false;
    for (var i = decorators.length - 1; i >= 0; i--) {
        var context = {};
        for (var p in contextIn) context[p] = p === "access" ? {} : contextIn[p];
        for (var p in contextIn.access) context.access[p] = contextIn.access[p];
        context.addInitializer = function (f) { if (done) throw new TypeError("Cannot add initializers after decoration has completed"); extraInitializers.push(accept(f || null)); };
        var result = (0, decorators[i])(kind === "accessor" ? { get: descriptor.get, set: descriptor.set } : descriptor[key], context);
        if (kind === "accessor") {
            if (result === void 0) continue;
            if (result === null || typeof result !== "object") throw new TypeError("Object expected");
            if (_ = accept(result.get)) descriptor.get = _;
            if (_ = accept(result.set)) descriptor.set = _;
            if (_ = accept(result.init)) initializers.unshift(_);
        }
        else if (_ = accept(result)) {
            if (kind === "field") initializers.unshift(_);
            else descriptor[key] = _;
        }
    }
    if (target) Object.defineProperty(target, contextIn.name, descriptor);
    done = true;
};
var __runInitializers = (this && this.__runInitializers) || function (thisArg, initializers, value) {
    var useValue = arguments.length > 2;
    for (var i = 0; i < initializers.length; i++) {
        value = useValue ? initializers[i].call(thisArg, value) : initializers[i].call(thisArg);
    }
    return useValue ? value : void 0;
};
import { entity, authenticated, uuid, text, decimal, boolean } from "@microsoft/rayfin-core";
/**
 * Ontology-grounded signal threshold band — mirrors the WindTurbine ontology
 * `Sensor` metadata (Unit, MinThreshold/MaxThreshold) for each measured signal.
 * Seeded from the app's single source of truth (`SIGNAL_METADATA`) so the warn /
 * alarm bands that drive turbine status are published to, and auditable from,
 * the Fabric ontology backend rather than living only in the client bundle.
 *
 * `warn` / `alarm` use a `-1` sentinel to represent "no band" (an informational
 * signal such as raw power output) because JSON cannot round-trip `Infinity`.
 */
let SensorThreshold = (() => {
    let _classDecorators = [entity(), authenticated("*")];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _id_decorators;
    let _id_initializers = [];
    let _id_extraInitializers = [];
    let _signalKey_decorators;
    let _signalKey_initializers = [];
    let _signalKey_extraInitializers = [];
    let _ontologyProperty_decorators;
    let _ontologyProperty_initializers = [];
    let _ontologyProperty_extraInitializers = [];
    let _unit_decorators;
    let _unit_initializers = [];
    let _unit_extraInitializers = [];
    let _warn_decorators;
    let _warn_initializers = [];
    let _warn_extraInitializers = [];
    let _alarm_decorators;
    let _alarm_initializers = [];
    let _alarm_extraInitializers = [];
    let _governsHealth_decorators;
    let _governsHealth_initializers = [];
    let _governsHealth_extraInitializers = [];
    var SensorThreshold = class {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
            _id_decorators = [uuid()];
            _signalKey_decorators = [text({ max: 16, unique: true })];
            _ontologyProperty_decorators = [text({ max: 48 })];
            _unit_decorators = [text({ max: 16 })];
            _warn_decorators = [decimal()];
            _alarm_decorators = [decimal()];
            _governsHealth_decorators = [boolean()];
            __esDecorate(null, null, _id_decorators, { kind: "field", name: "id", static: false, private: false, access: { has: obj => "id" in obj, get: obj => obj.id, set: (obj, value) => { obj.id = value; } }, metadata: _metadata }, _id_initializers, _id_extraInitializers);
            __esDecorate(null, null, _signalKey_decorators, { kind: "field", name: "signalKey", static: false, private: false, access: { has: obj => "signalKey" in obj, get: obj => obj.signalKey, set: (obj, value) => { obj.signalKey = value; } }, metadata: _metadata }, _signalKey_initializers, _signalKey_extraInitializers);
            __esDecorate(null, null, _ontologyProperty_decorators, { kind: "field", name: "ontologyProperty", static: false, private: false, access: { has: obj => "ontologyProperty" in obj, get: obj => obj.ontologyProperty, set: (obj, value) => { obj.ontologyProperty = value; } }, metadata: _metadata }, _ontologyProperty_initializers, _ontologyProperty_extraInitializers);
            __esDecorate(null, null, _unit_decorators, { kind: "field", name: "unit", static: false, private: false, access: { has: obj => "unit" in obj, get: obj => obj.unit, set: (obj, value) => { obj.unit = value; } }, metadata: _metadata }, _unit_initializers, _unit_extraInitializers);
            __esDecorate(null, null, _warn_decorators, { kind: "field", name: "warn", static: false, private: false, access: { has: obj => "warn" in obj, get: obj => obj.warn, set: (obj, value) => { obj.warn = value; } }, metadata: _metadata }, _warn_initializers, _warn_extraInitializers);
            __esDecorate(null, null, _alarm_decorators, { kind: "field", name: "alarm", static: false, private: false, access: { has: obj => "alarm" in obj, get: obj => obj.alarm, set: (obj, value) => { obj.alarm = value; } }, metadata: _metadata }, _alarm_initializers, _alarm_extraInitializers);
            __esDecorate(null, null, _governsHealth_decorators, { kind: "field", name: "governsHealth", static: false, private: false, access: { has: obj => "governsHealth" in obj, get: obj => obj.governsHealth, set: (obj, value) => { obj.governsHealth = value; } }, metadata: _metadata }, _governsHealth_initializers, _governsHealth_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            SensorThreshold = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        id = __runInitializers(this, _id_initializers, void 0);
        signalKey = (__runInitializers(this, _id_extraInitializers), __runInitializers(this, _signalKey_initializers, void 0));
        ontologyProperty = (__runInitializers(this, _signalKey_extraInitializers), __runInitializers(this, _ontologyProperty_initializers, void 0));
        unit = (__runInitializers(this, _ontologyProperty_extraInitializers), __runInitializers(this, _unit_initializers, void 0));
        warn = (__runInitializers(this, _unit_extraInitializers), __runInitializers(this, _warn_initializers, void 0));
        alarm = (__runInitializers(this, _warn_extraInitializers), __runInitializers(this, _alarm_initializers, void 0));
        governsHealth = (__runInitializers(this, _alarm_extraInitializers), __runInitializers(this, _governsHealth_initializers, void 0));
        constructor() {
            __runInitializers(this, _governsHealth_extraInitializers);
        }
    };
    return SensorThreshold = _classThis;
})();
export { SensorThreshold };
