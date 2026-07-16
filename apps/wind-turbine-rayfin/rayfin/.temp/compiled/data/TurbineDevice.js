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
import { entity, authenticated, uuid, text, decimal, int } from "@microsoft/rayfin-core";
/**
 * Ontology graph leaf for digital twin component hierarchy.
 *
 * Stores second-level device nodes under each turbine component so the twin view
 * can render and inspect a backend-driven component -> device graph instead of
 * a hard-coded client-only structure.
 */
let TurbineDevice = (() => {
    let _classDecorators = [entity(), authenticated("*")];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _id_decorators;
    let _id_initializers = [];
    let _id_extraInitializers = [];
    let _deviceKey_decorators;
    let _deviceKey_initializers = [];
    let _deviceKey_extraInitializers = [];
    let _component_decorators;
    let _component_initializers = [];
    let _component_extraInitializers = [];
    let _label_decorators;
    let _label_initializers = [];
    let _label_extraInitializers = [];
    let _property_decorators;
    let _property_initializers = [];
    let _property_extraInitializers = [];
    let _unit_decorators;
    let _unit_initializers = [];
    let _unit_extraInitializers = [];
    let _note_decorators;
    let _note_initializers = [];
    let _note_extraInitializers = [];
    let _anchorX_decorators;
    let _anchorX_initializers = [];
    let _anchorX_extraInitializers = [];
    let _anchorY_decorators;
    let _anchorY_initializers = [];
    let _anchorY_extraInitializers = [];
    let _anchorZ_decorators;
    let _anchorZ_initializers = [];
    let _anchorZ_extraInitializers = [];
    let _lookAtX_decorators;
    let _lookAtX_initializers = [];
    let _lookAtX_extraInitializers = [];
    let _lookAtY_decorators;
    let _lookAtY_initializers = [];
    let _lookAtY_extraInitializers = [];
    let _lookAtZ_decorators;
    let _lookAtZ_initializers = [];
    let _lookAtZ_extraInitializers = [];
    let _offsetX_decorators;
    let _offsetX_initializers = [];
    let _offsetX_extraInitializers = [];
    let _offsetY_decorators;
    let _offsetY_initializers = [];
    let _offsetY_extraInitializers = [];
    let _offsetZ_decorators;
    let _offsetZ_initializers = [];
    let _offsetZ_extraInitializers = [];
    let _zoom_decorators;
    let _zoom_initializers = [];
    let _zoom_extraInitializers = [];
    let _sortOrder_decorators;
    let _sortOrder_initializers = [];
    let _sortOrder_extraInitializers = [];
    var TurbineDevice = class {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
            _id_decorators = [uuid()];
            _deviceKey_decorators = [text({ max: 64, unique: true })];
            _component_decorators = [text({ max: 24 })];
            _label_decorators = [text({ max: 64 })];
            _property_decorators = [text({ max: 64 })];
            _unit_decorators = [text({ max: 16 })];
            _note_decorators = [text({ max: 260 })];
            _anchorX_decorators = [decimal()];
            _anchorY_decorators = [decimal()];
            _anchorZ_decorators = [decimal()];
            _lookAtX_decorators = [decimal()];
            _lookAtY_decorators = [decimal()];
            _lookAtZ_decorators = [decimal()];
            _offsetX_decorators = [decimal()];
            _offsetY_decorators = [decimal()];
            _offsetZ_decorators = [decimal()];
            _zoom_decorators = [decimal()];
            _sortOrder_decorators = [int()];
            __esDecorate(null, null, _id_decorators, { kind: "field", name: "id", static: false, private: false, access: { has: obj => "id" in obj, get: obj => obj.id, set: (obj, value) => { obj.id = value; } }, metadata: _metadata }, _id_initializers, _id_extraInitializers);
            __esDecorate(null, null, _deviceKey_decorators, { kind: "field", name: "deviceKey", static: false, private: false, access: { has: obj => "deviceKey" in obj, get: obj => obj.deviceKey, set: (obj, value) => { obj.deviceKey = value; } }, metadata: _metadata }, _deviceKey_initializers, _deviceKey_extraInitializers);
            __esDecorate(null, null, _component_decorators, { kind: "field", name: "component", static: false, private: false, access: { has: obj => "component" in obj, get: obj => obj.component, set: (obj, value) => { obj.component = value; } }, metadata: _metadata }, _component_initializers, _component_extraInitializers);
            __esDecorate(null, null, _label_decorators, { kind: "field", name: "label", static: false, private: false, access: { has: obj => "label" in obj, get: obj => obj.label, set: (obj, value) => { obj.label = value; } }, metadata: _metadata }, _label_initializers, _label_extraInitializers);
            __esDecorate(null, null, _property_decorators, { kind: "field", name: "property", static: false, private: false, access: { has: obj => "property" in obj, get: obj => obj.property, set: (obj, value) => { obj.property = value; } }, metadata: _metadata }, _property_initializers, _property_extraInitializers);
            __esDecorate(null, null, _unit_decorators, { kind: "field", name: "unit", static: false, private: false, access: { has: obj => "unit" in obj, get: obj => obj.unit, set: (obj, value) => { obj.unit = value; } }, metadata: _metadata }, _unit_initializers, _unit_extraInitializers);
            __esDecorate(null, null, _note_decorators, { kind: "field", name: "note", static: false, private: false, access: { has: obj => "note" in obj, get: obj => obj.note, set: (obj, value) => { obj.note = value; } }, metadata: _metadata }, _note_initializers, _note_extraInitializers);
            __esDecorate(null, null, _anchorX_decorators, { kind: "field", name: "anchorX", static: false, private: false, access: { has: obj => "anchorX" in obj, get: obj => obj.anchorX, set: (obj, value) => { obj.anchorX = value; } }, metadata: _metadata }, _anchorX_initializers, _anchorX_extraInitializers);
            __esDecorate(null, null, _anchorY_decorators, { kind: "field", name: "anchorY", static: false, private: false, access: { has: obj => "anchorY" in obj, get: obj => obj.anchorY, set: (obj, value) => { obj.anchorY = value; } }, metadata: _metadata }, _anchorY_initializers, _anchorY_extraInitializers);
            __esDecorate(null, null, _anchorZ_decorators, { kind: "field", name: "anchorZ", static: false, private: false, access: { has: obj => "anchorZ" in obj, get: obj => obj.anchorZ, set: (obj, value) => { obj.anchorZ = value; } }, metadata: _metadata }, _anchorZ_initializers, _anchorZ_extraInitializers);
            __esDecorate(null, null, _lookAtX_decorators, { kind: "field", name: "lookAtX", static: false, private: false, access: { has: obj => "lookAtX" in obj, get: obj => obj.lookAtX, set: (obj, value) => { obj.lookAtX = value; } }, metadata: _metadata }, _lookAtX_initializers, _lookAtX_extraInitializers);
            __esDecorate(null, null, _lookAtY_decorators, { kind: "field", name: "lookAtY", static: false, private: false, access: { has: obj => "lookAtY" in obj, get: obj => obj.lookAtY, set: (obj, value) => { obj.lookAtY = value; } }, metadata: _metadata }, _lookAtY_initializers, _lookAtY_extraInitializers);
            __esDecorate(null, null, _lookAtZ_decorators, { kind: "field", name: "lookAtZ", static: false, private: false, access: { has: obj => "lookAtZ" in obj, get: obj => obj.lookAtZ, set: (obj, value) => { obj.lookAtZ = value; } }, metadata: _metadata }, _lookAtZ_initializers, _lookAtZ_extraInitializers);
            __esDecorate(null, null, _offsetX_decorators, { kind: "field", name: "offsetX", static: false, private: false, access: { has: obj => "offsetX" in obj, get: obj => obj.offsetX, set: (obj, value) => { obj.offsetX = value; } }, metadata: _metadata }, _offsetX_initializers, _offsetX_extraInitializers);
            __esDecorate(null, null, _offsetY_decorators, { kind: "field", name: "offsetY", static: false, private: false, access: { has: obj => "offsetY" in obj, get: obj => obj.offsetY, set: (obj, value) => { obj.offsetY = value; } }, metadata: _metadata }, _offsetY_initializers, _offsetY_extraInitializers);
            __esDecorate(null, null, _offsetZ_decorators, { kind: "field", name: "offsetZ", static: false, private: false, access: { has: obj => "offsetZ" in obj, get: obj => obj.offsetZ, set: (obj, value) => { obj.offsetZ = value; } }, metadata: _metadata }, _offsetZ_initializers, _offsetZ_extraInitializers);
            __esDecorate(null, null, _zoom_decorators, { kind: "field", name: "zoom", static: false, private: false, access: { has: obj => "zoom" in obj, get: obj => obj.zoom, set: (obj, value) => { obj.zoom = value; } }, metadata: _metadata }, _zoom_initializers, _zoom_extraInitializers);
            __esDecorate(null, null, _sortOrder_decorators, { kind: "field", name: "sortOrder", static: false, private: false, access: { has: obj => "sortOrder" in obj, get: obj => obj.sortOrder, set: (obj, value) => { obj.sortOrder = value; } }, metadata: _metadata }, _sortOrder_initializers, _sortOrder_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            TurbineDevice = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        id = __runInitializers(this, _id_initializers, void 0);
        deviceKey = (__runInitializers(this, _id_extraInitializers), __runInitializers(this, _deviceKey_initializers, void 0));
        component = (__runInitializers(this, _deviceKey_extraInitializers), __runInitializers(this, _component_initializers, void 0));
        label = (__runInitializers(this, _component_extraInitializers), __runInitializers(this, _label_initializers, void 0));
        property = (__runInitializers(this, _label_extraInitializers), __runInitializers(this, _property_initializers, void 0));
        unit = (__runInitializers(this, _property_extraInitializers), __runInitializers(this, _unit_initializers, void 0));
        note = (__runInitializers(this, _unit_extraInitializers), __runInitializers(this, _note_initializers, void 0));
        anchorX = (__runInitializers(this, _note_extraInitializers), __runInitializers(this, _anchorX_initializers, void 0));
        anchorY = (__runInitializers(this, _anchorX_extraInitializers), __runInitializers(this, _anchorY_initializers, void 0));
        anchorZ = (__runInitializers(this, _anchorY_extraInitializers), __runInitializers(this, _anchorZ_initializers, void 0));
        lookAtX = (__runInitializers(this, _anchorZ_extraInitializers), __runInitializers(this, _lookAtX_initializers, void 0));
        lookAtY = (__runInitializers(this, _lookAtX_extraInitializers), __runInitializers(this, _lookAtY_initializers, void 0));
        lookAtZ = (__runInitializers(this, _lookAtY_extraInitializers), __runInitializers(this, _lookAtZ_initializers, void 0));
        offsetX = (__runInitializers(this, _lookAtZ_extraInitializers), __runInitializers(this, _offsetX_initializers, void 0));
        offsetY = (__runInitializers(this, _offsetX_extraInitializers), __runInitializers(this, _offsetY_initializers, void 0));
        offsetZ = (__runInitializers(this, _offsetY_extraInitializers), __runInitializers(this, _offsetZ_initializers, void 0));
        zoom = (__runInitializers(this, _offsetZ_extraInitializers), __runInitializers(this, _zoom_initializers, void 0));
        sortOrder = (__runInitializers(this, _zoom_extraInitializers), __runInitializers(this, _sortOrder_initializers, void 0));
        constructor() {
            __runInitializers(this, _sortOrder_extraInitializers);
        }
    };
    return TurbineDevice = _classThis;
})();
export { TurbineDevice };
