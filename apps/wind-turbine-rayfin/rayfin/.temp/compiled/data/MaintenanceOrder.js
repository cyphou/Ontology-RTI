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
import { entity, authenticated, uuid, text, int, date } from "@microsoft/rayfin-core";
/**
 * Writeback target — a predictive maintenance work order raised against an
 * ontology turbine. This is the durable store behind the "Raise work order"
 * action, closing the loop from the anomaly escalation forecast + what-if
 * simulator to a tracked, structured intervention (component, priority,
 * planned curtailment/downtime and its projected energy impact).
 */
let MaintenanceOrder = (() => {
    let _classDecorators = [entity(), authenticated("*")];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _id_decorators;
    let _id_initializers = [];
    let _id_extraInitializers = [];
    let _turbineId_decorators;
    let _turbineId_initializers = [];
    let _turbineId_extraInitializers = [];
    let _siteId_decorators;
    let _siteId_initializers = [];
    let _siteId_extraInitializers = [];
    let _component_decorators;
    let _component_initializers = [];
    let _component_extraInitializers = [];
    let _priority_decorators;
    let _priority_initializers = [];
    let _priority_extraInitializers = [];
    let _status_decorators;
    let _status_initializers = [];
    let _status_extraInitializers = [];
    let _curtailPct_decorators;
    let _curtailPct_initializers = [];
    let _curtailPct_extraInitializers = [];
    let _downtimeTicks_decorators;
    let _downtimeTicks_initializers = [];
    let _downtimeTicks_extraInitializers = [];
    let _projectedDeltaKwt_decorators;
    let _projectedDeltaKwt_initializers = [];
    let _projectedDeltaKwt_extraInitializers = [];
    let _assignee_decorators;
    let _assignee_initializers = [];
    let _assignee_extraInitializers = [];
    let _note_decorators;
    let _note_initializers = [];
    let _note_extraInitializers = [];
    let _createdAt_decorators;
    let _createdAt_initializers = [];
    let _createdAt_extraInitializers = [];
    var MaintenanceOrder = class {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
            _id_decorators = [uuid()];
            _turbineId_decorators = [text({ max: 48 })];
            _siteId_decorators = [text({ max: 32 })];
            _component_decorators = [text({ max: 16 })];
            _priority_decorators = [text({ max: 4 })];
            _status_decorators = [text({ max: 16 })];
            _curtailPct_decorators = [int()];
            _downtimeTicks_decorators = [int()];
            _projectedDeltaKwt_decorators = [int()];
            _assignee_decorators = [text({ max: 120 })];
            _note_decorators = [text({ max: 500 })];
            _createdAt_decorators = [date()];
            __esDecorate(null, null, _id_decorators, { kind: "field", name: "id", static: false, private: false, access: { has: obj => "id" in obj, get: obj => obj.id, set: (obj, value) => { obj.id = value; } }, metadata: _metadata }, _id_initializers, _id_extraInitializers);
            __esDecorate(null, null, _turbineId_decorators, { kind: "field", name: "turbineId", static: false, private: false, access: { has: obj => "turbineId" in obj, get: obj => obj.turbineId, set: (obj, value) => { obj.turbineId = value; } }, metadata: _metadata }, _turbineId_initializers, _turbineId_extraInitializers);
            __esDecorate(null, null, _siteId_decorators, { kind: "field", name: "siteId", static: false, private: false, access: { has: obj => "siteId" in obj, get: obj => obj.siteId, set: (obj, value) => { obj.siteId = value; } }, metadata: _metadata }, _siteId_initializers, _siteId_extraInitializers);
            __esDecorate(null, null, _component_decorators, { kind: "field", name: "component", static: false, private: false, access: { has: obj => "component" in obj, get: obj => obj.component, set: (obj, value) => { obj.component = value; } }, metadata: _metadata }, _component_initializers, _component_extraInitializers);
            __esDecorate(null, null, _priority_decorators, { kind: "field", name: "priority", static: false, private: false, access: { has: obj => "priority" in obj, get: obj => obj.priority, set: (obj, value) => { obj.priority = value; } }, metadata: _metadata }, _priority_initializers, _priority_extraInitializers);
            __esDecorate(null, null, _status_decorators, { kind: "field", name: "status", static: false, private: false, access: { has: obj => "status" in obj, get: obj => obj.status, set: (obj, value) => { obj.status = value; } }, metadata: _metadata }, _status_initializers, _status_extraInitializers);
            __esDecorate(null, null, _curtailPct_decorators, { kind: "field", name: "curtailPct", static: false, private: false, access: { has: obj => "curtailPct" in obj, get: obj => obj.curtailPct, set: (obj, value) => { obj.curtailPct = value; } }, metadata: _metadata }, _curtailPct_initializers, _curtailPct_extraInitializers);
            __esDecorate(null, null, _downtimeTicks_decorators, { kind: "field", name: "downtimeTicks", static: false, private: false, access: { has: obj => "downtimeTicks" in obj, get: obj => obj.downtimeTicks, set: (obj, value) => { obj.downtimeTicks = value; } }, metadata: _metadata }, _downtimeTicks_initializers, _downtimeTicks_extraInitializers);
            __esDecorate(null, null, _projectedDeltaKwt_decorators, { kind: "field", name: "projectedDeltaKwt", static: false, private: false, access: { has: obj => "projectedDeltaKwt" in obj, get: obj => obj.projectedDeltaKwt, set: (obj, value) => { obj.projectedDeltaKwt = value; } }, metadata: _metadata }, _projectedDeltaKwt_initializers, _projectedDeltaKwt_extraInitializers);
            __esDecorate(null, null, _assignee_decorators, { kind: "field", name: "assignee", static: false, private: false, access: { has: obj => "assignee" in obj, get: obj => obj.assignee, set: (obj, value) => { obj.assignee = value; } }, metadata: _metadata }, _assignee_initializers, _assignee_extraInitializers);
            __esDecorate(null, null, _note_decorators, { kind: "field", name: "note", static: false, private: false, access: { has: obj => "note" in obj, get: obj => obj.note, set: (obj, value) => { obj.note = value; } }, metadata: _metadata }, _note_initializers, _note_extraInitializers);
            __esDecorate(null, null, _createdAt_decorators, { kind: "field", name: "createdAt", static: false, private: false, access: { has: obj => "createdAt" in obj, get: obj => obj.createdAt, set: (obj, value) => { obj.createdAt = value; } }, metadata: _metadata }, _createdAt_initializers, _createdAt_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            MaintenanceOrder = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        id = __runInitializers(this, _id_initializers, void 0);
        turbineId = (__runInitializers(this, _id_extraInitializers), __runInitializers(this, _turbineId_initializers, void 0));
        siteId = (__runInitializers(this, _turbineId_extraInitializers), __runInitializers(this, _siteId_initializers, void 0));
        component = (__runInitializers(this, _siteId_extraInitializers), __runInitializers(this, _component_initializers, void 0));
        priority = (__runInitializers(this, _component_extraInitializers), __runInitializers(this, _priority_initializers, void 0));
        status = (__runInitializers(this, _priority_extraInitializers), __runInitializers(this, _status_initializers, void 0));
        curtailPct = (__runInitializers(this, _status_extraInitializers), __runInitializers(this, _curtailPct_initializers, void 0));
        downtimeTicks = (__runInitializers(this, _curtailPct_extraInitializers), __runInitializers(this, _downtimeTicks_initializers, void 0));
        projectedDeltaKwt = (__runInitializers(this, _downtimeTicks_extraInitializers), __runInitializers(this, _projectedDeltaKwt_initializers, void 0));
        assignee = (__runInitializers(this, _projectedDeltaKwt_extraInitializers), __runInitializers(this, _assignee_initializers, void 0));
        note = (__runInitializers(this, _assignee_extraInitializers), __runInitializers(this, _note_initializers, void 0));
        createdAt = (__runInitializers(this, _note_extraInitializers), __runInitializers(this, _createdAt_initializers, void 0));
        constructor() {
            __runInitializers(this, _createdAt_extraInitializers);
        }
    };
    return MaintenanceOrder = _classThis;
})();
export { MaintenanceOrder };
