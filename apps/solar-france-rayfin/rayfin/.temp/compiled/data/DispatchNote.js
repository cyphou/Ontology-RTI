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
 * Writeback target — an operator dispatch note attached to an ontology
 * turbine. This is the durable store behind the "Writeback This Turbine"
 * action. Records are keyed to ontology entity ids (turbineId / siteId).
 */
let DispatchNote = (() => {
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
    let _status_decorators;
    let _status_initializers = [];
    let _status_extraInitializers = [];
    let _powerKw_decorators;
    let _powerKw_initializers = [];
    let _powerKw_extraInitializers = [];
    let _note_decorators;
    let _note_initializers = [];
    let _note_extraInitializers = [];
    let _author_decorators;
    let _author_initializers = [];
    let _author_extraInitializers = [];
    let _createdAt_decorators;
    let _createdAt_initializers = [];
    let _createdAt_extraInitializers = [];
    var DispatchNote = class {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
            _id_decorators = [uuid()];
            _turbineId_decorators = [text({ max: 48 })];
            _siteId_decorators = [text({ max: 32 })];
            _status_decorators = [text({ max: 16 })];
            _powerKw_decorators = [int()];
            _note_decorators = [text({ max: 500 })];
            _author_decorators = [text({ max: 120 })];
            _createdAt_decorators = [date()];
            __esDecorate(null, null, _id_decorators, { kind: "field", name: "id", static: false, private: false, access: { has: obj => "id" in obj, get: obj => obj.id, set: (obj, value) => { obj.id = value; } }, metadata: _metadata }, _id_initializers, _id_extraInitializers);
            __esDecorate(null, null, _turbineId_decorators, { kind: "field", name: "turbineId", static: false, private: false, access: { has: obj => "turbineId" in obj, get: obj => obj.turbineId, set: (obj, value) => { obj.turbineId = value; } }, metadata: _metadata }, _turbineId_initializers, _turbineId_extraInitializers);
            __esDecorate(null, null, _siteId_decorators, { kind: "field", name: "siteId", static: false, private: false, access: { has: obj => "siteId" in obj, get: obj => obj.siteId, set: (obj, value) => { obj.siteId = value; } }, metadata: _metadata }, _siteId_initializers, _siteId_extraInitializers);
            __esDecorate(null, null, _status_decorators, { kind: "field", name: "status", static: false, private: false, access: { has: obj => "status" in obj, get: obj => obj.status, set: (obj, value) => { obj.status = value; } }, metadata: _metadata }, _status_initializers, _status_extraInitializers);
            __esDecorate(null, null, _powerKw_decorators, { kind: "field", name: "powerKw", static: false, private: false, access: { has: obj => "powerKw" in obj, get: obj => obj.powerKw, set: (obj, value) => { obj.powerKw = value; } }, metadata: _metadata }, _powerKw_initializers, _powerKw_extraInitializers);
            __esDecorate(null, null, _note_decorators, { kind: "field", name: "note", static: false, private: false, access: { has: obj => "note" in obj, get: obj => obj.note, set: (obj, value) => { obj.note = value; } }, metadata: _metadata }, _note_initializers, _note_extraInitializers);
            __esDecorate(null, null, _author_decorators, { kind: "field", name: "author", static: false, private: false, access: { has: obj => "author" in obj, get: obj => obj.author, set: (obj, value) => { obj.author = value; } }, metadata: _metadata }, _author_initializers, _author_extraInitializers);
            __esDecorate(null, null, _createdAt_decorators, { kind: "field", name: "createdAt", static: false, private: false, access: { has: obj => "createdAt" in obj, get: obj => obj.createdAt, set: (obj, value) => { obj.createdAt = value; } }, metadata: _metadata }, _createdAt_initializers, _createdAt_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            DispatchNote = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        id = __runInitializers(this, _id_initializers, void 0);
        turbineId = (__runInitializers(this, _id_extraInitializers), __runInitializers(this, _turbineId_initializers, void 0));
        siteId = (__runInitializers(this, _turbineId_extraInitializers), __runInitializers(this, _siteId_initializers, void 0));
        status = (__runInitializers(this, _siteId_extraInitializers), __runInitializers(this, _status_initializers, void 0));
        powerKw = (__runInitializers(this, _status_extraInitializers), __runInitializers(this, _powerKw_initializers, void 0));
        note = (__runInitializers(this, _powerKw_extraInitializers), __runInitializers(this, _note_initializers, void 0));
        author = (__runInitializers(this, _note_extraInitializers), __runInitializers(this, _author_initializers, void 0));
        createdAt = (__runInitializers(this, _author_extraInitializers), __runInitializers(this, _createdAt_initializers, void 0));
        constructor() {
            __runInitializers(this, _createdAt_extraInitializers);
        }
    };
    return DispatchNote = _classThis;
})();
export { DispatchNote };
