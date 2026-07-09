//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

export type OperatorRole = "operator" | "viewer";

export function normalizeOperatorRole(value: string | null | undefined): OperatorRole {
    return value === "viewer" ? "viewer" : "operator";
}

export function canManageDispatch(role: OperatorRole): boolean {
    return role === "operator";
}
