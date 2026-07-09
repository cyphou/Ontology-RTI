//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
    children: ReactNode;
    label?: string;
}

interface State {
    hasError: boolean;
}

// Guards the WebGL/Three.js scenes: if a scene throws during render (driver loss,
// shader/geometry error, etc.), the rest of the app keeps working and this shows a
// calm fallback instead of a blank screen or a crashed React tree.
export class SceneErrorBoundary extends Component<Props, State> {
    state: State = { hasError: false };

    static getDerivedStateFromError(): State {
        return { hasError: true };
    }

    componentDidCatch(error: Error, info: ErrorInfo): void {
        console.error("Scene render failed:", error, info);
    }

    render(): ReactNode {
        if (this.state.hasError) {
            return (
                <div className="flex h-full w-full flex-col items-center justify-center gap-1 bg-[#051020] p-4 text-center text-slate-400">
                    <p className="text-sm font-medium text-slate-300">{this.props.label ?? "3D view"} unavailable</p>
                    <p className="text-xs">The scene failed to render in this environment. Data, alerts and dispatch remain available.</p>
                </div>
            );
        }
        return this.props.children;
    }
}
