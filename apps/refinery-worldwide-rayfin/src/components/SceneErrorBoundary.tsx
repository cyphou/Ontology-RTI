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
    error?: Error;
}

// Guards the WebGL/Three.js scenes: if a scene throws during render (driver loss,
// shader/geometry error, etc.), the rest of the app keeps working and this shows a
// calm fallback instead of a blank screen or a crashed React tree.
export class SceneErrorBoundary extends Component<Props, State> {
    state: State = { hasError: false };

    static getDerivedStateFromError(error: Error): State {
        return { hasError: true, error };
    }

    componentDidCatch(error: Error, info: ErrorInfo): void {
        console.error("Scene render failed:", error, info);
    }

    private handleRetry = (): void => this.setState({ hasError: false, error: undefined });

    render(): ReactNode {
        if (this.state.hasError) {
            return (
                <div className="flex h-full w-full flex-col items-center justify-center gap-2 bg-[#051020] p-4 text-center text-slate-400">
                    <p className="text-sm font-medium text-slate-300">{this.props.label ?? "3D view"} unavailable</p>
                    <p className="text-xs">The scene failed to render in this environment. Data, alerts and dispatch remain available.</p>
                    {this.state.error?.message && (
                        <p className="max-w-md break-words text-[11px] text-slate-500">{this.state.error.message}</p>
                    )}
                    <button
                        type="button"
                        onClick={this.handleRetry}
                        className="mt-1 rounded border border-slate-600 bg-[#0a1830] px-3 py-1 text-xs font-medium text-slate-200 transition-colors hover:border-cyan-500 hover:text-cyan-200"
                    >
                        Retry
                    </button>
                </div>
            );
        }
        return this.props.children;
    }
}
