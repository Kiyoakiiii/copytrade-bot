import { Nav } from "@/components/Nav";
import type { ReactNode } from "react";

export function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-screen bg-slate-100">
      <Nav />
      <main className="min-w-0 flex-1 p-5 lg:p-6">{children}</main>
    </div>
  );
}
