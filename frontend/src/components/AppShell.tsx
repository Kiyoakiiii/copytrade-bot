import { Nav } from "@/components/Nav";
import type { ReactNode } from "react";

export function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-[#eef2f7] lg:flex">
      <Nav />
      <main className="min-w-0 flex-1 px-4 py-5 sm:px-6 lg:px-8 lg:py-7">{children}</main>
    </div>
  );
}
