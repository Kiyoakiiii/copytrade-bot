"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import {
  BookOpenCheck,
  Gauge,
  LogOut,
  Shield,
  SlidersHorizontal,
} from "lucide-react";
import { apiFetch } from "@/lib/api";

const items = [
  { href: "/dashboard", label: "Overview", icon: Gauge },
  { href: "/leaders", label: "Leader Desk", icon: BookOpenCheck },
  { href: "/risk", label: "System & Risk", icon: SlidersHorizontal }
];

export function Nav() {
  const pathname = usePathname();
  const router = useRouter();

  async function logout() {
    await apiFetch("/auth/logout", { method: "POST", body: "{}" }).catch(() => undefined);
    router.push("/login");
  }

  return (
    <aside className="sticky top-0 z-40 flex h-auto w-full flex-col bg-navy text-white shadow-xl lg:h-screen lg:w-64 lg:shrink-0">
      <div className="flex h-16 items-center gap-3 border-b border-white/10 px-5">
        <span className="grid h-9 w-9 place-items-center rounded-xl bg-accent/20 ring-1 ring-accent/40">
          <Shield className="h-5 w-5 text-teal-300" />
        </span>
        <span>
          <span className="block text-sm font-semibold tracking-wide">Copytrade</span>
          <span className="block text-[10px] uppercase tracking-[0.2em] text-slate-400">Operations Console</span>
        </span>
      </div>
      <nav className="flex flex-1 gap-1 overflow-x-auto px-3 py-3 lg:block lg:space-y-1 lg:overflow-visible lg:py-5">
        {items.map((item) => {
          const Icon = item.icon;
          const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex h-10 shrink-0 items-center gap-3 rounded-lg px-3 text-sm font-medium transition lg:w-full ${
                active
                  ? "bg-white/10 text-white shadow-sm ring-1 ring-white/10"
                  : "text-slate-400 hover:bg-white/10 hover:text-white"
              }`}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="hidden border-t border-white/10 p-3 lg:block">
        <div className="mb-2 px-3 text-[10px] uppercase tracking-[0.18em] text-slate-500">Secure session</div>
      <button
        type="button"
        onClick={logout}
        className="flex h-10 w-full items-center gap-3 rounded-lg px-3 text-left text-sm text-slate-400 hover:bg-white/10 hover:text-white"
        title="Logout"
      >
        <LogOut className="h-4 w-4" />
        Logout
      </button>
      </div>
    </aside>
  );
}
