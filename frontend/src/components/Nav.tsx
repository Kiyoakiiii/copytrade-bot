"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import {
  Activity,
  BookOpenCheck,
  ClipboardCheck,
  Gauge,
  LogOut,
  Shield,
  SlidersHorizontal,
} from "lucide-react";
import { apiFetch } from "@/lib/api";

const items = [
  { href: "/dashboard", label: "Dashboard", icon: Gauge },
  { href: "/leaders", label: "Leaders", icon: BookOpenCheck },
  { href: "/orders", label: "Orders", icon: Activity },
  { href: "/preflight", label: "Preflight", icon: ClipboardCheck },
  { href: "/risk", label: "Settings / Risk", icon: SlidersHorizontal }
];

export function Nav() {
  const pathname = usePathname();
  const router = useRouter();

  async function logout() {
    await apiFetch("/auth/logout", { method: "POST", body: "{}" }).catch(() => undefined);
    router.push("/login");
  }

  return (
    <aside className="flex min-h-screen w-64 flex-col border-r border-line bg-white shadow-sm">
      <div className="flex h-16 items-center gap-3 border-b border-line px-5">
        <Shield className="h-5 w-5 text-accent" />
        <span className="text-sm font-semibold tracking-normal text-ink">
          Copytrade Control
        </span>
      </div>
      <nav className="flex-1 space-y-1 px-3 py-4">
        {items.map((item) => {
          const Icon = item.icon;
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex h-10 items-center gap-3 rounded-md px-3 text-sm font-medium transition ${
                active
                  ? "bg-teal-50 text-accent shadow-sm"
                  : "text-slate-600 hover:bg-panel hover:text-ink"
              }`}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <button
        type="button"
        onClick={logout}
        className="m-3 flex h-10 items-center gap-3 rounded-md px-3 text-left text-sm text-slate-600 hover:bg-panel hover:text-ink"
        title="Logout"
      >
        <LogOut className="h-4 w-4" />
        Logout
      </button>
    </aside>
  );
}
