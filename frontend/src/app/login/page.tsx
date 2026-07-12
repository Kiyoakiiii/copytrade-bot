"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";
import { Lock, LogIn } from "lucide-react";
import { apiFetch } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [totp, setTotp] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setLoading(true);
    setError("");
    try {
      await apiFetch("/auth/login", {
        method: "POST",
        body: JSON.stringify({ email, password, totp_code: totp || null })
      });
      router.push("/dashboard");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="grid min-h-screen place-items-center bg-panel p-6">
      <form onSubmit={submit} className="panel w-full max-w-sm panel-pad">
        <div className="mb-5 flex items-center gap-3">
          <div className="grid h-9 w-9 place-items-center rounded-md bg-teal-50">
            <Lock className="h-5 w-5 text-accent" />
          </div>
          <h1 className="text-lg font-semibold text-ink">Control Login</h1>
        </div>
        <div className="space-y-3">
          <input
            className="field"
            type="email"
            placeholder="Email"
            autoComplete="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            required
          />
          <input
            className="field"
            type="password"
            placeholder="Password"
            autoComplete="current-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            required
          />
          <input
            className="field"
            inputMode="numeric"
            placeholder="TOTP"
            value={totp}
            onChange={(event) => setTotp(event.target.value)}
          />
          {error ? <p className="text-sm text-danger">{error}</p> : null}
          <button className="btn btn-primary w-full" type="submit" disabled={loading}>
            <LogIn className="h-4 w-4" />
            {loading ? "Signing in" : "Sign in"}
          </button>
        </div>
      </form>
    </main>
  );
}

