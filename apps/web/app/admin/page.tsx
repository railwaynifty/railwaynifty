"use client";

import Link from "next/link";
import { FormEvent, useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

type User = {
  id: number;
  email: string;
  role: "admin" | "viewer";
  is_active: boolean;
  created_at: string;
  session_active?: boolean;
  active_session_started_at?: string | null;
};

export default function AdminPage() {
  const router = useRouter();
  const [users, setUsers] = useState<User[]>([]);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<"admin" | "viewer">("viewer");
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    const response = await fetch("/api/admin/users", { cache: "no-store" });
    if (response.status === 401 || response.status === 403) {
      router.replace("/dashboard");
      return;
    }
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || "Could not load users");
    setUsers(payload.users || []);
  }, [router]);

  useEffect(() => { load().catch((e) => setError(e.message)); }, [load]);

  async function createUser(event: FormEvent) {
    event.preventDefault();
    setError("");
    const response = await fetch("/api/admin/users", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ email, password, role }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return setError(payload.detail || "Could not create user");
    setEmail(""); setPassword(""); setRole("viewer");
    await load();
  }

  async function setStatus(user: User) {
    setError("");
    const response = await fetch(`/api/admin/users/${user.id}/status`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ is_active: !user.is_active }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return setError(payload.detail || "Could not update user");
    await load();
  }

  async function revokeSession(user: User) {
    setError("");
    const response = await fetch(`/api/admin/users/${user.id}/revoke-session`, { method: "POST" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return setError(payload.detail || "Could not revoke session");
    await load();
  }

  async function resetPassword(user: User) {
    const next = window.prompt(`New password for ${user.email} (minimum 10 characters)`);
    if (!next) return;
    const response = await fetch(`/api/admin/users/${user.id}/reset-password`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ password: next }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) setError(payload.detail || "Could not reset password");
    else window.alert("Password reset successfully.");
  }

  return (
    <main>
      <header className="topbar"><span className="brand">User Administration</span><Link href="/audit">Audit Log</Link><Link href="/archive">Archive</Link><Link href="/dashboard">Dashboard</Link></header>
      <div className="page">
        {error ? <div className="error">{error}</div> : null}
        <section className="panel">
          <h2>Create user</h2>
          <form className="grid" onSubmit={createUser}>
            <div className="field"><label>Email</label><input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required /></div>
            <div className="field"><label>Temporary password</label><input type="password" minLength={10} value={password} onChange={(e) => setPassword(e.target.value)} required /></div>
            <div className="field"><label>Role</label><select value={role} onChange={(e) => setRole(e.target.value as "admin" | "viewer")}><option value="viewer">Viewer</option><option value="admin">Admin</option></select></div>
            <div className="field" style={{ alignSelf: "end" }}><button className="button" type="submit">Create user</button></div>
          </form>
        </section>
        <section className="panel">
          <h2>Users</h2>
          <div style={{ overflowX: "auto" }}>
            <table><thead><tr><th>Email</th><th>Role</th><th>Status</th><th>Viewer session</th><th>Created</th><th>Actions</th></tr></thead>
              <tbody>{users.map((user) => <tr key={user.id}>
                <td>{user.email}</td><td><span className="badge">{user.role}</span></td>
                <td><span className={`badge ${user.is_active ? "active" : "disabled"}`}>{user.is_active ? "Active" : "Disabled"}</span></td>
                <td>{user.role === "viewer" ? (user.session_active ? <><span className="badge active">In use</span><br /><small>{user.active_session_started_at ? new Date(user.active_session_started_at).toLocaleString() : ""}</small></> : <span className="badge">Signed out</span>) : "Multiple allowed"}</td>
                <td>{new Date(user.created_at).toLocaleString()}</td>
                <td><div className="actions"><button className={`button ${user.is_active ? "danger" : "secondary"}`} onClick={() => setStatus(user)}>{user.is_active ? "Disable" : "Enable"}</button>{user.role === "viewer" && user.session_active ? <button className="button secondary" onClick={() => revokeSession(user)}>Sign out session</button> : null}<button className="button secondary" onClick={() => resetPassword(user)}>Reset password</button></div></td>
              </tr>)}</tbody></table>
          </div>
        </section>
      </div>
    </main>
  );
}
