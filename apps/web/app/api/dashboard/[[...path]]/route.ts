import { NextRequest, NextResponse } from "next/server";
import { backendBase, internalHeaders, sessionToken } from "@/lib/backend";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

const AUDIT_HOOK = String.raw`<script>
(function () {
  const clean = (value, limit) => String(value || "").replace(/\s+/g, " ").trim().slice(0, limit || 300);
  const activePage = () => {
    const active = document.querySelector(".tab.active");
    return clean(active ? (active.textContent || active.getAttribute("data-tab")) : "NSE 360 Dashboard", 200);
  };
  const sendAudit = (eventType, page, action, target, details) => {
    fetch("/api/audit/event", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        event_type: clean(eventType, 100),
        page: clean(page || activePage(), 200),
        action: clean(action, 300),
        target: clean(target, 500),
        details: details || {}
      }),
      credentials: "same-origin",
      keepalive: true
    }).catch(() => undefined);
  };

  const recordInitialPage = () => sendAudit("page_view", activePage(), "Opened dashboard tab", "initial", {});
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", recordInitialPage, { once: true });
  else recordInitialPage();

  document.addEventListener("click", (event) => {
    if (!event.isTrusted) return;
    const raw = event.target;
    if (!(raw instanceof Element)) return;
    const control = raw.closest(".tab, button, a, [role='button'], [data-open-tab]");
    if (!control) return;
    const label = clean(control.textContent || control.getAttribute("aria-label") || control.getAttribute("title") || control.id, 300);
    const id = clean(control.id || control.getAttribute("data-tab") || control.getAttribute("data-open-tab") || "", 200);
    const isTab = control.classList.contains("tab") || control.hasAttribute("data-open-tab");
    const isExport = /export|download/i.test(label + " " + id);
    if (isTab) {
      sendAudit("page_view", label || activePage(), "Opened dashboard tab", id, {});
    } else if (isExport) {
      sendAudit("export_action", activePage(), label || "Export", id, {});
    } else {
      sendAudit("button_action", activePage(), label || "Button click", id, {});
    }
  }, true);

  let changeTimer = 0;
  document.addEventListener("change", (event) => {
    if (!event.isTrusted) return;
    const control = event.target;
    if (!(control instanceof HTMLInputElement || control instanceof HTMLSelectElement || control instanceof HTMLTextAreaElement)) return;
    if (control instanceof HTMLInputElement && ["password", "hidden", "range"].includes(control.type)) return;
    window.clearTimeout(changeTimer);
    changeTimer = window.setTimeout(() => {
      const id = clean(control.id || control.name || control.getAttribute("aria-label") || "filter", 200);
      const labelElement = control.id ? document.querySelector('label[for="' + CSS.escape(control.id) + '"]') : null;
      const label = clean(labelElement ? labelElement.textContent : id, 200);
      const value = control instanceof HTMLInputElement && control.type === "checkbox" ? String(control.checked) : clean(control.value, 300);
      sendAudit("filter_change", activePage(), "Changed " + (label || id), id, { value });
    }, 250);
  }, true);
})();
</script>`;

async function handle(request: NextRequest, context: { params: Promise<{ path?: string[] }> }) {
  const token = sessionToken(request);
  if (!token) return NextResponse.json({ detail: "Not authenticated" }, { status: 401 });

  const { path } = await context.params;
  const pieces = path ?? [];
  const suffix = pieces.join("/");
  const target = new URL(`${backendBase()}/legacy/${suffix}`);
  request.nextUrl.searchParams.forEach((value, key) => target.searchParams.append(key, value));

  const headers = internalHeaders();
  headers.set("authorization", `Bearer ${token}`);
  const forwardedFor = request.headers.get("x-forwarded-for") ?? request.headers.get("x-real-ip") ?? "";
  const userAgent = request.headers.get("user-agent") ?? "";
  if (forwardedFor) headers.set("x-client-ip", forwardedFor.split(",")[0].trim());
  if (userAgent) headers.set("x-client-user-agent", userAgent);
  const incomingType = request.headers.get("content-type");
  if (incomingType) headers.set("content-type", incomingType);

  const method = request.method;
  const body = method === "GET" || method === "HEAD" ? undefined : await request.arrayBuffer();
  const response = await fetch(target, { method, headers, body, cache: "no-store", redirect: "manual" });
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";

  if (contentType.includes("text/html")) {
    let html = await response.text();
    html = html
      .replaceAll('"/api/', '"/api/dashboard/api/')
      .replaceAll("'/api/", "'/api/dashboard/api/")
      .replaceAll("`/api/", "`/api/dashboard/api/")
      .replaceAll('href="/"', 'href="/api/dashboard/"');
    const bodyCloseIndex = html.toLowerCase().lastIndexOf("</body>");

html =
  bodyCloseIndex >= 0
    ? `${html.slice(0, bodyCloseIndex)}${AUDIT_HOOK}${html.slice(bodyCloseIndex)}`
    : html + AUDIT_HOOK;
    return new NextResponse(html, {
      status: response.status,
      headers: {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
        "x-frame-options": "SAMEORIGIN",
      },
    });
  }

  const outputHeaders = new Headers();
  for (const name of ["content-type", "content-disposition", "cache-control"]) {
    const value = response.headers.get(name);
    if (value) outputHeaders.set(name, value);
  }
  outputHeaders.set("cache-control", "no-store");
  return new NextResponse(response.body, { status: response.status, headers: outputHeaders });
}

export const GET = handle;
export const POST = handle;
export const PATCH = handle;
export const DELETE = handle;
