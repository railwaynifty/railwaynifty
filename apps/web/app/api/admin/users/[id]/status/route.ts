import { NextRequest } from "next/server";
import { proxyJson } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function PATCH(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return proxyJson(request, `/admin/users/${encodeURIComponent(id)}/status`, {
    method: "PATCH",
    body: await request.text(),
  });
}
