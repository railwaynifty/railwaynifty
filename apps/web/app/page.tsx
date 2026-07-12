import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export default async function Home() {
  const store = await cookies();
  redirect(store.has("nse360_session") ? "/dashboard" : "/login");
}
