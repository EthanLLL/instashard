import { useState } from "react";
import Dashboard from "./pages/Dashboard";
import Migration from "./pages/Migration";

type Page = "dashboard" | "migration";

export default function App() {
  const [page, setPage] = useState<Page>("dashboard");

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      {/* Nav */}
      <nav className="border-b bg-white px-6 py-3 flex gap-6 items-center">
        <span className="font-semibold text-blue-600">InstaShard</span>
        {(["dashboard", "migration"] as Page[]).map((p) => (
          <button
            key={p}
            onClick={() => setPage(p)}
            className={`capitalize text-sm px-3 py-1 rounded ${
              page === p
                ? "bg-blue-50 text-blue-600 font-medium"
                : "text-gray-500 hover:text-gray-800"
            }`}
          >
            {p}
          </button>
        ))}
      </nav>

      {/* Page */}
      <main className="max-w-6xl mx-auto">
        {page === "dashboard" && <Dashboard />}
        {page === "migration" && <Migration />}
      </main>
    </div>
  );
}
