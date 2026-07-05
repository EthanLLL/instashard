import { useState, useEffect } from "react";
import { useChannel } from "../hooks/useChannel";

interface DbInfo {
  id: string;
  host: string;
  port: number;
  pool_size: number;
  idle: number;
}

interface ShardInfo {
  shard: string;
  db_id: string;
  active_tx: number;
  gate: string;
}

const EMPTY_FORM = { id: "", host: "", port: "5432", username: "postgres", password: "", database: "", pool_size: "10" };

export default function Dashboard() {
  const [dbs, setDbs] = useState<DbInfo[]>([]);
  const [shards, setShards] = useState<ShardInfo[]>([]);
  const [showAddForm, setShowAddForm] = useState(false);
  const [form, setForm] = useState(EMPTY_FORM);
  const [formError, setFormError] = useState<string | null>(null);
  // id → editing pool_size string (null means not editing)
  const [editingPool, setEditingPool] = useState<Record<string, string>>({});

  const { push } = useChannel("admin:dashboard", {
    "state:snapshot": (payload) => {
      const p = payload as { dbs: DbInfo[]; shards: ShardInfo[] };
      setDbs(p.dbs);
      setShards(p.shards);
    },
    "state:update": (payload) => {
      const p = payload as { dbs?: DbInfo[]; shards?: ShardInfo[] };
      if (p.dbs) setDbs(p.dbs);
      if (p.shards) setShards(p.shards);
    },
    "db:ok": () => {
      setShowAddForm(false);
      setForm(EMPTY_FORM);
      setFormError(null);
      setEditingPool({});
    },
    "db:error": (payload) => {
      const p = payload as { reason: string };
      setFormError(p.reason);
    },
  });

  useEffect(() => {
    push("request:snapshot");
  }, [push]);

  const submitAdd = () => {
    const port = parseInt(form.port);
    const pool_size = parseInt(form.pool_size);
    if (!form.id || !form.host || !form.username || !form.database) {
      setFormError("id, host, username, database are required");
      return;
    }
    if (isNaN(port) || isNaN(pool_size)) {
      setFormError("port and pool_size must be numbers");
      return;
    }
    setFormError(null);
    push("db:add", { ...form, port, pool_size });
  };

  const commitPoolSize = (id: string) => {
    const val = parseInt(editingPool[id] ?? "");
    if (isNaN(val) || val < 0) return;
    push("db:set_pool_size", { id, pool_size: val });
  };

  return (
    <div className="p-6 space-y-8">
      <h1 className="text-2xl font-semibold">InstaShard Dashboard</h1>

      {/* ── Databases ── */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-medium">Databases</h2>
          <button
            className="text-sm px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700"
            onClick={() => { setShowAddForm((v) => !v); setFormError(null); }}
          >
            {showAddForm ? "Cancel" : "+ Add DB"}
          </button>
        </div>

        {/* Add DB form */}
        {showAddForm && (
          <div className="border rounded-lg p-4 mb-4 space-y-3 bg-gray-50">
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 text-sm">
              {(["id", "host", "port", "username", "password", "database", "pool_size"] as const).map((f) => (
                <label key={f} className="space-y-1">
                  <span className="text-xs text-gray-500">{f}</span>
                  <input
                    className="block w-full border rounded px-2 py-1 font-mono text-sm"
                    value={form[f]}
                    onChange={(e) => setForm((prev) => ({ ...prev, [f]: e.target.value }))}
                    type={f === "password" ? "password" : "text"}
                  />
                </label>
              ))}
            </div>
            {formError && <p className="text-red-500 text-xs">{formError}</p>}
            <button
              className="px-4 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
              onClick={submitAdd}
            >
              Add
            </button>
          </div>
        )}

        {/* DB cards */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {dbs.map((db) => {
            const isEditing = editingPool[db.id] !== undefined;
            return (
              <div key={db.id} className="border rounded-lg p-4 space-y-2">
                <div className="font-mono font-medium">{db.id}</div>
                <div className="text-sm text-gray-500">{db.host}:{db.port}</div>
                <div className="flex items-center justify-between text-sm mt-1">
                  <span className="text-green-600">Idle: {db.idle}</span>
                  {/* Pool size inline edit */}
                  <div className="flex items-center gap-1">
                    <span className="text-gray-500 text-xs">Pool:</span>
                    {isEditing ? (
                      <>
                        <input
                          className="w-14 border rounded px-1 py-0.5 text-xs font-mono text-center"
                          value={editingPool[db.id]}
                          autoFocus
                          onChange={(e) =>
                            setEditingPool((p) => ({ ...p, [db.id]: e.target.value }))
                          }
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commitPoolSize(db.id);
                            if (e.key === "Escape")
                              setEditingPool((p) => { const n = { ...p }; delete n[db.id]; return n; });
                          }}
                        />
                        <button
                          className="text-green-600 text-xs hover:text-green-700"
                          onClick={() => commitPoolSize(db.id)}
                        >✓</button>
                        <button
                          className="text-gray-400 text-xs hover:text-gray-600"
                          onClick={() => setEditingPool((p) => { const n = { ...p }; delete n[db.id]; return n; })}
                        >✗</button>
                      </>
                    ) : (
                      <button
                        className="font-mono text-xs underline decoration-dashed text-gray-700 hover:text-blue-600"
                        onClick={() =>
                          setEditingPool((p) => ({ ...p, [db.id]: String(db.pool_size) }))
                        }
                      >
                        {db.pool_size}
                      </button>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* ── Shards ── */}
      <section>
        <h2 className="text-lg font-medium mb-3">Shards</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="py-2 pr-4">Shard</th>
                <th className="py-2 pr-4">DB</th>
                <th className="py-2 pr-4">Active TX</th>
                <th className="py-2">Gate</th>
              </tr>
            </thead>
            <tbody>
              {shards.map((s) => (
                <tr key={s.shard} className="border-b hover:bg-gray-50">
                  <td className="py-2 pr-4 font-mono">{s.shard}</td>
                  <td className="py-2 pr-4 font-mono">{s.db_id}</td>
                  <td className="py-2 pr-4">{s.active_tx}</td>
                  <td className="py-2"><GateBadge status={s.gate} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function GateBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    open: "bg-green-100 text-green-700",
    closing: "bg-yellow-100 text-yellow-700",
    closed: "bg-red-100 text-red-700",
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[status] ?? "bg-gray-100 text-gray-600"}`}>
      {status}
    </span>
  );
}
