import { useState } from "react";
import { useChannel } from "../hooks/useChannel";

interface MigrationStatus {
  shard: string;
  source_db_id: string | null;
  target_db_id: string | null;
  phase: string;
  lag_bytes: number | null;
  gate: string | null;
}

interface MigrationEvent {
  shard: string;
  status: string;
  detail: string | null;
  ts: number;
}

export default function Migration() {
  const [migrations, setMigrations] = useState<MigrationStatus[]>([]);
  const [events, setEvents] = useState<MigrationEvent[]>([]);
  const [shard, setShard] = useState("");
  const [targetDb, setTargetDb] = useState("");

  const { push } = useChannel("admin:migration", {
    "migration:status": (payload) => {
      const p = payload as { migrations: MigrationStatus[] };
      setMigrations(p.migrations);
    },
    "migration:event": (payload) => {
      const e = payload as MigrationEvent;
      setEvents((prev) => [e, ...prev].slice(0, 500));
    },
  });

  const startMigration = () => {
    if (!shard || !targetDb) return;
    push("migration:start", { shard, target_db: targetDb });
    setShard("");
    setTargetDb("");
  };

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-semibold">Migration</h1>

      {/* Start form */}
      <div className="flex gap-3 items-end">
        <label className="space-y-1">
          <span className="text-sm text-gray-500">Shard</span>
          <input
            className="block border rounded px-3 py-1.5 font-mono text-sm w-40"
            placeholder="shard_0001"
            value={shard}
            onChange={(e) => setShard(e.target.value)}
          />
        </label>
        <label className="space-y-1">
          <span className="text-sm text-gray-500">Target DB</span>
          <input
            className="block border rounded px-3 py-1.5 font-mono text-sm w-44"
            placeholder="pg-primary-1"
            value={targetDb}
            onChange={(e) => setTargetDb(e.target.value)}
          />
        </label>
        <button
          className="px-4 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-40"
          disabled={!shard || !targetDb}
          onClick={startMigration}
        >
          Start
        </button>
      </div>

      {/* Active migrations table */}
      {migrations.length > 0 && (
        <section>
          <h2 className="text-lg font-medium mb-3">Active Migrations</h2>
          <div className="space-y-3">
            {migrations.map((m) => (
              <MigrationRow key={m.shard} migration={m} push={push} />
            ))}
          </div>
        </section>
      )}

      {migrations.length === 0 && (
        <div className="text-sm text-gray-400">No active migrations</div>
      )}

      {/* Event log */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-medium">Event Log</h2>
          {events.length > 0 && (
            <button
              className="text-xs text-gray-400 hover:text-gray-600"
              onClick={() => setEvents([])}
            >
              Clear
            </button>
          )}
        </div>
        <div className="border rounded-lg divide-y font-mono text-sm max-h-[40vh] overflow-y-auto">
          {events.length === 0 && (
            <div className="p-4 text-gray-400 text-center">No events yet</div>
          )}
          {events.map((e) => (
            <div key={`${e.ts}-${e.status}`} className="p-3 flex gap-4 items-baseline">
              <span className="text-gray-400 shrink-0 text-xs">
                {new Date(e.ts).toLocaleTimeString()}
              </span>
              <span className="text-blue-600 shrink-0">{e.shard}</span>
              <EventStatusBadge status={e.status} />
              {e.detail && (
                <span className="text-gray-500 text-xs truncate">{e.detail}</span>
              )}
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function MigrationRow({
  migration: m,
  push,
}: {
  migration: MigrationStatus;
  push: (event: string, payload: unknown) => void;
}) {
  return (
    <div className="border rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <span className="font-mono font-medium">{m.shard}</span>
        <PhaseBadge phase={m.phase} />
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
        <Stat label="Source DB" value={m.source_db_id ?? "—"} mono />
        <Stat label="Target DB" value={m.target_db_id ?? "—"} mono />
        <Stat
          label="Replication Lag"
          value={m.lag_bytes === null ? "—" : `${m.lag_bytes} bytes`}
          highlight={m.lag_bytes === 0 ? "green" : m.lag_bytes !== null ? "yellow" : undefined}
        />
        <Stat label="Gate" value={m.gate ?? "—"} mono />
      </div>

      <div className="flex gap-2">
        <button
          className="px-3 py-1 border border-yellow-500 text-yellow-600 rounded text-xs hover:bg-yellow-50 disabled:opacity-40"
          disabled={m.phase !== "replicating"}
          onClick={() => push("migration:drain", { shard: m.shard })}
        >
          Drain
        </button>
        <button
          className="px-3 py-1 border border-green-600 text-green-700 rounded text-xs hover:bg-green-50 disabled:opacity-40"
          disabled={m.phase !== "draining" || m.lag_bytes !== 0}
          onClick={() => push("migration:cutover", { shard: m.shard })}
        >
          Cutover
        </button>
        <button
          className="ml-auto px-3 py-1 border border-red-400 text-red-500 rounded text-xs hover:bg-red-50"
          onClick={() => push("migration:cancel", { shard: m.shard })}
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

function Stat({ label, value, mono, highlight }: {
  label: string; value: string; mono?: boolean; highlight?: "green" | "yellow";
}) {
  const valueClass = highlight === "green" ? "text-green-600 font-medium"
    : highlight === "yellow" ? "text-yellow-600 font-medium"
    : "text-gray-900";
  return (
    <div>
      <div className="text-xs text-gray-400 mb-0.5">{label}</div>
      <div className={`${mono ? "font-mono" : ""} ${valueClass} text-sm`}>{value}</div>
    </div>
  );
}

function PhaseBadge({ phase }: { phase: string }) {
  const colors: Record<string, string> = {
    preparing: "bg-blue-100 text-blue-700",
    replicating: "bg-indigo-100 text-indigo-700",
    draining: "bg-yellow-100 text-yellow-700",
    cutting_over: "bg-orange-100 text-orange-700",
  };
  return (
    <span className={`px-2.5 py-0.5 rounded-full text-xs font-medium ${colors[phase] ?? "bg-gray-100 text-gray-600"}`}>
      {phase}
    </span>
  );
}

function EventStatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    preparing: "text-blue-500",
    replicating: "text-indigo-500",
    lag: "text-gray-400",
    draining: "text-yellow-600",
    drained: "text-green-600",
    cutover_complete: "text-green-700",
    cancelled: "text-gray-400",
    error: "text-red-600",
  };
  return (
    <span className={`shrink-0 font-medium text-xs ${colors[status] ?? "text-gray-600"}`}>
      {status}
    </span>
  );
}
