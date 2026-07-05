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
  const [status, setStatus] = useState<MigrationStatus | null>(null);
  const [events, setEvents] = useState<MigrationEvent[]>([]);
  const [shard, setShard] = useState("");
  const [targetDb, setTargetDb] = useState("");

  const { push } = useChannel("admin:migration", {
    "migration:status": (payload) => {
      const p = payload as { status: MigrationStatus | null };
      setStatus(p.status);
    },
    "migration:event": (payload) => {
      const e = payload as MigrationEvent;
      setEvents((prev) => [e, ...prev].slice(0, 500));
    },
  });

  const cmd = (event: string, extra: Record<string, string> = {}) =>
    push(event, { shard: status?.shard ?? shard, ...extra });

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
          disabled={!shard || !targetDb || (status !== null)}
          onClick={() => push("migration:start", { shard, target_db: targetDb })}
        >
          Start
        </button>
      </div>

      {/* Active migration status card */}
      {status && (
        <div className="border rounded-lg p-5 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-medium">Active Migration</h2>
            <PhaseBadge phase={status.phase} />
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
            <Stat label="Shard" value={status.shard} mono />
            <Stat label="Source DB" value={status.source_db_id ?? "—"} mono />
            <Stat label="Target DB" value={status.target_db_id ?? "—"} mono />
            <Stat
              label="Replication Lag"
              value={status.lag_bytes === null ? "—" : `${status.lag_bytes} bytes`}
              highlight={status.lag_bytes === 0 ? "green" : status.lag_bytes !== null ? "yellow" : undefined}
            />
            <Stat label="Gate" value={status.gate ?? "—"} mono />
          </div>

          {/* Action buttons */}
          <div className="flex gap-3 pt-1">
            <button
              className="px-4 py-1.5 border border-yellow-500 text-yellow-600 rounded text-sm hover:bg-yellow-50 disabled:opacity-40"
              disabled={status.phase !== "replicating"}
              onClick={() => cmd("migration:drain")}
            >
              Drain
            </button>
            <button
              className="px-4 py-1.5 border border-green-600 text-green-700 rounded text-sm hover:bg-green-50 disabled:opacity-40"
              disabled={status.phase !== "draining" || status.lag_bytes !== 0}
              onClick={() => cmd("migration:cutover")}
            >
              Cutover
            </button>
            <button
              className="ml-auto px-4 py-1.5 border border-red-400 text-red-500 rounded text-sm hover:bg-red-50"
              onClick={() => cmd("migration:cancel")}
            >
              Cancel
            </button>
          </div>
        </div>
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
        <div className="border rounded-lg divide-y font-mono text-sm max-h-[50vh] overflow-y-auto">
          {events.length === 0 && (
            <div className="p-4 text-gray-400 text-center">No events yet</div>
          )}
          {events.map((e) => (
            <div key={e.ts + e.status} className="p-3 flex gap-4 items-baseline">
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

function Stat({
  label, value, mono, highlight,
}: {
  label: string;
  value: string;
  mono?: boolean;
  highlight?: "green" | "yellow";
}) {
  const valueClass = highlight === "green"
    ? "text-green-600 font-medium"
    : highlight === "yellow"
    ? "text-yellow-600 font-medium"
    : "text-gray-900";
  return (
    <div>
      <div className="text-xs text-gray-400 mb-0.5">{label}</div>
      <div className={`${mono ? "font-mono" : ""} ${valueClass}`}>{value}</div>
    </div>
  );
}

function PhaseBadge({ phase }: { phase: string }) {
  const colors: Record<string, string> = {
    preparing: "bg-blue-100 text-blue-700",
    replicating: "bg-indigo-100 text-indigo-700",
    draining: "bg-yellow-100 text-yellow-700",
    cutting_over: "bg-orange-100 text-orange-700",
    idle: "bg-gray-100 text-gray-500",
  };
  return (
    <span className={`px-2.5 py-1 rounded-full text-xs font-medium ${colors[phase] ?? "bg-gray-100 text-gray-600"}`}>
      {phase}
    </span>
  );
}

function EventStatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    preparing: "text-blue-500",
    replicating: "text-indigo-500",
    lag: "text-gray-500",
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
