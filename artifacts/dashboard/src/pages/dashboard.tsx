import { useState, useEffect, useRef } from "react";
import {
  useStartTest,
  useStopTest,
  useGetTestStatus,
  getGetTestStatusQueryKey,
  useGetTestReport,
  getGetTestReportQueryKey,
  useGetTokensByProvider,
  getGetTokensByProviderQueryKey,
  useResetDb,
} from "@workspace/api-client-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Terminal,
  Activity,
  Zap,
  Server,
  Settings2,
  Play,
  Square,
  Database,
  Loader2,
  FileText,
  CheckCircle2,
  AlertTriangle,
  Trash2,
} from "lucide-react";

type Mode = "SINGLE" | "DUAL" | "TRIPLE";

export default function Dashboard() {
  const [selectedMode, setSelectedMode] = useState<Mode>("SINGLE");
  const [selectedProvider, setSelectedProvider] = useState<string | null>(null);

  const { data: statusData, isLoading: isLoadingStatus } = useGetTestStatus({
    query: { refetchInterval: 1000, queryKey: getGetTestStatusQueryKey() },
  });

  const startTest = useStartTest();
  const stopTest = useStopTest();
  const resetDb = useResetDb();

  const isRunning = statusData?.isRunning ?? false;
  const wasRunning = useRef(isRunning);
  const [showReport, setShowReport] = useState(false);

  useEffect(() => {
    if (wasRunning.current && !isRunning && statusData && statusData.logs?.length > 0) {
      setShowReport(true);
    }
    wasRunning.current = isRunning;
  }, [isRunning, statusData]);

  const { data: reportData } = useGetTestReport({
    query: {
      enabled: showReport && !isRunning,
      queryKey: getGetTestReportQueryKey(),
    },
  });

  const { data: tokens, isLoading: isLoadingTokens } = useGetTokensByProvider(
    selectedProvider || "",
    {
      query: {
        enabled: !!selectedProvider,
        queryKey: getGetTokensByProviderQueryKey(selectedProvider || ""),
      },
    }
  );

  const logsEndRef = useRef<HTMLDivElement>(null);

  const handleStart = () => {
    setShowReport(false);
    setSelectedProvider(null);
    startTest.mutate({ mode: selectedMode });
  };

  const handleStop = () => {
    stopTest.mutate();
  };

  const handleResetDb = () => {
    resetDb.mutate();
  };

  const renderLogLine = (log: string, i: number) => {
    let colorClass = "text-muted-foreground";
    if (log.includes("INFO")) colorClass = "text-primary";
    if (log.includes("WARN")) colorClass = "text-accent";
    if (log.includes("ERROR")) colorClass = "text-destructive font-bold";
    const isSimultaneous = log.includes("SIMULTANEOUS STALL");
    return (
      <div
        key={i}
        className={`text-xs font-mono mb-1 ${isSimultaneous ? "text-destructive font-bold bg-destructive/10 px-1 rounded" : colorClass}`}
      >
        {log}
      </div>
    );
  };

  const proxies = statusData?.proxies ?? [];
  const providerCount = 1 + proxies.length; // test + connected proxies

  const elapsedLabel = (() => {
    if (!statusData?.testStartAt || statusData.testStartAt === 0) return "—";
    const secs = Math.floor((Date.now() - statusData.testStartAt) / 1000);
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = secs % 60;
    return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  })();

  return (
    <div className="min-h-screen bg-background text-foreground p-6 selection:bg-primary/30">
      <div className="max-w-7xl mx-auto space-y-6">

        {/* HEADER */}
        <header className="flex items-center justify-between border-b border-border pb-4">
          <div className="flex items-center gap-3">
            <div className="h-10 w-10 rounded bg-primary/10 flex items-center justify-center border border-primary/30 text-primary">
              <Terminal size={20} />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight uppercase flex items-center gap-2 text-primary">
                Sys_Ops <span className="text-muted-foreground font-light">|</span> Rotation Core
              </h1>
              <p className="text-xs text-muted-foreground font-mono">Stress Test Monitoring Console v2.0.0</p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2 border border-border px-3 py-1.5 bg-card rounded-md font-mono text-sm">
              <span className="text-muted-foreground">STATUS:</span>
              {isRunning ? (
                <span className="text-primary flex items-center gap-1.5 animate-pulse">
                  <div className="w-2 h-2 rounded-full bg-primary" /> RUNNING
                </span>
              ) : (
                <span className="text-muted-foreground flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full bg-muted-foreground" /> STOPPED
                </span>
              )}
            </div>
          </div>
        </header>

        {/* CONTROLS */}
        <section className="flex flex-col md:flex-row gap-6">
          <Card className="flex-1 bg-card/50 border-border/50">
            <CardHeader className="py-4 border-b border-border/50">
              <CardTitle className="text-sm font-mono flex items-center gap-2 text-muted-foreground uppercase">
                <Settings2 size={16} /> Operation Mode
              </CardTitle>
            </CardHeader>
            <CardContent className="py-4 flex gap-3">
              {(["SINGLE", "DUAL", "TRIPLE"] as Mode[]).map((mode) => (
                <Button
                  key={mode}
                  variant={selectedMode === mode ? "default" : "outline"}
                  onClick={() => setSelectedMode(mode)}
                  disabled={isRunning}
                  className={`font-mono text-xs tracking-wider ${selectedMode === mode ? "shadow-[0_0_15px_rgba(34,197,94,0.3)]" : ""}`}
                >
                  {mode}_MODE
                </Button>
              ))}
            </CardContent>
          </Card>

          <Card className="md:w-auto bg-card/50 border-border/50">
            <CardHeader className="py-4 border-b border-border/50">
              <CardTitle className="text-sm font-mono flex items-center gap-2 text-muted-foreground uppercase">
                <Zap size={16} /> Execution
              </CardTitle>
            </CardHeader>
            <CardContent className="py-4 flex gap-3">
              <Button
                onClick={handleStart}
                disabled={isRunning || startTest.isPending}
                className="bg-primary/20 hover:bg-primary/30 text-primary border border-primary/50 font-mono tracking-wider w-32"
              >
                {startTest.isPending ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Play className="w-4 h-4 mr-2" />}
                INIT
              </Button>
              <Button
                onClick={handleStop}
                disabled={!isRunning || stopTest.isPending}
                variant="destructive"
                className="bg-destructive/20 hover:bg-destructive/30 text-destructive border border-destructive/50 font-mono tracking-wider w-32"
              >
                {stopTest.isPending ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Square className="w-4 h-4 mr-2" />}
                HALT
              </Button>
              <Button
                onClick={handleResetDb}
                disabled={isRunning || resetDb.isPending}
                variant="outline"
                className="bg-transparent hover:bg-destructive/10 text-muted-foreground hover:text-destructive border border-border/50 hover:border-destructive/50 font-mono tracking-wider w-36"
              >
                {resetDb.isPending ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Trash2 className="w-4 h-4 mr-2" />}
                RESET_DB
              </Button>
            </CardContent>
          </Card>
        </section>

        {/* SIMULTANEOUS STALL BANNER */}
        {statusData?.simultaneousStall && (
          <div className="flex items-center gap-3 border border-destructive/70 bg-destructive/10 rounded-md px-4 py-3 animate-pulse">
            <AlertTriangle size={18} className="text-destructive shrink-0" />
            <span className="font-mono text-sm text-destructive font-bold tracking-wider uppercase">
              !! SIMULTANEOUS STALL — multiple providers silent
            </span>
          </div>
        )}

        {/* METRICS GRID */}
        <section className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-4">
          <StatBox label="SUBSCRIPTIONS" value={statusData?.subscriptionsCount ?? 0} icon={Activity} />
          <StatBox label="CAPACITY_LMT" value={statusData?.capacityLimit ?? 0} icon={Database} />
          <StatBox label="TOTAL_TOKENS" value={statusData?.totalTokens ?? 0} icon={Server} />
          <StatBox
            label="ROTATIONS"
            value={statusData?.rotationCount ?? 0}
            icon={Zap}
            highlight={!!(statusData?.rotationCount && statusData.rotationCount > 0)}
          />
          <StatBox label="TRADES" value={statusData?.totalTrades ?? 0} icon={Activity} />
          <StatBox label="WALLETS" value={statusData?.uniqueWalletsCount ?? 0} icon={Activity} />
          <StatBox label="PROVIDERS" value={isRunning ? providerCount : proxies.length} icon={Server} />
          <StatBox label="ELAPSED" value={elapsedLabel} icon={Activity} />
        </section>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* LEFT COL */}
          <div className="space-y-6 lg:col-span-1">
            <Card className="bg-card border-border shadow-md">
              <CardHeader className="py-3 border-b border-border/50 bg-black/20">
                <CardTitle className="text-sm font-mono flex items-center gap-2 text-primary">
                  <Activity size={16} /> RECONNECT_LATENCY
                </CardTitle>
              </CardHeader>
              <CardContent className="p-4">
                <LatencyGrid stats={statusData?.reconnectStats} />
              </CardContent>
            </Card>

            <Card className="bg-card border-border shadow-md">
              <CardHeader className="py-3 border-b border-border/50 bg-black/20">
                <CardTitle className="text-sm font-mono flex items-center gap-2 text-accent">
                  <Zap size={16} /> TRADE_RESUME_LATENCY
                </CardTitle>
              </CardHeader>
              <CardContent className="p-4">
                <LatencyGrid stats={statusData?.tradeResumeStats} />
              </CardContent>
            </Card>

            {/* CONNECTED NODES */}
            <Card className="bg-card border-border shadow-md">
              <CardHeader className="py-3 border-b border-border/50 bg-black/20">
                <CardTitle className="text-sm font-mono flex items-center gap-2 text-muted-foreground uppercase">
                  <Server size={16} /> CONNECTED_NODES
                </CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <ScrollArea className="h-[240px]">
                  {/* Fixed "test" row — always shown when running */}
                  {isRunning && (
                    <div
                      onClick={() => setSelectedProvider("test")}
                      className={`flex flex-col gap-1.5 p-3 border-b border-border/30 hover:bg-white/5 transition-colors cursor-pointer ${selectedProvider === "test" ? "bg-primary/10 border-l-2 border-l-primary" : ""}`}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-xs text-primary font-semibold">test</span>
                        <div className="flex items-center gap-1.5">
                          {statusData?.testIsStalled ? (
                            <span className="text-[10px] font-mono text-destructive flex items-center gap-0.5">
                              <AlertTriangle size={11} /> STALL
                            </span>
                          ) : (
                            <>
                              <span className="text-[10px] font-mono text-muted-foreground">built-in</span>
                              <CheckCircle2 size={13} className="text-primary" />
                            </>
                          )}
                        </div>
                      </div>
                      <FillBar
                        value={statusData?.testSubscriptions ?? statusData?.subscriptionsCount ?? 0}
                        max={4950}
                        stalled={statusData?.testIsStalled ?? false}
                      />
                    </div>
                  )}

                  {/* Proxy rows */}
                  {proxies.length > 0 ? (
                    proxies.map((p) => (
                      <div
                        key={p.id}
                        onClick={() => setSelectedProvider(p.id)}
                        className={`flex flex-col gap-1.5 p-3 border-b border-border/30 hover:bg-white/5 transition-colors cursor-pointer ${selectedProvider === p.id ? "bg-primary/10 border-l-2 border-l-primary" : ""}`}
                      >
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2 min-w-0">
                            <span className="font-mono text-xs text-foreground font-semibold truncate">{p.name}</span>
                            <span className="text-[10px] font-mono text-muted-foreground/60 shrink-0">v{p.version}</span>
                          </div>
                          <div className="flex items-center gap-1.5 shrink-0 ml-2">
                            {p.isStalled ? (
                              <span className="text-[10px] font-mono text-destructive flex items-center gap-0.5">
                                <AlertTriangle size={11} /> STALL
                              </span>
                            ) : (
                              <CheckCircle2 size={13} className="text-primary" />
                            )}
                          </div>
                        </div>
                        <FillBar value={p.subscriptions} max={p.capacity} stalled={p.isStalled} />
                      </div>
                    ))
                  ) : (
                    !isRunning && (
                      <div className="flex items-center justify-center h-full pt-8 text-muted-foreground text-xs font-mono">
                        [ NO_NODES_ONLINE ]
                      </div>
                    )
                  )}

                  {!isRunning && proxies.length === 0 && (
                    <div className="flex items-center justify-center h-[160px] text-muted-foreground text-xs font-mono">
                      [ NO_NODES_ONLINE ]
                    </div>
                  )}
                </ScrollArea>
              </CardContent>
            </Card>
          </div>

          {/* RIGHT COL */}
          <div className="lg:col-span-2 space-y-6 flex flex-col">
            {showReport && reportData ? (
              <Card className="bg-black border-primary/50 shadow-[0_0_30px_rgba(34,197,94,0.1)]">
                <CardHeader className="py-3 border-b border-primary/20 bg-primary/5 flex flex-row items-center justify-between">
                  <CardTitle className="text-sm font-mono flex items-center gap-2 text-primary uppercase">
                    <FileText size={16} /> FINAL_REPORT
                  </CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setShowReport(false)}
                    className="h-6 text-xs font-mono text-muted-foreground hover:text-primary"
                  >
                    [CLOSE]
                  </Button>
                </CardHeader>
                <CardContent className="p-0">
                  <ScrollArea className="h-[400px] w-full p-4">
                    <pre className="text-[11px] font-mono text-primary leading-relaxed whitespace-pre-wrap">
                      {reportData.report}
                    </pre>
                  </ScrollArea>
                </CardContent>
              </Card>
            ) : null}

            {selectedProvider && (
              <Card className="bg-card border-border shadow-md">
                <CardHeader className="py-3 border-b border-border/50 bg-black/20 flex flex-row items-center justify-between">
                  <CardTitle className="text-sm font-mono flex items-center gap-2 text-primary uppercase">
                    <Database size={16} /> NODE_TOKENS:{" "}
                    {proxies.find((p) => p.id === selectedProvider)?.name ?? selectedProvider.slice(0, 8)}
                  </CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setSelectedProvider(null)}
                    className="h-6 text-xs font-mono text-muted-foreground"
                  >
                    [X]
                  </Button>
                </CardHeader>
                <CardContent className="p-0">
                  {isLoadingTokens ? (
                    <div className="flex items-center justify-center p-8 text-primary font-mono text-xs">
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" /> FETCHING_DATA...
                    </div>
                  ) : tokens && tokens.length > 0 ? (
                    <ScrollArea className="h-[200px]">
                      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 p-4">
                        {tokens.map((t, i) => (
                          <div
                            key={i}
                            className="bg-black/50 border border-border/50 p-2 rounded text-[10px] font-mono flex flex-col gap-1"
                          >
                            <span className="text-muted-foreground truncate" title={t.mint}>
                              {t.mint.substring(0, 16)}...
                            </span>
                            <div className="flex gap-1 justify-between">
                              <span className="text-primary/70">P1: {t.provider1?.substring(0, 6) || "N/A"}</span>
                              <span className="text-accent/70">P2: {t.provider2?.substring(0, 6) || "N/A"}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </ScrollArea>
                  ) : (
                    <div className="flex items-center justify-center p-8 text-muted-foreground font-mono text-xs">
                      [ NO_TOKENS_ASSIGNED ]
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            <Card className="bg-black border-border shadow-md flex-1 flex flex-col min-h-[400px]">
              <CardHeader className="py-3 border-b border-border/50 bg-black/40">
                <CardTitle className="text-sm font-mono flex items-center gap-2 text-muted-foreground uppercase">
                  <Terminal size={16} /> SYSTEM_STREAM
                </CardTitle>
              </CardHeader>
              <CardContent className="p-4 flex-1 flex flex-col relative overflow-hidden">
                <ScrollArea className="absolute inset-4 pr-4">
                  {statusData?.logs?.length ? (
                    <div className="flex flex-col">
                      {statusData.logs.map((log, i) => renderLogLine(log, i))}
                      <div ref={logsEndRef} />
                    </div>
                  ) : (
                    <div className="text-muted-foreground text-xs font-mono h-full flex items-center justify-center pt-20">
                      [ STREAM_WAITING ]
                    </div>
                  )}
                </ScrollArea>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}

function FillBar({ value, max, stalled }: { value: number; max: number; stalled: boolean }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  const color = stalled
    ? "bg-destructive"
    : pct > 90
    ? "bg-accent"
    : "bg-primary";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-black/40 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-500 ${color}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-[10px] font-mono text-muted-foreground shrink-0 tabular-nums">
        {value}/{max}
      </span>
    </div>
  );
}

function StatBox({
  label,
  value,
  icon: Icon,
  highlight,
}: {
  label: string;
  value: number | string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
  highlight?: boolean;
}) {
  return (
    <Card className={`bg-card/50 border-border/50 overflow-hidden ${highlight ? "border-primary shadow-[0_0_10px_rgba(34,197,94,0.1)]" : ""}`}>
      <div className="p-3 flex flex-col gap-2 relative">
        <div className="flex items-center gap-2 text-muted-foreground z-10">
          <Icon size={14} className={highlight ? "text-primary" : ""} />
          <span className="text-[10px] font-mono tracking-wider uppercase">{label}</span>
        </div>
        <div className={`text-2xl font-mono tracking-tight z-10 ${highlight ? "text-primary" : "text-foreground"}`}>
          {value}
        </div>
        {highlight && (
          <div className="absolute right-0 top-0 bottom-0 w-16 bg-gradient-to-l from-primary/10 to-transparent pointer-events-none" />
        )}
      </div>
    </Card>
  );
}

function LatencyGrid({
  stats,
}: {
  stats?: { best: number; worst: number; avg: number; count: number };
}) {
  if (!stats) return <div className="text-muted-foreground text-xs font-mono">[ NO_DATA ]</div>;
  return (
    <div className="grid grid-cols-2 gap-3">
      <div className="flex flex-col p-2 bg-black/30 rounded border border-border/30">
        <span className="text-[10px] font-mono text-muted-foreground mb-1">AVG_MS</span>
        <span className="text-lg font-mono text-foreground">{stats.avg.toFixed(1)}</span>
      </div>
      <div className="flex flex-col p-2 bg-black/30 rounded border border-border/30">
        <span className="text-[10px] font-mono text-muted-foreground mb-1">BEST_MS</span>
        <span className="text-lg font-mono text-primary">
          {stats.best === Infinity ? 0 : stats.best.toFixed(1)}
        </span>
      </div>
      <div className="flex flex-col p-2 bg-black/30 rounded border border-border/30">
        <span className="text-[10px] font-mono text-muted-foreground mb-1">WORST_MS</span>
        <span className="text-lg font-mono text-destructive">
          {stats.worst === 0 ? 0 : stats.worst.toFixed(1)}
        </span>
      </div>
      <div className="flex flex-col p-2 bg-black/30 rounded border border-border/30">
        <span className="text-[10px] font-mono text-muted-foreground mb-1">SAMPLES</span>
        <span className="text-lg font-mono text-muted-foreground">{stats.count}</span>
      </div>
    </div>
  );
}
