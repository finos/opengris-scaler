const { useState, useEffect, useCallback, useRef } = React;

/* ── NumericStepper ── */
function NumericStepper({ value, onChange, min = 0, max = Infinity, step = 1, width = 56 }) {
  const [hov, setHov] = useState(null);
  const [localValue, setLocalValue] = useState(null);
  const btnStyle = (side) => ({
    width: 28,
    height: "100%",
    background: "transparent",
    border: "none",
    borderLeft: side === "plus" ? "1px solid var(--border-accent)" : "none",
    borderRight: side === "minus" ? "1px solid var(--border-accent)" : "none",
    color: "var(--accent-cyan)",
    fontFamily: "inherit",
    fontSize: 16,
    lineHeight: 1,
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    flexShrink: 0,
    transition: "background 0.12s, color 0.12s",
    padding: 0,
  });
  return (
    <div
      style={{
        display: "flex",
        alignItems: "stretch",
        width: "fit-content",
        background: "var(--bg-surface)",
        border: "1px solid var(--border-accent)",
        borderRadius: 3,
        height: 36,
        overflow: "hidden",
      }}
    >
      <button
        style={{
          ...btnStyle("minus"),
          background: hov === "minus" ? "rgba(0,200,224,0.1)" : "transparent",
        }}
        onMouseEnter={() => setHov("minus")}
        onMouseLeave={() => setHov(null)}
        onClick={() => onChange(Math.max(min, value - step))}
      >
        −
      </button>
      <input
        type="number"
        value={localValue !== null ? localValue : value}
        onFocus={() => setLocalValue(String(value))}
        onChange={(e) => setLocalValue(e.target.value)}
        onBlur={() => {
          const v = parseFloat(localValue);
          if (!isNaN(v)) onChange(Math.min(max, Math.max(min, Math.round(v))));
          setLocalValue(null);
        }}
        style={{
          width,
          background: "transparent",
          border: "none",
          outline: "none",
          color: "var(--text-primary)",
          fontFamily: "inherit",
          fontSize: 13,
          fontWeight: 600,
          textAlign: "center",
          padding: "0 4px",
        }}
      />
      <button
        style={{
          ...btnStyle("plus"),
          background: hov === "plus" ? "rgba(0,200,224,0.1)" : "transparent",
        }}
        onMouseEnter={() => setHov("plus")}
        onMouseLeave={() => setHov(null)}
        onClick={() => onChange(Math.min(max, value + step))}
      >
        +
      </button>
    </div>
  );
}

/* ── PanelBox ── */
function PanelBox({ title, children, style }) {
  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--border-accent)",
        borderRadius: 6,
        padding: "20px 22px",
        display: "flex",
        flexDirection: "column",
        gap: 14,
        ...style,
      }}
    >
      {title && (
        <div
          style={{
            fontSize: 11,
            color: "var(--accent-cyan)",
            borderBottom: "1px solid var(--border-accent)",
            paddingBottom: 10,
            marginBottom: 2,
            fontWeight: 600,
          }}
        >
          {title}
        </div>
      )}
      {children}
    </div>
  );
}

/* ── WorkerManagerCard ── */
function WorkerManagerCard({ wm, onChange, onRemove, allInstances, canRemove, fullWidth }) {
  const [localId, setLocalId] = useState(wm.id);
  const [showAdv, setShowAdv] = useState(false);
  useEffect(() => {
    setLocalId(wm.id);
  }, [wm.id]);

  const Label = ({ children, help }) => (
    <div
      style={{
        fontSize: 11,
        color: "var(--text-label)",
        marginBottom: 5,
        display: "flex",
        alignItems: "center",
        gap: 6,
      }}
    >
      <span>{children}</span>
      {help && <HelpTip text={help} />}
    </div>
  );
  const inp = {
    width: "100%",
    background: "var(--bg-surface)",
    border: "1px solid var(--border-accent)",
    borderRadius: 3,
    padding: "7px 10px",
    color: "var(--text-primary)",
    fontFamily: "inherit",
    fontSize: 12,
    outline: "none",
  };
  const set = (k, v) => onChange({ ...wm, [k]: v });

  const ToggleRow = ({ options, value, onChange: onTog }) => (
    <div
      style={{
        display: "flex",
        borderRadius: 3,
        overflow: "hidden",
        border: "1px solid var(--border-accent)",
      }}
    >
      {options.map(([val, lbl, disabled]) => (
        <button
          key={val}
          disabled={!!disabled}
          onClick={() => !disabled && onTog(val)}
          style={{
            flex: 1,
            padding: "6px 0",
            fontFamily: "inherit",
            fontSize: 11,
            cursor: disabled ? "not-allowed" : "pointer",
            border: "none",
            background: value === val ? "rgba(0,200,224,0.18)" : "transparent",
            color: disabled ? "var(--text-dim)" : value === val ? "var(--text-accent)" : "var(--text-muted)",
            transition: "background 0.15s, color 0.15s",
          }}
        >
          {lbl}
        </button>
      ))}
    </div>
  );

  const workerInst = allInstances.find((i) => i.type === wm.instanceType) || {
    price: 0,
  };
  const derivedCount =
    wm.capMode === "instances"
      ? Math.max(0, wm.instanceCap || 0)
      : Math.max(0, Math.floor((wm.budgetCap || 0) / (workerInst.price || 1)));
  const costPerHr = derivedCount * workerInst.price;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 12,
        width: fullWidth ? "100%" : 340,
        flexShrink: fullWidth ? 1 : 0,
      }}
    >
      {/* header */}
      <div style={{ display: "flex", alignItems: "flex-end", gap: 8 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <Label>Type</Label>
          <WorkerManagerTypeSelect value={wm.type} onChange={(v) => set("type", v)} />
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4, flex: 1 }}>
          <Label help="Unique name for this worker manager.">Name</Label>
          <input
            value={localId}
            onChange={(e) => setLocalId(e.target.value)}
            onBlur={() => {
              const v = localId.trim();
              if (!v) setLocalId(wm.id);
              else if (v !== wm.id) set("id", v);
            }}
            placeholder="wm-id"
            style={{
              width: "100%",
              background: "var(--bg-surface)",
              border: "1px solid var(--border-accent)",
              borderRadius: 3,
              padding: "5px 8px",
              color: "var(--text-primary)",
              fontFamily: "inherit",
              fontSize: 11,
              outline: "none",
            }}
          />
        </div>
        {canRemove && (
          <button
            onClick={onRemove}
            style={{
              background: "none",
              border: "1px solid var(--border-danger)",
              borderRadius: 3,
              color: "var(--text-danger)",
              fontFamily: "inherit",
              fontSize: 12,
              width: 28,
              height: 28,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              flexShrink: 0,
              transition: "border-color 0.15s",
            }}
            onMouseEnter={(e) => (e.currentTarget.style.borderColor = "rgba(255,80,60,0.6)")}
            onMouseLeave={(e) => (e.currentTarget.style.borderColor = "var(--border-danger)")}
          >
            ✕
          </button>
        )}
      </div>

      {/* orb_aws_ec2 */}
      {wm.type === "orb_aws_ec2" && (
        <>
          <div>
            <Label>Worker Instance Type</Label>
            <InstancePicker value={wm.instanceType} onChange={(v) => set("instanceType", v)} defaultCat="all" />
          </div>
          <div>
            <Label>Scale Limit</Label>
            <ToggleRow
              options={[
                ["instances", "Instance cap"],
                ["budget", "USD/hr cap"],
              ]}
              value={wm.capMode}
              onChange={(v) => set("capMode", v)}
            />
            <div style={{ marginTop: 7 }}>
              {wm.capMode === "instances" ? (
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <NumericStepper
                    value={wm.instanceCap || 1}
                    onChange={(v) => set("instanceCap", v)}
                    min={1}
                    max={1000}
                  />
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>max instances</span>
                </div>
              ) : (
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <NumericStepper
                    value={wm.budgetCap || 10}
                    onChange={(v) => set("budgetCap", v)}
                    min={0}
                    step={0.5}
                    width={64}
                  />
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>max USD/h</span>
                </div>
              )}
            </div>
          </div>
          <div
            style={{
              padding: "8px 10px",
              background: "rgba(0,255,136,0.04)",
              border: "1px solid var(--border-success)",
              borderRadius: 3,
              display: "flex",
              justifyContent: "space-between",
              alignItems: "baseline",
            }}
          >
            <span
              style={{
                fontSize: 10,
                color: "var(--text-muted)",
              }}
            >
              Cost
            </span>
            <span
              style={{
                fontSize: 13,
                fontWeight: 600,
                color: "var(--text-success)",
              }}
            >
              USD {costPerHr.toFixed(2)}/h
            </span>
          </div>
          <button
            onClick={() => setShowAdv((v) => !v)}
            style={{
              background: "none",
              border: "1px solid var(--border-accent)",
              borderRadius: 3,
              padding: "6px 10px",
              color: "var(--text-muted)",
              fontFamily: "inherit",
              fontSize: 11,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              width: "100%",
            }}
          >
            <span>Advanced</span>
            <span style={{ fontSize: 11 }}>{showAdv ? "▴" : "▾"}</span>
          </button>
          {showAdv && (
            <div>
              <Label help={"- Installed on each worker instance\n- opengris-scaler must be included"}>
                requirements.txt
              </Label>
              <textarea
                value={wm.requirements}
                onChange={(e) => set("requirements", e.target.value)}
                style={{
                  width: "100%",
                  background: "var(--bg-surface)",
                  border: "1px solid var(--border-accent)",
                  borderRadius: 3,
                  padding: "7px 10px",
                  color: "var(--text-primary)",
                  fontFamily: "inherit",
                  fontSize: 11,
                  outline: "none",
                  resize: "vertical",
                  minHeight: 72,
                  lineHeight: 1.6,
                }}
              />
            </div>
          )}
        </>
      )}

      {/* aws_raw_ecs */}
      {wm.type === "aws_raw_ecs" && (
        <>
          <div>
            <Label>ECS Cluster</Label>
            <input
              value={wm.ecsCluster || ""}
              onChange={(e) => set("ecsCluster", e.target.value)}
              style={inp}
              placeholder="scaler-cluster"
            />
          </div>
          <div>
            <Label>Container Image</Label>
            <input
              value={wm.ecsTaskImage || ""}
              onChange={(e) => set("ecsTaskImage", e.target.value)}
              style={inp}
              placeholder="public.ecr.aws/v4u8j8r6/scaler:latest"
            />
          </div>
          <div>
            <Label>Subnets (comma-separated)</Label>
            <input
              value={wm.ecsSubnets || ""}
              onChange={(e) => set("ecsSubnets", e.target.value)}
              style={inp}
              placeholder="subnet-abc123, subnet-def456"
            />
          </div>
          <div>
            <Label>Task Definition</Label>
            <input
              value={wm.ecsTaskDefinition || ""}
              onChange={(e) => set("ecsTaskDefinition", e.target.value)}
              style={inp}
              placeholder="scaler-task-definition"
            />
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label>vCPU</Label>
              <NumericStepper value={wm.ecsTaskCpu || 4} onChange={(v) => set("ecsTaskCpu", v)} min={1} max={64} />
            </div>
            <div style={{ flex: 1 }}>
              <Label>Memory (GB)</Label>
              <NumericStepper
                value={wm.ecsTaskMemory || 30}
                onChange={(v) => set("ecsTaskMemory", v)}
                min={1}
                max={512}
              />
            </div>
          </div>
        </>
      )}

      {/* aws_hpc */}
      {wm.type === "aws_hpc" && (
        <>
          <div>
            <Label>Job Queue</Label>
            <input
              value={wm.jobQueue || ""}
              onChange={(e) => set("jobQueue", e.target.value)}
              style={inp}
              placeholder="scaler-batch-queue"
            />
          </div>
          <div>
            <Label>Job Definition</Label>
            <input
              value={wm.jobDefinition || ""}
              onChange={(e) => set("jobDefinition", e.target.value)}
              style={inp}
              placeholder="scaler-job-definition"
            />
          </div>
          <div>
            <Label>S3 Bucket</Label>
            <input
              value={wm.s3Bucket || ""}
              onChange={(e) => set("s3Bucket", e.target.value)}
              style={inp}
              placeholder="my-scaler-bucket"
            />
          </div>
          <div>
            <Label>S3 Prefix</Label>
            <input
              value={wm.s3Prefix || "scaler-tasks"}
              onChange={(e) => set("s3Prefix", e.target.value)}
              style={inp}
              placeholder="scaler-tasks"
            />
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label>Max Concurrent Jobs</Label>
              <NumericStepper
                value={wm.maxConcurrentJobs || 100}
                onChange={(v) => set("maxConcurrentJobs", v)}
                min={1}
                max={10000}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label>Timeout (min)</Label>
              <NumericStepper
                value={wm.jobTimeoutMinutes || 60}
                onChange={(v) => set("jobTimeoutMinutes", v)}
                min={1}
                max={1440}
              />
            </div>
          </div>
        </>
      )}

      {/* symphony */}
      {wm.type === "symphony" && (
        <>
          <div>
            <Label>Service Name</Label>
            <input
              value={wm.serviceName || ""}
              onChange={(e) => set("serviceName", e.target.value)}
              style={inp}
              placeholder="my-symphony-service"
            />
          </div>
        </>
      )}

      {/* baremetal_native */}
      {wm.type === "baremetal_native" && (
        <>
          <div>
            <Label>Mode</Label>
            <ToggleRow
              options={[
                ["dynamic", "Dynamic"],
                ["fixed", "Fixed"],
              ]}
              value={wm.mode || "fixed"}
              onChange={(v) => set("mode", v)}
            />
          </div>
          <div>
            <Label>Worker Type Prefix</Label>
            <input
              value={wm.workerType || ""}
              onChange={(e) => set("workerType", e.target.value)}
              style={inp}
              placeholder="optional"
            />
          </div>
          {(wm.mode || "fixed") === "fixed" ? (
            <div>
              <Label help="Exact number of worker processes to pre-spawn.">Number of Workers</Label>
              <NumericStepper
                value={wm.maxTaskConcurrency != null && wm.maxTaskConcurrency >= 1 ? wm.maxTaskConcurrency : 4}
                onChange={(v) => set("maxTaskConcurrency", Math.max(1, v))}
                min={1}
              />
            </div>
          ) : (
            <div>
              <Label help="Maximum concurrent workers the scheduler may spawn. -1 = no limit (uses cpu_count).">
                Max Task Concurrency
              </Label>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <NumericStepper
                  value={wm.maxTaskConcurrency != null ? wm.maxTaskConcurrency : -1}
                  onChange={(v) => set("maxTaskConcurrency", v)}
                  min={-1}
                />
                <span style={{ fontSize: 11, color: "var(--text-muted)" }}>(-1 = no limit)</span>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

/* ── CopyBtn ── */
function CopyBtn({ value }) {
  const [copied, setCopied] = useState(false);
  const [hov, setHov] = useState(false);
  return (
    <button
      onClick={() =>
        navigator.clipboard.writeText(value).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        })
      }
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        background: hov && !copied ? "rgba(0,200,224,0.08)" : "none",
        border:
          "1px solid " + (copied ? "var(--border-success)" : hov ? "var(--border-strong)" : "var(--border-accent)"),
        borderRadius: 3,
        color: copied ? "var(--text-success)" : hov ? "var(--text-accent)" : "var(--text-muted)",
        fontFamily: "inherit",
        fontSize: 10,
        padding: "2px 7px",
        cursor: "pointer",
        letterSpacing: "0.06em",
        transition: "color 0.12s, border-color 0.12s, background 0.12s",
        flexShrink: 0,
      }}
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

/* ── DeploymentCard ── */
function DeploymentCard({ state, onDownload, keyMaterial, isRunning }) {
  const rows = [
    { label: "Scheduler", value: state.scheduler_address },
    { label: "Object storage", value: state.object_storage_address },
    { label: "Monitor", value: state.monitor_address },
    { label: "GUI", value: state.gui_address, href: state.gui_address },
    {
      label: "SSH",
      value: state.public_ip
        ? "chmod 400 " + state.key_file + " &&\nssh -i " + state.key_file + " ec2-user@" + state.public_ip
        : null,
      code: true,
    },
    { label: "Instance", value: state.instance_id },
  ];
  return (
    <div
      style={{
        background: "rgba(0,255,136,0.03)",
        border: "1px solid var(--border-success)",
        borderRadius: 4,
        padding: "20px 24px",
        display: "flex",
        flexDirection: "column",
        gap: 16,
        animation: "fadeSlideIn 0.3s ease",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <div
          style={{
            fontSize: 11,
            color: "var(--text-success)",
            fontWeight: 600,
          }}
        >
          Active Deployment
        </div>
        <button
          onClick={onDownload}
          style={{
            background: "none",
            border: "1px solid var(--border-accent)",
            borderRadius: 3,
            color: "var(--text-muted)",
            fontFamily: "inherit",
            fontSize: 10,
            padding: "3px 9px",
            cursor: "pointer",
            letterSpacing: "0.05em",
          }}
        >
          ↓ State JSON
        </button>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {rows.map(({ label, value, href, code }) => (
          <div
            key={label}
            style={{
              display: "flex",
              alignItems: code ? "flex-start" : "baseline",
              gap: 10,
            }}
          >
            <span
              style={{
                fontSize: 11,
                color: "var(--text-dim)",
                width: 120,
                flexShrink: 0,
                paddingTop: code ? 2 : 0,
              }}
            >
              {label}
            </span>
            <div
              style={{
                display: "flex",
                alignItems: code ? "flex-start" : "baseline",
                gap: 6,
                flex: 1,
                minWidth: 0,
              }}
            >
              {value ? (
                <>
                  {code ? (
                    <pre
                      style={{
                        fontSize: 11,
                        color: "var(--text-primary)",
                        fontFamily: "var(--font-mono)",
                        margin: 0,
                        whiteSpace: "pre",
                        overflowX: "auto",
                        flex: 1,
                        minWidth: 0,
                      }}
                    >
                      {value}
                    </pre>
                  ) : href ? (
                    <a
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{
                        fontSize: 12,
                        color: "var(--text-accent)",
                        fontWeight: 500,
                        overflowWrap: "anywhere",
                        whiteSpace: "pre-wrap",
                        fontFamily: "var(--font-mono)",
                        textDecoration: "none",
                        borderBottom: "1px solid var(--border-accent)",
                      }}
                    >
                      {value}
                    </a>
                  ) : (
                    <span
                      style={{
                        fontSize: 12,
                        color: "var(--text-primary)",
                        fontWeight: 500,
                        overflowWrap: "anywhere",
                        whiteSpace: "pre-wrap",
                        fontFamily: "var(--font-mono)",
                      }}
                    >
                      {value}
                    </span>
                  )}
                  <CopyBtn value={value} />
                </>
              ) : (
                <span
                  style={{
                    fontSize: 12,
                    color: "var(--text-dim)",
                    fontStyle: "italic",
                  }}
                >
                  pending…
                </span>
              )}
            </div>
          </div>
        ))}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span
            style={{
              fontSize: 11,
              color: "var(--text-dim)",
              width: 120,
              flexShrink: 0,
            }}
          >
            SSH Key
          </span>
          {keyMaterial ? (
            <button
              onClick={() => downloadText(keyMaterial.name + ".pem", keyMaterial.mat)}
              style={{
                background: "none",
                border: "1px solid var(--border-accent)",
                borderRadius: 3,
                color: "var(--text-accent)",
                fontFamily: "inherit",
                fontSize: 10,
                padding: "3px 9px",
                cursor: "pointer",
                letterSpacing: "0.05em",
              }}
            >
              ↓ {keyMaterial.name}.pem
            </button>
          ) : isRunning ? (
            <span style={{ fontSize: 12, color: "var(--text-dim)", fontStyle: "italic" }}>
              pending…
            </span>
          ) : (
            <span style={{ fontSize: 11, color: "var(--text-dim)", lineHeight: 1.5 }}>
              not saved — download during provisioning
              <br />
              or retrieve from the AWS console
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

/* ── Python syntax highlighter (theme-aware, no external deps) ── */
const PY_KEYWORDS = new Set([
  "False", "None", "True", "and", "as", "assert", "async", "await",
  "break", "class", "continue", "def", "del", "elif", "else", "except",
  "finally", "for", "from", "global", "if", "import", "in", "is",
  "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try",
  "while", "with", "yield",
]);

function tokenizePython(code) {
  const tokens = [];
  const re = /(#[^\n]*)|("""[\s\S]*?"""|'''[\s\S]*?'''|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)(\s*\()?|(\s+|[^\w\s#"']+)/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    if (m[1])       tokens.push({ text: m[1], color: "var(--text-dim)" });
    else if (m[2])  tokens.push({ text: m[2], color: "var(--text-warning)" });
    else if (m[3])  tokens.push({ text: m[3], color: "var(--text-danger)" });
    else if (m[4]) {
      const word = m[4], call = m[5] || "";
      const color = PY_KEYWORDS.has(word)
        ? "var(--text-accent)"
        : call ? "var(--text-success)" : "var(--text-primary)";
      tokens.push({ text: word, color });
      if (call) tokens.push({ text: call, color: "var(--text-primary)" });
    } else {
      tokens.push({ text: m[6], color: "var(--text-primary)" });
    }
  }
  return tokens;
}

function PyCode({ code }) {
  const tokens = tokenizePython(code);
  return (
    <pre
      style={{
        margin: 0,
        padding: "14px 16px",
        background: "var(--bg-surface)",
        border: "1px solid var(--border-accent)",
        borderRadius: 3,
        fontSize: 11,
        fontFamily: "var(--font-mono)",
        whiteSpace: "pre",
        overflowX: "auto",
        lineHeight: 1.7,
      }}
    >
      {tokens.map((t, i) => (
        <span key={i} style={{ color: t.color }}>{t.text}</span>
      ))}
    </pre>
  );
}

/* ── GettingStartedCard ── */
function GettingStartedCard({ schedulerAddress, ready }) {
  const addr = schedulerAddress || "tcp://<scheduler-address>:2345";
  const snippet = `from scaler import Client

with Client(address="${addr}") as client:
    result = client.submit(pow, 2, 10).result()
    print(result)  # 1024`;

  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--border-accent)",
        borderRadius: 4,
        padding: "16px 20px",
        display: "flex",
        flexDirection: "column",
        gap: 12,
      }}
    >
      <div
        style={{
          fontSize: 11,
          color: "var(--text-muted)",
          fontWeight: 600,
        }}
      >
        Getting Started
      </div>
      {ready ? (
        <>
          <div style={{ fontSize: 11, color: "var(--text-dim)", lineHeight: 1.5 }}>
            Connect a client to your deployment and submit tasks:
          </div>
          <div style={{ position: "relative" }}>
            <PyCode code={snippet} />
            <div style={{ position: "absolute", top: 8, right: 8 }}>
              <CopyBtn value={snippet} />
            </div>
          </div>
        </>
      ) : (
        <div style={{ fontSize: 11, color: "var(--text-dim)", fontStyle: "italic" }}>
          Waiting for scheduler…
        </div>
      )}
    </div>
  );
}

/* ── TopNav ── */
function TopNav({ activeTab, setActiveTab, theme, setTheme, showPostLaunch, launchControl, guiAddress }) {
  const tabs = [
    { id: "config", label: "Config" },
    { id: "deployment", label: "Deployment", postLaunch: true },
    { id: "logs", label: "Scheduler Logs", postLaunch: true },
    // { id: "gui", label: "GUI", postLaunch: true },
    { id: "gui", label: "GUI", postLaunch: true, isLink: true, href: guiAddress },
  ];
  return (
    <div
      style={{
        padding: "0 28px",
        borderBottom: "1px solid var(--border-accent)",
        background: "var(--bg-panel)",
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
      }}
    >
      <img
        src="https://raw.githubusercontent.com/finos/branding/master/project-logos/active-project-logos/OpenGRIS/Scaler/2025_OpenGRIS_Scaler.svg"
        alt="OpenGRIS Scaler"
        style={{ height: 34, marginRight: 28, flexShrink: 0 }}
      />
      <div style={{ display: "flex", flex: 1 }}>
        {tabs
          .filter((t) => !t.postLaunch || showPostLaunch)
          .map((t) =>
            t.isLink ? (
              <a
                key={t.id}
                href={t.href}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  padding: "14px 18px",
                  background: "transparent",
                  border: "none",
                  borderBottom: "2px solid transparent",
                  color: t.href ? "var(--text-muted)" : "var(--text-dim)",
                  fontFamily: "inherit",
                  fontSize: 12,
                  cursor: t.href ? "pointer" : "default",
                  textDecoration: "none",
                  display: "inline-flex",
                  alignItems: "center",
                  pointerEvents: t.href ? undefined : "none",
                }}
              >
                {t.label} ↗
              </a>
            ) : (
              <button
                key={t.id}
                onClick={() => setActiveTab(t.id)}
                style={{
                  padding: "14px 18px",
                  background: "transparent",
                  border: "none",
                  borderBottom: activeTab === t.id ? "2px solid var(--tab-active)" : "2px solid transparent",
                  color: activeTab === t.id ? "var(--text-accent)" : "var(--text-muted)",
                  fontFamily: "inherit",
                  fontSize: 12,
                  cursor: "pointer",
                }}
              >
                {t.label}
              </button>
            )
          )}
      </div>
      {launchControl && <div style={{ marginRight: 16 }}>{launchControl}</div>}
      <label
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          fontSize: 11,
          color: "var(--text-muted)",
        }}
      >
        Theme
        <select
          value={theme}
          onChange={(e) => setTheme(e.target.value)}
          style={{
          background: "var(--bg-surface)",
          border: "1px solid var(--border-accent)",
          borderRadius: 3,
          color: "var(--text-secondary)",
          fontFamily: "inherit",
          fontSize: 10,
          padding: "4px 8px",
          cursor: "pointer",
          outline: "none",
        }}
      >
          <option value="dark">Dark</option>
          <option value="light">Light</option>
          <option value="zenburn">Zenburn</option>
        </select>
      </label>
    </div>
  );
}

/* ── App ── */
function App() {
  const [region, setRegion] = useState("us-east-1");
  const [accessKeyId, setAKI] = useState("");
  const [secretKey, setSK] = useState("");
  const [transport, setTransport] = useState("ws");
  const [networkBackend, setNetBack] = useState("ymq");
  const [pythonVersion, setPyVer] = useState("3.14");
  const [schedulerRequirements, setSchedulerReqs] = useState("opengris-scaler[all]");
  const [schedulerType, setSchedulerType] = useState("c5.xlarge");
  const [schedulerPort, setSchedPort] = useState(6788);
  const [objectStoragePort, setObjPort] = useState(6789);
  const [showSchedAdv, setShowSchedAdv] = useState(false);
  const [activeTab, setActiveTab] = useState("config");
  const [theme, setTheme] = useState(() =>
    localStorage.getItem("launchpad-theme") ||
    (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"),
  );

  const wmCounterRef = useRef(1);
  const [workerManagers, setWorkerManagers] = useState([
    {
      _uid: 1,
      id: "wm-1",
      type: "orb_aws_ec2",
      instanceType: "t3.medium",
      capMode: "instances",
      instanceCap: 4,
      budgetCap: 10,
      requirements: "opengris-scaler[all]",
    },
  ]);
  const [selectedWmId, setSelectedWmId] = useState("wm-1");

  const [phase, setPhase] = useState(() => {
    try {
      return localStorage.getItem("scaler_state") ? "ready" : "idle";
    } catch {
      return "idle";
    }
  });
  const [log, setLog] = useState(() => {
    try {
      const s = localStorage.getItem("scaler_log");
      return s ? JSON.parse(s) : [];
    } catch {
      return [];
    }
  });
  const [provState, setProvState] = useState(() => {
    try {
      const s = localStorage.getItem("scaler_state");
      return s ? JSON.parse(s) : null;
    } catch {
      return null;
    }
  });
  const [keyMaterial, setKeyMaterial] = useState(null);
  const abortRef = useRef(null);

  const [guiReady, setGuiReady] = useState(false);
  const [guiElapsed, setGuiElapsed] = useState(0);

  useEffect(() => {
    const addr = provState?.gui_address;
    if (!addr) {
      setGuiReady(false);
      setGuiElapsed(0);
      return;
    }
    setGuiReady(false);
    setGuiElapsed(0);
    let cancelled = false;
    const start = Date.now();
    const ticker = setInterval(() => {
      if (!cancelled) setGuiElapsed(Math.floor((Date.now() - start) / 1000));
    }, 1000);
    const poll = async () => {
      if (cancelled) return;
      const ctrl = new AbortController();
      const timeout = setTimeout(() => ctrl.abort(), 4000);
      try {
        await fetch(addr, {
          mode: "no-cors",
          cache: "no-store",
          signal: ctrl.signal,
        });
        clearTimeout(timeout);
        if (!cancelled) {
          setGuiReady(true);
          clearInterval(ticker);
        }
      } catch {
        clearTimeout(timeout);
        if (!cancelled) setTimeout(poll, 5000);
      }
    };
    poll();
    return () => {
      cancelled = true;
      clearInterval(ticker);
    };
  }, [provState?.gui_address]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("launchpad-theme", theme);
  }, [theme]);

  useEffect(() => {
    try {
      localStorage.setItem("scaler_log", JSON.stringify(log));
    } catch (_) {}
  }, [log]);

  useEffect(() => {
    if (phase === "provisioning") setActiveTab("deployment");
  }, [phase]);

  const addLog = useCallback((text, cls) => {
    setLog((prev) => [...prev, { text, cls: cls || "info" }]);
  }, []);
  const savePartial = useCallback((partial) => {
    setProvState(partial);
    try {
      localStorage.setItem("scaler_state", JSON.stringify(partial));
    } catch (_) {}
  }, []);

  const allInstances = window.SCALER_INSTANCES || [];
  const schedulerInst = allInstances.find((i) => i.type === schedulerType) || {
    price: 0.17,
  };
  const wmCosts = workerManagers.map((wm) => {
    if (wm.type !== "orb_aws_ec2") return 0;
    const inst = allInstances.find((i) => i.type === wm.instanceType) || {
      price: 0,
    };
    const count =
      wm.capMode === "instances"
        ? Math.max(0, wm.instanceCap || 0)
        : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
    return count * inst.price;
  });
  const totalCostPerHr = schedulerInst.price + wmCosts.reduce((a, b) => a + b, 0);

  const addWorkerManager = useCallback(() => {
    wmCounterRef.current += 1;
    const n = wmCounterRef.current;
    const newId = "wm-" + n;
    setWorkerManagers((prev) => [
      ...prev,
      {
        _uid: n,
        id: newId,
        type: "orb_aws_ec2",
        instanceType: "t3.medium",
        capMode: "instances",
        instanceCap: 4,
        budgetCap: 10,
        requirements: "opengris-scaler[all]",
      },
    ]);
    setSelectedWmId(newId);
  }, []);
  const removeWorkerManager = useCallback((id) => {
    setWorkerManagers((prev) => {
      const next = prev.filter((wm) => wm.id !== id);
      setSelectedWmId((s) => (s === id ? next[0]?.id || "" : s));
      return next;
    });
  }, []);
  const updateWorkerManager = useCallback(
    (id, updated) => setWorkerManagers((prev) => prev.map((wm) => (wm.id === id ? updated : wm))),
    [],
  );

  const hasCredentials = accessKeyId.trim().length > 0 && secretKey.trim().length > 0;

  const monitorPort = schedulerPort + 2;
  const GUI_PORT = 50001;
  const portConflicts = [];
  if (schedulerPort === objectStoragePort)
    portConflicts.push("Scheduler port and object storage port must differ.");
  if (objectStoragePort === monitorPort)
    portConflicts.push(`Object storage port conflicts with the monitor port (scheduler + 2 = ${monitorPort}).`);
  if (schedulerPort === GUI_PORT)
    portConflicts.push(`Scheduler port conflicts with the GUI port (${GUI_PORT}).`);
  if (objectStoragePort === GUI_PORT)
    portConflicts.push(`Object storage port conflicts with the GUI port (${GUI_PORT}).`);
  if (monitorPort === GUI_PORT)
    portConflicts.push(`Monitor port (scheduler + 2 = ${monitorPort}) conflicts with the GUI port (${GUI_PORT}).`);

  const checks = [
    {
      key: "aki",
      label: "Access Key ID required",
      ok: accessKeyId.trim().length > 0,
    },
    {
      key: "sk",
      label: "Secret Access Key required",
      ok: secretKey.trim().length > 0,
    },
    {
      key: "wm",
      label: "At least one worker manager required",
      ok: workerManagers.length > 0,
    },
    {
      key: "ports",
      label: portConflicts.join(" "),
      ok: portConflicts.length === 0,
    },
  ];
  const blocking = checks.filter((c) => !c.ok);
  const formReady = blocking.length === 0;
  const isRunning = phase === "provisioning" || phase === "destroying";

  const handleLaunch = useCallback(async () => {
    if (!formReady) return;
    setLog([]);
    try {
      localStorage.removeItem("scaler_log");
    } catch (_) {}
    setPhase("provisioning");
    const cfg = {
      region,
      nameSuffix: randomSuffix(),
      instanceType: schedulerType,
      amiId: null,
      transport,
      networkBackend,
      schedulerPort,
      objectStoragePort,
      pythonVersion,
      scalerPackage: schedulerRequirements,
      instanceProfileName: null,
      pollTimeout: 600,
      pollInterval: 15,
      debugDumpPath: null,
      workerManagers: workerManagers.map((wm) => ({
        ...wm,
        requirements: wm.requirements,
      })),
    };
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const state = await provision(
        cfg,
        { accessKeyId, secretKey },
        addLog,
        savePartial,
        (name, mat) => setKeyMaterial({ name, mat }),
        controller.signal,
      );
      savePartial(state);
      setPhase("ready");
    } catch (err) {
      addLog(
        err.name === "AbortError"
          ? "\nAborted. Any resources created so far are saved — use Destroy to clean them up."
          : "\nError: " + err.message,
        err.name === "AbortError" ? "warn" : "err",
      );
      setPhase("error");
    } finally {
      abortRef.current = null;
    }
  }, [
    formReady,
    region,
    schedulerType,
    transport,
    networkBackend,
    schedulerPort,
    objectStoragePort,
    pythonVersion,
    schedulerRequirements,
    workerManagers,
    accessKeyId,
    secretKey,
    addLog,
    savePartial,
  ]);

  const handleAbort = useCallback(() => {
    if (abortRef.current) abortRef.current.abort();
  }, []);

  const handleDestroy = useCallback(async () => {
    if (!provState || !hasCredentials) return;
    if (
      !window.confirm(
        "Terminate all AWS resources in this deployment?\n\n" +
          "• EC2 instance: " +
          (provState.instance_id || "—") +
          "\n" +
          "• Security group: " +
          (provState.security_group_id || "—") +
          "\n" +
          "• Key pair: " +
          (provState.key_pair_name || "—") +
          "\n" +
          (provState.iam && provState.iam.created ? "• IAM role & profile\n" : "") +
          "\nThis cannot be undone.",
      )
    )
      return;
    setPhase("destroying");
    setActiveTab("deployment");
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      await teardown(provState, { accessKeyId, secretKey }, addLog, controller.signal);
      try {
        localStorage.removeItem("scaler_state");
        localStorage.removeItem("scaler_log");
      } catch (_) {}
      setProvState(null);
      setKeyMaterial(null);
      setPhase("idle");
    } catch (err) {
      if (err.name === "AbortError") {
        addLog("\nTeardown aborted. Some resources may still exist — run Destroy again to retry.", "warn");
      } else {
        addLog("\nError during teardown: " + err.message + "\nFix the issue and run Destroy again to retry.", "err");
      }
      setPhase("ready");
    } finally {
      abortRef.current = null;
    }
  }, [provState, hasCredentials, accessKeyId, secretKey, addLog, setActiveTab]);

  const handleDownloadConfig = useCallback(() => {
    const cfg = {
      region,
      transport,
      networkBackend,
      schedulerPort,
      objectStoragePort,
      pythonVersion,
      workerManagers: workerManagers.map((wm) => ({
        ...wm,
        requirements: wm.requirements,
      })),
    };
    downloadText("config.toml", buildConfigToml(cfg));
  }, [
    region,
    transport,
    networkBackend,
    schedulerPort,
    objectStoragePort,
    pythonVersion,
    workerManagers,
  ]);

  const handleReset = useCallback(() => {
    setLog([]);
    setPhase("idle");
    setProvState(null);
    setKeyMaterial(null);
    try {
      localStorage.removeItem("scaler_state");
      localStorage.removeItem("scaler_log");
    } catch (_) {}
  }, []);

  const handleLoadState = useCallback(
    (e) => {
      const file = e.target.files[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = (ev) => {
        try {
          const state = JSON.parse(ev.target.result);
          savePartial(state);
          setLog([]);
          try {
            localStorage.removeItem("scaler_log");
          } catch (_) {}
          setPhase("ready");
        } catch {
          alert("Invalid state file — expected JSON from a previous provision run.");
        }
      };
      reader.readAsText(file);
      e.target.value = "";
    },
    [savePartial],
  );

  const Label = ({ children, help }) => (
    <div
      style={{
        fontSize: 11,
        color: "var(--text-label)",
        marginBottom: 5,
        display: "flex",
        alignItems: "center",
        gap: 6,
      }}
    >
      <span>{children}</span>
      {help && <HelpTip text={help} />}
    </div>
  );
  const inp = {
    width: "100%",
    background: "var(--bg-surface)",
    border: "1px solid var(--border-accent)",
    borderRadius: 3,
    padding: "7px 10px",
    color: "var(--text-primary)",
    fontFamily: "inherit",
    fontSize: 12,
    outline: "none",
  };
  const TogglePair = ({ options, value, onSelect }) => (
    <div
      style={{
        display: "flex",
        borderRadius: 3,
        overflow: "hidden",
        border: "1px solid var(--border-accent)",
      }}
    >
      {options.map(([val, lbl, dis]) => (
        <button
          key={val}
          disabled={!!dis}
          onClick={() => !dis && onSelect(val)}
          style={{
            flex: 1,
            padding: "7px 0",
            fontFamily: "inherit",
            fontSize: 11,
            cursor: dis ? "not-allowed" : "pointer",
            border: "none",
            background: value === val ? "rgba(0,200,224,0.18)" : "transparent",
            color: dis ? "var(--text-dim)" : value === val ? "var(--text-accent)" : "var(--text-muted)",
            transition: "background 0.15s, color 0.15s",
          }}
        >
          {lbl}
        </button>
      ))}
    </div>
  );

  const advBtn = (show, onToggle, label) => (
    <button
      onClick={onToggle}
      style={{
        background: "none",
        border: "1px solid var(--border-accent)",
        borderRadius: 3,
        padding: "6px 10px",
        color: "var(--text-muted)",
        fontFamily: "inherit",
        fontSize: 11,
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        width: "100%",
      }}
    >
      <span>{label}</span>
      <span style={{ fontSize: 11 }}>{show ? "▴" : "▾"}</span>
    </button>
  );

  let launchControl;
  if (phase === "error" && provState) {
    launchControl = (
      <button
        onClick={handleDestroy}
        disabled={!hasCredentials}
        style={{
          padding: "8px 20px",
          background: !hasCredentials
            ? "rgba(255,80,60,0.04)"
            : "linear-gradient(135deg, oklch(0.32 0.18 15) 0%, oklch(0.26 0.14 30) 100%)",
          border: "1px solid " + (!hasCredentials ? "var(--border-danger)" : "oklch(0.48 0.18 15)"),
          borderRadius: 4,
          color: !hasCredentials ? "var(--text-danger)" : "oklch(0.88 0.1 30)",
          fontFamily: "inherit",
          fontSize: 11,
          fontWeight: 700,
          cursor: !hasCredentials ? "default" : "pointer",
          transition: "all 0.2s",
          flexShrink: 0,
        }}
      >
        Destroy Cluster{!hasCredentials ? " (missing credentials)" : ""}
      </button>
    );
  } else if (phase === "idle" || phase === "error") {
    launchControl = (
      <button
        onClick={handleLaunch}
        disabled={!formReady}
        style={{
          padding: "8px 20px",
          background: !formReady
            ? "var(--bg-surface)"
            : "linear-gradient(135deg, oklch(0.38 0.16 155) 0%, oklch(0.32 0.14 200) 100%)",
          border: "1px solid " + (!formReady ? "var(--border-accent)" : "oklch(0.55 0.16 155)"),
          borderRadius: 4,
          color: !formReady ? "var(--text-muted)" : "oklch(0.92 0.1 155)",
          fontFamily: "inherit",
          fontSize: 11,
          fontWeight: 700,
          cursor: !formReady ? "default" : "pointer",
          transition: "all 0.2s",
          flexShrink: 0,
        }}
      >
        Launch Cluster
      </button>
    );
  } else if (phase === "ready") {
    launchControl = (
      <button
        onClick={handleDestroy}
        disabled={!hasCredentials}
        style={{
          padding: "8px 20px",
          background: !hasCredentials
            ? "rgba(255,80,60,0.04)"
            : "linear-gradient(135deg, oklch(0.32 0.18 15) 0%, oklch(0.26 0.14 30) 100%)",
          border: "1px solid " + (!hasCredentials ? "var(--border-danger)" : "oklch(0.48 0.18 15)"),
          borderRadius: 4,
          color: !hasCredentials ? "var(--text-danger)" : "oklch(0.88 0.1 30)",
          fontFamily: "inherit",
          fontSize: 11,
          fontWeight: 700,
          cursor: !hasCredentials ? "default" : "pointer",
          transition: "all 0.2s",
          flexShrink: 0,
        }}
      >
        Destroy Cluster{!hasCredentials ? " (missing credentials)" : ""}
      </button>
    );
  } else if (isRunning) {
    launchControl = (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div
          style={{
            padding: "6px 12px",
            background: phase === "destroying" ? "rgba(255,80,60,0.04)" : "rgba(0,200,224,0.04)",
            border: "1px solid " + (phase === "destroying" ? "var(--border-danger)" : "var(--border-accent)"),
            borderRadius: 4,
            color: phase === "destroying" ? "var(--text-danger)" : "var(--text-muted)",
            fontSize: 11,
          }}
        >
          {phase === "destroying" ? "Tearing down…" : "Deploying…"}
        </div>
        <button
          onClick={handleAbort}
          style={{
            padding: "6px 12px",
            background: "transparent",
            border: "1px solid rgba(255,160,60,0.3)",
            borderRadius: 4,
            color: "var(--text-warning)",
            fontFamily: "inherit",
            fontSize: 11,
            cursor: "pointer",
            transition: "border-color 0.15s, color 0.15s",
            flexShrink: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = "rgba(255,160,60,0.6)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = "rgba(255,160,60,0.3)";
          }}
        >
          ✕ Abort
        </button>
      </div>
    );
  }

  return (
    <div
      style={{
        height: "100%",
        background: "var(--bg-page)",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <TopNav
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        theme={theme}
        setTheme={setTheme}
        showPostLaunch={phase !== "idle" || ["deployment", "logs", "gui"].includes(activeTab)}
        launchControl={launchControl}
        guiAddress={phase === "ready" ? provState?.gui_address : undefined}
      />

      {/* ── Config Tab ── */}
      <div
        style={{
          display: activeTab === "config" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div
          style={{
            flex: 1,
            padding: "20px 28px",
            overflowY: "auto",
            display: "flex",
            flexDirection: "column",
            gap: 16,
          }}
        >
          {/* Three columns */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "320px 340px 1fr",
              gap: 16,
              alignItems: "start",
            }}
          >
            {/* Column 1: Credentials + General */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox title="Credentials">
                <div
                  style={{
                    display: "flex",
                    borderBottom: "1px solid var(--border-accent)",
                    gap: 0,
                    marginBottom: 2,
                  }}
                >
                  {[
                    ["aws", "AWS"],
                    ["ibm", "IBM"],
                    ["oci", "OCI"],
                  ].map(([id, lbl]) => {
                    const active = id === "aws";
                    const disabled = id !== "aws";
                    return (
                      <button
                        key={id}
                        disabled={disabled}
                        style={{
                          padding: "5px 12px",
                          fontFamily: "inherit",
                          fontSize: 10,
                          letterSpacing: "0.08em",
                          textTransform: "uppercase",
                          cursor: disabled ? "default" : "pointer",
                          border: "none",
                          marginBottom: -1,
                          borderBottom: active ? "2px solid var(--tab-active)" : "2px solid transparent",
                          background: "transparent",
                          color: active ? "var(--text-label)" : "var(--text-dim)",
                          opacity: disabled ? 0.35 : 1,
                        }}
                      >
                        {lbl}
                      </button>
                    );
                  })}
                </div>
                <div>
                  <Label help="The AWS region where your cluster will be deployed.">AWS Region</Label>
                  <RegionSelect value={region} onChange={setRegion} />
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  <div
                    style={{
                      background: "var(--bg-surface)",
                      border: "1px solid var(--border-accent)",
                      borderRadius: 3,
                      padding: "8px 10px",
                      display: "flex",
                      alignItems: "center",
                    }}
                  >
                    <span
                      style={{
                        fontSize: 10,
                        color: "var(--text-muted)",
                        marginRight: 8,
                        flexShrink: 0,
                      }}
                    >
                      KEY_ID
                    </span>
                    <SecretInput
                      value={accessKeyId}
                      onChange={setAKI}
                      placeholder="AKIA…"
                      style={{
                        flex: 1,
                        fontSize: 12,
                        color: "var(--text-primary)",
                      }}
                    />
                  </div>
                  <div
                    style={{
                      background: "var(--bg-surface)",
                      border: "1px solid var(--border-accent)",
                      borderRadius: 3,
                      padding: "8px 10px",
                      display: "flex",
                      alignItems: "center",
                    }}
                  >
                    <span
                      style={{
                        fontSize: 10,
                        color: "var(--text-muted)",
                        marginRight: 8,
                        flexShrink: 0,
                      }}
                    >
                      SECRET
                    </span>
                    <SecretInput
                      value={secretKey}
                      onChange={setSK}
                      placeholder="wJalr…"
                      style={{
                        flex: 1,
                        fontSize: 12,
                        color: "var(--text-primary)",
                      }}
                    />
                  </div>
                  <a
                    href="https://console.aws.amazon.com/iam/home#/security_credentials"
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{
                      fontSize: 10,
                      color: "var(--text-muted)",
                      textDecoration: "none",
                      alignSelf: "flex-end",
                    }}
                    onMouseOver={(e) => (e.currentTarget.style.color = "var(--text-accent)")}
                    onMouseOut={(e) => (e.currentTarget.style.color = "var(--text-muted)")}
                  >
                    Generate access keys in AWS Console ↗
                  </a>
                  <span
                    style={{
                      fontSize: 10,
                      color: "var(--text-dim)",
                      lineHeight: 1.5,
                    }}
                  >
                    Your credentials are never stored anywhere and never leave your machine. All API calls are made
                    directly from your browser.
                  </span>
                </div>
              </PanelBox>

              <PanelBox title="General Options">
                <div>
                  <Label
                    help={
                      "WebSocket — connect to your cluster from a browser or any WebSocket client. Requires YMQ.\n---\nTCP — direct socket connection; slightly lower overhead. Works with YMQ or ZMQ."
                    }
                  >
                    Transport Protocol
                  </Label>
                  <TogglePair
                    options={[
                      ["ws", "WS"],
                      ["tcp", "TCP"],
                    ]}
                    value={transport}
                    onSelect={(v) => {
                      setTransport(v);
                      if (v === "ws") setNetBack("ymq");
                    }}
                  />
                </div>
                <div>
                  <Label
                    help={
                      "YMQ — OpenGRIS's built-in high-performance networking layer. Required for WebSocket connections. Recommended.\n---\nZMQ — industry-standard messaging library. TCP only. Use if you need ZMQ compatibility."
                    }
                  >
                    Network Backend
                  </Label>
                  <TogglePair
                    options={[
                      ["ymq", "YMQ"],
                      ["zmq", "ZMQ", transport === "ws"],
                    ]}
                    value={networkBackend}
                    onSelect={(v) => {
                      setNetBack(v);
                      if (v === "zmq") setTransport("tcp");
                    }}
                  />
                </div>
                <div>
                  <Label help="Python version installed via uv on the scheduler and all workers.">
                    Python Version
                  </Label>
                  <input
                    value={pythonVersion}
                    onChange={(e) => setPyVer(e.target.value)}
                    style={inp}
                    placeholder="3.14"
                  />
                </div>
              </PanelBox>
            </div>

            {/* Column 2: Scheduler EC2 + Policy */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox title="Scheduler">
                <div>
                  <Label help="EC2 instance type for the scheduler. Compute-optimized (c5/c6i) works well for most deployments.">
                    Instance Type
                  </Label>
                  <InstancePicker value={schedulerType} onChange={setSchedulerType} defaultCat="all" />
                </div>
                <div
                  style={{
                    padding: "10px 12px",
                    background: "rgba(0,255,136,0.04)",
                    border: "1px solid var(--border-success)",
                    borderRadius: 3,
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "baseline",
                  }}
                >
                  <span
                    style={{
                      fontSize: 10,
                      color: "var(--text-muted)",
                    }}
                  >
                    Cost
                  </span>
                  <span
                    style={{
                      fontSize: 13,
                      fontWeight: 600,
                      color: "var(--text-success)",
                    }}
                  >
                    USD {schedulerInst.price.toFixed(2)}/h
                  </span>
                </div>
                {advBtn(showSchedAdv, () => setShowSchedAdv((v) => !v), "Advanced")}
                {showSchedAdv && (
                  <div
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      gap: 10,
                    }}
                  >
                    <div>
                      <Label>Scheduler Port</Label>
                      <NumericStepper value={schedulerPort} onChange={setSchedPort} min={1024} max={65535} width={80} />
                    </div>
                    <div>
                      <Label>Object Storage Port</Label>
                      <NumericStepper
                        value={objectStoragePort}
                        onChange={setObjPort}
                        min={1024}
                        max={65535}
                        width={80}
                      />
                    </div>
                    {portConflicts.length > 0 && (
                      <div style={{ color: "var(--text-danger)", fontSize: 11, lineHeight: 1.5 }}>
                        {portConflicts.map((msg, i) => (
                          <div key={i}>{msg}</div>
                        ))}
                      </div>
                    )}
                    <div>
                      <Label help={"- Installed on the scheduler instance\n- Shared by native worker manager workers (same instance)\n- opengris-scaler must be included"}>
                        requirements.txt
                      </Label>
                      <textarea
                        value={schedulerRequirements}
                        onChange={(e) => setSchedulerReqs(e.target.value)}
                        style={{
                          width: "100%",
                          background: "var(--bg-surface)",
                          border: "1px solid var(--border-accent)",
                          borderRadius: 3,
                          padding: "7px 10px",
                          color: "var(--text-primary)",
                          fontFamily: "inherit",
                          fontSize: 11,
                          outline: "none",
                          resize: "vertical",
                          minHeight: 72,
                          lineHeight: 1.6,
                        }}
                      />
                    </div>
                  </div>
                )}
              </PanelBox>

              {/* Policy panel — display only, not yet wired up */}
              <div
                style={{
                  opacity: 0.45,
                  pointerEvents: "none",
                  userSelect: "none",
                }}
              >
                <PanelBox title="Policy">
                  <div>
                    <Label help="Policy engine that controls task allocation and worker scaling.">Engine</Label>
                    <select
                      disabled
                      style={{
                        width: "100%",
                        background: "var(--bg-surface)",
                        border: "1px solid var(--border-accent)",
                        borderRadius: 3,
                        padding: "7px 10px",
                        color: "var(--text-primary)",
                        fontFamily: "inherit",
                        fontSize: 12,
                        outline: "none",
                      }}
                    >
                      <option value="simple">simple</option>
                      <option value="waterfall_v1">waterfall_v1</option>
                    </select>
                  </div>
                  <div>
                    <Label help="How tasks are assigned to workers. even_load distributes work evenly; capability routes tasks to workers that advertise matching capabilities.">
                      Allocate
                    </Label>
                    <select
                      disabled
                      style={{
                        width: "100%",
                        background: "var(--bg-surface)",
                        border: "1px solid var(--border-accent)",
                        borderRadius: 3,
                        padding: "7px 10px",
                        color: "var(--text-primary)",
                        fontFamily: "inherit",
                        fontSize: 12,
                        outline: "none",
                      }}
                    >
                      <option value="even_load">even_load</option>
                      <option value="capability">capability</option>
                    </select>
                  </div>
                  <div>
                    <Label help="How the scheduler scales worker counts up or down. vanilla uses a task-to-worker ratio; capability scales per-capability group; no disables autoscaling.">
                      Scaling
                    </Label>
                    <select
                      disabled
                      style={{
                        width: "100%",
                        background: "var(--bg-surface)",
                        border: "1px solid var(--border-accent)",
                        borderRadius: 3,
                        padding: "7px 10px",
                        color: "var(--text-primary)",
                        fontFamily: "inherit",
                        fontSize: 12,
                        outline: "none",
                      }}
                    >
                      <option value="vanilla">vanilla</option>
                      <option value="no">no</option>
                      <option value="capability">capability</option>
                    </select>
                  </div>
                </PanelBox>
              </div>
            </div>

            {/* Column 3: Worker Managers + Cost Summary */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox title={`Worker Managers (${workerManagers.length})`} style={{ gap: 8, padding: "16px 22px 0" }}>
                <div
                  style={{
                    display: "flex",
                    marginLeft: -22,
                    marginRight: -22,
                    borderTop: "1px solid var(--border-accent)",
                    height: 360,
                    overflow: "hidden",
                  }}
                >
                  {/* vertical tab list */}
                  <div
                    style={{
                      width: 130,
                      borderRight: "1px solid var(--border-accent)",
                      display: "flex",
                      flexDirection: "column",
                      flexShrink: 0,
                      overflowY: "auto",
                    }}
                  >
                    {workerManagers.map((wm) => (
                      <button
                        key={wm.id}
                        title={wm.id}
                        onClick={() => setSelectedWmId(wm.id)}
                        style={{
                          background: selectedWmId === wm.id ? "rgba(0,200,224,0.1)" : "transparent",
                          borderLeft: selectedWmId === wm.id ? "2px solid var(--tab-active)" : "2px solid transparent",
                          borderRight: "none",
                          borderTop: "none",
                          borderBottom: "1px solid rgba(255,255,255,0.04)",
                          color: selectedWmId === wm.id ? "var(--text-accent)" : "var(--text-muted)",
                          fontFamily: "inherit",
                          fontSize: 10,
                          padding: "10px 10px",
                          textAlign: "left",
                          cursor: "pointer",
                          letterSpacing: "0.05em",
                          transition: "background 0.12s, color 0.12s",
                          width: "100%",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {wm.id}
                      </button>
                    ))}
                    <button
                      onClick={addWorkerManager}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.background = "rgba(0,200,224,0.08)";
                        e.currentTarget.style.color = "var(--text-accent)";
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.background = "transparent";
                        e.currentTarget.style.color = "var(--accent-cyan)";
                      }}
                      style={{
                        background: "transparent",
                        border: "none",
                        borderTop: "1px dashed rgba(0,200,224,0.2)",
                        color: "var(--accent-cyan)",
                        fontFamily: "inherit",
                        fontSize: 10,
                        padding: "10px 10px",
                        cursor: "pointer",
                        textAlign: "left",
                        marginTop: "auto",
                        letterSpacing: "0.05em",
                        transition: "background 0.12s, color 0.12s",
                      }}
                    >
                      + Add
                    </button>
                  </div>
                  {/* selected card */}
                  <div style={{ flex: 1, overflowY: "auto", padding: "14px 16px" }}>
                    {workerManagers
                      .filter((wm) => wm.id === selectedWmId)
                      .map((wm) => (
                        <WorkerManagerCard
                          key={wm._uid}
                          wm={wm}
                          onChange={(updated) => {
                            if (updated.id !== wm.id) setSelectedWmId(updated.id);
                            updateWorkerManager(wm.id, updated);
                          }}
                          onRemove={() => removeWorkerManager(wm.id)}
                          allInstances={allInstances}
                          canRemove={workerManagers.length > 1}
                          fullWidth={true}
                        />
                      ))}
                  </div>
                </div>
              </PanelBox>

              <PanelBox title="Cost Summary">
                {workerManagers.map((wm, idx) => {
                  const label = wm.id || `(wm ${idx + 1})`;
                  if (wm.type !== "orb_aws_ec2")
                    return (
                      <div
                        key={wm._uid}
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          alignItems: "baseline",
                        }}
                      >
                        <span
                          style={{
                            fontSize: 10,
                            color: "var(--text-muted)",
                          }}
                        >
                          {label}
                        </span>
                        <span
                          style={{
                            fontSize: 11,
                            color: "var(--text-dim)",
                            fontStyle: "italic",
                          }}
                        >
                          n/a
                        </span>
                      </div>
                    );
                  const inst = allInstances.find((i) => i.type === wm.instanceType) || { price: 0 };
                  const count =
                    wm.capMode === "instances"
                      ? Math.max(0, wm.instanceCap || 0)
                      : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
                  return (
                    <div
                      key={wm._uid}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "baseline",
                      }}
                    >
                      <span
                        style={{
                          fontSize: 10,
                          color: "var(--text-muted)",
                        }}
                      >
                        {label} · {count}× {wm.instanceType}
                      </span>
                      <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                        USD {(count * inst.price).toFixed(2)}/h
                      </span>
                    </div>
                  );
                })}
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "baseline",
                  }}
                >
                  <span
                    style={{
                      fontSize: 10,
                      color: "var(--text-muted)",
                    }}
                  >
                    Scheduler · {schedulerType}
                  </span>
                  <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                    USD {schedulerInst.price.toFixed(2)}/h
                  </span>
                </div>
                <div
                  style={{
                    borderTop: "1px solid var(--border-success)",
                    paddingTop: 10,
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "baseline",
                  }}
                >
                  <span
                    style={{
                      fontSize: 11,
                      color: "var(--text-accent)",
                      fontWeight: 600,
                    }}
                  >
                    Max est. total
                  </span>
                  <span
                    style={{
                      fontSize: 16,
                      fontWeight: 700,
                      color: "var(--text-success)",
                    }}
                  >
                    USD {totalCostPerHr.toFixed(2)}/h
                  </span>
                </div>
              </PanelBox>
            </div>
          </div>

          {/* Load state / download config links (idle only) */}
          {phase === "idle" && (
            <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
              <label
                style={{
                  fontSize: 10,
                  color: "var(--text-accent)",
                  cursor: "pointer",
                  textDecoration: "underline",
                  textDecorationColor: "var(--border-accent)",
                }}
              >
                Load state from file
                <input type="file" accept=".json" onChange={handleLoadState} style={{ display: "none" }} />
              </label>
              <span
                style={{
                  width: 1,
                  height: 12,
                  background: "var(--text-muted)",
                  display: "inline-block",
                }}
              />
              <button
                onClick={handleDownloadConfig}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  color: "var(--text-accent)",
                  fontFamily: "inherit",
                  fontSize: 10,
                  padding: 0,
                  letterSpacing: "0.06em",
                  textDecoration: "underline",
                  textDecorationColor: "var(--border-accent)",
                }}
              >
                Download config.toml
              </button>
            </div>
          )}
        </div>
      </div>

      {/* ── Deployment Tab ── */}
      <div
        style={{
          display: activeTab === "deployment" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div
          style={{
            flex: 1,
            padding: "20px 28px",
            display: "grid",
            gridTemplateColumns: "1fr 600px",
            gap: 20,
            minHeight: 0,
            overflow: "hidden",
          }}
        >
          {/* Left: terminal only */}
          <LiveTerminal lines={log} isRunning={isRunning} bare style={{ minHeight: 0 }} />
          {/* Right: active deployment card */}
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 12,
              overflowY: "auto",
              minHeight: 0,
            }}
          >
            {provState && phase !== "destroying" && (
              <div
                style={{
                  animation: "fadeSlideIn 0.3s ease",
                  display: "flex",
                  flexDirection: "column",
                  gap: 12,
                }}
              >
                {phase === "ready" && log.length === 0 && (
                  <div
                    style={{
                      padding: "10px 14px",
                      background: "var(--bg-surface)",
                      border: "1px solid var(--border-accent)",
                      borderRadius: 3,
                      fontSize: 11,
                      color: "var(--text-muted)",
                    }}
                  >
                    Deployment loaded from saved state.
                  </div>
                )}
                <DeploymentCard
                  state={provState}
                  onDownload={() =>
                    downloadText("scaler-state-" + provState.name_suffix + ".json", JSON.stringify(provState, null, 2))
                  }
                  isRunning={isRunning}
                  keyMaterial={keyMaterial}
                />
                <GettingStartedCard schedulerAddress={provState.scheduler_address} ready={phase === "ready"} />
              </div>
            )}
            {phase === "error" && (
              <button
                onClick={handleReset}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  color: "var(--text-muted)",
                  fontFamily: "inherit",
                  fontSize: 10,
                  padding: 0,
                  letterSpacing: "0.06em",
                }}
              >
                ← Clear state
              </button>
            )}
          </div>
        </div>
      </div>

      {/* ── Scheduler Logs Tab ── */}
      <div
        style={{
          display: activeTab === "logs" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div
          style={{
            flex: 1,
            minHeight: 0,
            padding: "20px 28px",
            display: "flex",
            flexDirection: "column",
          }}
        >
          {!provState?.instance_id ? (
            <div style={{ color: "var(--text-muted)", fontSize: 12 }}>No instance deployed yet.</div>
          ) : (
            <SchedulerLogTerminal
              instanceId={provState.instance_id}
              region={provState.region}
              credentials={{ accessKeyId, secretKey }}
              isActive={activeTab === "logs"}
            />
          )}
        </div>
      </div>

      {/* ── GUI Tab ── */}
      <div
        style={{
          display: activeTab === "gui" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        {!provState?.gui_address ? (
          <div
            style={{
              padding: "20px 28px",
              color: "var(--text-muted)",
              fontSize: 12,
            }}
          >
            GUI address not yet available.
          </div>
        ) : (
          <>
            <div
              style={{
                padding: "8px 14px",
                background: "var(--bg-panel)",
                borderBottom: "1px solid var(--border-accent)",
                display: "flex",
                gap: 10,
                alignItems: "center",
                flexShrink: 0,
              }}
            >
              <span style={{ fontSize: 11, color: "var(--text-muted)" }}>{provState.gui_address}</span>
              <a
                href={provState.gui_address}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  fontSize: 10,
                  color: "var(--text-accent)",
                  border: "1px solid var(--border-accent)",
                  borderRadius: 3,
                  padding: "2px 8px",
                  textDecoration: "none",
                }}
              >
                Open in new tab
              </a>
              {guiReady ? (
                <span style={{ fontSize: 10, color: "var(--text-success)" }}>server ready</span>
              ) : (
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>waiting for server… {guiElapsed}s</span>
              )}
            </div>
            {guiReady ? (
              <iframe
                src={provState.gui_address}
                style={{
                  flex: 1,
                  border: "none",
                  background: "var(--bg-page)",
                }}
                title="Scaler GUI"
              />
            ) : (
              <div
                style={{
                  flex: 1,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  flexDirection: "column",
                  gap: 10,
                  color: "var(--text-muted)",
                }}
              >
                <div style={{ fontSize: 13 }}>Waiting for GUI server to start</div>
                <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{guiElapsed}s elapsed · retrying every 5s</div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
