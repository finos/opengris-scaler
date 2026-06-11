const { useState, useEffect, useCallback, useRef } = React;

const IS_ADVANCED = new URLSearchParams(window.location.search).has('advanced');

const OCI_SHAPE_PRICING = {
  "CI.Standard.A1.Flex": { ocpuPrice: 0.013106, memPrice: 0.0019659 },
  "CI.Standard.E4.Flex": { ocpuPrice: 0.032765, memPrice: 0.0019659 },
};

/* ── NumericStepper ── */
function NumericStepper({
  value,
  onChange,
  min = 0,
  max = Infinity,
  step = 1,
  width = 56,
}) {
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
function WorkerManagerCard({
  wm,
  onChange,
  onRemove,
  allInstances,
  canRemove,
  fullWidth,
}) {
  const [localId, setLocalId] = useState(wm.id);
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
            color: disabled
              ? "var(--text-dim)"
              : value === val
                ? "var(--text-accent)"
                : "var(--text-muted)",
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
          <WorkerManagerTypeSelect
            value={wm.type}
            onChange={(v) => {
              if (v === "oci_raw") {
                onChange({ ...wm, type: v, ociShape: "CI.Standard.A1.Flex", ociContainerImage: "ghcr.io/finos/scaler:latest-arm64" });
              } else {
                set("type", v);
              }
            }}
          />
        </div>
        <div
          style={{ display: "flex", flexDirection: "column", gap: 4, flex: 1 }}
        >
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
      </div>

      {/* orb_aws_ec2 */}
      {wm.type === "orb_aws_ec2" && (
        <>
          <div>
            <Label>Worker Instance Type</Label>
            <InstancePicker
              value={wm.instanceType}
              onChange={(v) => set("instanceType", v)}
              defaultCat="all"
            />
          </div>
          <div>
            <Label>Budget</Label>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {wm.capMode === "instances" ? (
                <NumericStepper
                  value={wm.instanceCap || 1}
                  onChange={(v) => set("instanceCap", v)}
                  min={1}
                  max={1000}
                />
              ) : (
                <NumericStepper
                  value={wm.budgetCap || 10}
                  onChange={(v) => set("budgetCap", v)}
                  min={0}
                  step={0.5}
                  width={64}
                />
              )}
              <select
                value={wm.capMode}
                onChange={(e) => set("capMode", e.target.value)}
                style={{
                  background: "var(--bg-surface)",
                  border: "1px solid var(--border-accent)",
                  borderRadius: 3,
                  padding: "6px 8px",
                  color: "var(--text-primary)",
                  fontFamily: "inherit",
                  fontSize: 11,
                  outline: "none",
                  cursor: "pointer",
                }}
              >
                <option value="budget">USD/h cap</option>
                <option value="instances">instance cap</option>
              </select>
            </div>
          </div>
          <div>
            <Label
              help={
                "- Installed on each worker instance\n- opengris-scaler must be included"
              }
            >
              requirements.txt
            </Label>
            <textarea
              value={wm.requirements}
              onChange={(e) => set("requirements", e.target.value)}
              spellCheck={false}
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
            <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
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
              <NumericStepper
                value={wm.ecsTaskCpu || 4}
                onChange={(v) => set("ecsTaskCpu", v)}
                min={1}
                max={64}
              />
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

      {/* oci_raw */}
      {wm.type === "oci_raw" && (() => {
        const ociShape = wm.ociShape || "CI.Standard.A1.Flex";
        const ociPricing = OCI_SHAPE_PRICING[ociShape] || OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
        const ociCostPerHr = ociPricing.ocpuPrice * (wm.ociOcpus || 4) + ociPricing.memPrice * (wm.ociMemoryGb || 30);
        return (
          <>
            <div>
              <Label help="OCI Compartment OCID where container instances are launched.">Compartment ID</Label>
              <input
                value={wm.ociCompartmentId || ""}
                onChange={(e) => set("ociCompartmentId", e.target.value)}
                style={inp}
                placeholder="ocid1.compartment.oc1..aaa..."
              />
            </div>
            <div>
              <Label help="OCI Availability Domain (e.g. AD-1 or Uocm:PHX-AD-1).">Availability Domain</Label>
              <input
                value={wm.ociAvailabilityDomain || ""}
                onChange={(e) => set("ociAvailabilityDomain", e.target.value)}
                style={inp}
                placeholder="AD-1"
              />
            </div>
            <div>
              <Label help="OCI Subnet OCID for container instance network interfaces.">Subnet ID</Label>
              <input
                value={wm.ociSubnetId || ""}
                onChange={(e) => set("ociSubnetId", e.target.value)}
                style={inp}
                placeholder="ocid1.subnet.oc1..aaa..."
              />
            </div>
            <div>
              <Label help="OCI region identifier.">Region</Label>
              <input
                value={wm.ociRegion || ""}
                onChange={(e) => set("ociRegion", e.target.value)}
                style={inp}
                placeholder="us-ashburn-1"
              />
            </div>
            <div>
              <Label help="Instance shape determines the CPU architecture and pricing.">Instance Shape</Label>
              <OciShapeSelect
                value={ociShape}
                onChange={(v) => {
                  const image = v === "CI.Standard.A1.Flex"
                    ? "ghcr.io/finos/scaler:latest-arm64"
                    : "ghcr.io/finos/scaler:latest-amd64";
                  onChange({ ...wm, ociShape: v, ociContainerImage: image });
                }}
              />
            </div>
            <div style={{ display: "flex", gap: 10 }}>
              <div style={{ flex: 1 }}>
                <Label help="Number of OCPUs per container instance (also determines worker count).">OCPUs</Label>
                <NumericStepper
                  value={wm.ociOcpus || 4}
                  onChange={(v) => set("ociOcpus", v)}
                  min={1}
                  max={64}
                />
              </div>
              <div style={{ flex: 1 }}>
                <Label>Memory (GB)</Label>
                <NumericStepper
                  value={wm.ociMemoryGb || 30}
                  onChange={(v) => set("ociMemoryGb", v)}
                  min={1}
                  max={512}
                />
              </div>
            </div>
            <div>
              <Label help={"- Installed inside the container instance\n- opengris-scaler must be included"}>requirements.txt</Label>
              <textarea
                value={wm.requirements || ""}
                onChange={(e) => set("requirements", e.target.value)}
                spellCheck={false}
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
            <div
              style={{
                padding: "8px 10px",
                background: "rgba(0,255,136,0.04)",
                border: "1px solid var(--border-success)",
                borderRadius: 3,
                display: "flex",
                flexDirection: "column",
                gap: 4,
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
                  {wm.ociOcpus || 4} OCPU × ${ociPricing.ocpuPrice.toFixed(2)}/h
                </span>
                <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                  ${(ociPricing.ocpuPrice * (wm.ociOcpus || 4)).toFixed(2)}/h
                </span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
                  {wm.ociMemoryGb || 30} GB × ${ociPricing.memPrice.toFixed(3)}/h
                </span>
                <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                  ${(ociPricing.memPrice * (wm.ociMemoryGb || 30)).toFixed(2)}/h
                </span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", borderTop: "1px solid var(--border-success)", paddingTop: 4, marginTop: 2 }}>
                <span style={{ fontSize: 10, color: "var(--text-muted)" }}>Total</span>
                <span style={{ fontSize: 13, fontWeight: 600, color: "var(--text-success)" }}>
                  USD {ociCostPerHr.toFixed(2)}/h
                </span>
              </div>
            </div>
          </>
        );
      })()}

      {/* oci_hpc */}
      {wm.type === "oci_hpc" && (
        <>
          <div>
            <Label help="OCI Compartment OCID where container instances are launched.">Compartment ID</Label>
            <input
              value={wm.ociCompartmentId || ""}
              onChange={(e) => set("ociCompartmentId", e.target.value)}
              style={inp}
              placeholder="ocid1.compartment.oc1..aaa..."
            />
          </div>
          <div>
            <Label help="OCI Availability Domain (e.g. AD-1 or Uocm:PHX-AD-1).">Availability Domain</Label>
            <input
              value={wm.ociAvailabilityDomain || ""}
              onChange={(e) => set("ociAvailabilityDomain", e.target.value)}
              style={inp}
              placeholder="AD-1"
            />
          </div>
          <div>
            <Label help="OCI Subnet OCID for container instance network interfaces.">Subnet ID</Label>
            <input
              value={wm.ociSubnetId || ""}
              onChange={(e) => set("ociSubnetId", e.target.value)}
              style={inp}
              placeholder="ocid1.subnet.oc1..aaa..."
            />
          </div>
          <div>
            <Label help="OCIR image URI (e.g. &lt;region&gt;.ocir.io/&lt;ns&gt;/&lt;repo&gt;:latest).">Container Image</Label>
            <input
              value={wm.ociContainerImage || ""}
              onChange={(e) => set("ociContainerImage", e.target.value)}
              style={inp}
              placeholder="us-ashburn-1.ocir.io/myns/scaler:latest"
            />
          </div>
          <div>
            <Label help="OCI region identifier.">Region</Label>
            <input
              value={wm.ociRegion || "us-ashburn-1"}
              onChange={(e) => set("ociRegion", e.target.value)}
              style={inp}
              placeholder="us-ashburn-1"
            />
          </div>
          <div>
            <Label help="OCI Object Storage tenancy namespace.">Object Storage Namespace</Label>
            <input
              value={wm.ociObjectStorageNamespace || ""}
              onChange={(e) => set("ociObjectStorageNamespace", e.target.value)}
              style={inp}
              placeholder="mytenancy"
            />
          </div>
          <div>
            <Label>Object Storage Bucket</Label>
            <input
              value={wm.ociObjectStorageBucket || ""}
              onChange={(e) => set("ociObjectStorageBucket", e.target.value)}
              style={inp}
              placeholder="scaler-tasks-bucket"
            />
          </div>
          <div>
            <Label help="Key prefix for task inputs and results in Object Storage.">Object Storage Prefix</Label>
            <input
              value={wm.ociObjectStoragePrefix || "scaler-tasks"}
              onChange={(e) => set("ociObjectStoragePrefix", e.target.value)}
              style={inp}
              placeholder="scaler-tasks"
            />
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label>OCPUs per Job</Label>
              <NumericStepper
                value={wm.ociOcpus || 1}
                onChange={(v) => set("ociOcpus", v)}
                min={1}
                max={64}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label>Memory (GB)</Label>
              <NumericStepper
                value={wm.ociMemoryGb || 6}
                onChange={(v) => set("ociMemoryGb", v)}
                min={1}
                max={512}
              />
            </div>
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label help="Maximum number of concurrently running container instances.">Max Concurrent Jobs</Label>
              <NumericStepper
                value={wm.ociMaxConcurrentJobs || 100}
                onChange={(v) => set("ociMaxConcurrentJobs", v)}
                min={1}
                max={10000}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label>Timeout (min)</Label>
              <NumericStepper
                value={wm.ociJobTimeoutMinutes || 60}
                onChange={(v) => set("ociJobTimeoutMinutes", v)}
                min={1}
                max={1440}
              />
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/* ── CopyBtn ── */
function copyText(text, onSuccess) {
  if (navigator.clipboard) {
    navigator.clipboard.writeText(text).then(onSuccess);
  } else {
    const el = document.createElement("textarea");
    el.value = text;
    el.style.position = "fixed";
    el.style.opacity = "0";
    document.body.appendChild(el);
    el.select();
    document.execCommand("copy");
    document.body.removeChild(el);
    onSuccess();
  }
}

function CopyBtn({ value }) {
  const [copied, setCopied] = useState(false);
  const [hov, setHov] = useState(false);
  return (
    <button
      onClick={() =>
        copyText(value, () => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        })
      }
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        background: hov && !copied ? "rgba(0,200,224,0.08)" : "none",
        border:
          "1px solid " +
          (copied
            ? "var(--border-success)"
            : hov
              ? "var(--border-strong)"
              : "var(--border-accent)"),
        borderRadius: 3,
        color: copied
          ? "var(--text-success)"
          : hov
            ? "var(--text-accent)"
            : "var(--text-muted)",
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
    { label: "Worker Monitor", value: state.worker_monitor_address, href: state.worker_monitor_address },
    {
      label: "SSH",
      value: state.public_ip
        ? "chmod 400 " +
          state.key_file +
          " &&\nssh -i " +
          state.key_file +
          " ec2-user@" +
          state.public_ip
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
      <div style={{ display: "flex", alignItems: "center" }}>
        <div
          style={{
            fontSize: 11,
            color: "var(--text-success)",
            fontWeight: 600,
          }}
        >
          Active Deployment
        </div>
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
              onClick={() =>
                downloadText(keyMaterial.name + ".pem", keyMaterial.mat)
              }
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
            <span
              style={{
                fontSize: 12,
                color: "var(--text-dim)",
                fontStyle: "italic",
              }}
            >
              pending…
            </span>
          ) : (
            <span
              style={{
                fontSize: 11,
                color: "var(--text-dim)",
                lineHeight: 1.5,
              }}
            >
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
  "False",
  "None",
  "True",
  "and",
  "as",
  "assert",
  "async",
  "await",
  "break",
  "class",
  "continue",
  "def",
  "del",
  "elif",
  "else",
  "except",
  "finally",
  "for",
  "from",
  "global",
  "if",
  "import",
  "in",
  "is",
  "lambda",
  "nonlocal",
  "not",
  "or",
  "pass",
  "raise",
  "return",
  "try",
  "while",
  "with",
  "yield",
]);

function tokenizePython(code) {
  const tokens = [];
  const re =
    /(#[^\n]*)|("""[\s\S]*?"""|'''[\s\S]*?'''|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)(\s*\()?|(\s+|[^\w\s#"']+)/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    if (m[1]) tokens.push({ text: m[1], color: "var(--text-dim)" });
    else if (m[2]) tokens.push({ text: m[2], color: "var(--text-warning)" });
    else if (m[3]) tokens.push({ text: m[3], color: "var(--text-danger)" });
    else if (m[4]) {
      const word = m[4],
        call = m[5] || "";
      const color = PY_KEYWORDS.has(word)
        ? "var(--text-accent)"
        : call
          ? "var(--text-success)"
          : "var(--text-primary)";
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
        <span key={i} style={{ color: t.color }}>
          {t.text}
        </span>
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
          <div
            style={{ fontSize: 11, color: "var(--text-dim)", lineHeight: 1.5 }}
          >
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
        <div
          style={{
            fontSize: 11,
            color: "var(--text-dim)",
            fontStyle: "italic",
          }}
        >
          Waiting for scheduler…
        </div>
      )}
    </div>
  );
}

/* ── TopNav ── */
function TopNav({
  activeTab,
  setActiveTab,
  theme,
  setTheme,
  showPostLaunch,
  launchControl,
  workerMonitorAddress,
}) {
  const tabs = [
    { id: "config", label: "Config" },
    { id: "deployment", label: "Deployment", postLaunch: true },
    { id: "logs", label: "Scheduler Logs", postLaunch: true },
    // { id: "worker-monitor", label: "Worker Monitor", postLaunch: true },
    {
      id: "worker-monitor",
      label: "Worker Monitor",
      postLaunch: true,
      isLink: true,
      href: workerMonitorAddress,
    },
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
        {tabs.map((t) => {
          const disabled = t.postLaunch && !showPostLaunch;
          return t.isLink ? (
            <a
              key={t.id}
              href={disabled ? undefined : t.href}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                padding: "14px 18px",
                background: "transparent",
                border: "none",
                borderBottom: "2px solid transparent",
                color:
                  disabled || !t.href ? "var(--text-dim)" : "var(--text-muted)",
                fontFamily: "inherit",
                fontSize: 12,
                cursor: disabled || !t.href ? "default" : "pointer",
                textDecoration: "none",
                display: "inline-flex",
                alignItems: "center",
                pointerEvents: disabled || !t.href ? "none" : undefined,
                opacity: disabled ? 0.4 : 1,
              }}
            >
              {t.label} ↗
            </a>
          ) : (
            <button
              key={t.id}
              onClick={() => !disabled && setActiveTab(t.id)}
              style={{
                padding: "14px 18px",
                background: "transparent",
                border: "none",
                borderBottom:
                  activeTab === t.id
                    ? "2px solid var(--tab-active)"
                    : "2px solid transparent",
                color: disabled
                  ? "var(--text-dim)"
                  : activeTab === t.id
                    ? "var(--text-accent)"
                    : "var(--text-muted)",
                fontFamily: "inherit",
                fontSize: 12,
                cursor: disabled ? "default" : "pointer",
                opacity: disabled ? 0.4 : 1,
              }}
            >
              {t.label}
            </button>
          );
        })}
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
  const [credTab, setCredTab] = useState("aws");
  const [ociUserId, setOciUserId] = useState("");
  const [ociTenancyId, setOciTenancyId] = useState("");
  const [ociFingerprint, setOciFingerprint] = useState("");
  const [ociPrivateKey, setOciPrivateKey] = useState("");
  const [transport, setTransport] = useState("ws");
  const [networkBackend, setNetBack] = useState("ymq");
  const [pythonVersion, setPyVer] = useState("3.13");
  const [schedulerRequirements, setSchedulerReqs] = useState(
    "opengris-scaler[all]",
  );
  const [schedulerType, setSchedulerType] = useState("c5.xlarge");
  const [schedulerPort, setSchedPort] = useState(6788);
  const [objectStoragePort, setObjPort] = useState(6789);
  const [activeTab, setActiveTab] = useState("config");
  const [theme, setTheme] = useState(
    () =>
      localStorage.getItem("launchpad-theme") ||
      (window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"),
  );

  const wmCounterRef = useRef(1);
  const [workerManagers, setWorkerManagers] = useState([
    {
      _uid: 1,
      id: "wm-1",
      type: "orb_aws_ec2",
      instanceType: "t3.medium",
      capMode: "budget",
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
  const [pausedOp, setPausedOp] = useState(null); // "deploy" | "teardown"
  const abortRef = useRef(null);
  const partialRef = useRef(null); // latest partial state, readable synchronously in catch blocks

  const [workerMonitorReady, setWorkerMonitorReady] = useState(false);
  const [workerMonitorElapsed, setWorkerMonitorElapsed] = useState(0);

  useEffect(() => {
    const addr = provState?.worker_monitor_address;
    if (!addr) {
      setWorkerMonitorReady(false);
      setWorkerMonitorElapsed(0);
      return;
    }
    setWorkerMonitorReady(false);
    setWorkerMonitorElapsed(0);
    let cancelled = false;
    const start = Date.now();
    const ticker = setInterval(() => {
      if (!cancelled) setWorkerMonitorElapsed(Math.floor((Date.now() - start) / 1000));
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
          setWorkerMonitorReady(true);
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
  }, [provState?.worker_monitor_address]);

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
    partialRef.current = partial;
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
    if (wm.type === "orb_aws_ec2") {
      const inst = allInstances.find((i) => i.type === wm.instanceType) || { price: 0 };
      const count =
        wm.capMode === "instances"
          ? Math.max(0, wm.instanceCap || 0)
          : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
      return count * inst.price;
    }
    if (wm.type === "oci_raw") {
      const shape = wm.ociShape || "CI.Standard.A1.Flex";
      const pricing = OCI_SHAPE_PRICING[shape] || OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
      return pricing.ocpuPrice * (wm.ociOcpus || 4) + pricing.memPrice * (wm.ociMemoryGb || 30);
    }
    return 0;
  });
  const totalCostPerHr =
    schedulerInst.price + wmCosts.reduce((a, b) => a + b, 0);

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
        capMode: "budget",
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
    (id, updated) =>
      setWorkerManagers((prev) =>
        prev.map((wm) => (wm.id === id ? updated : wm)),
      ),
    [],
  );

  const hasCredentials =
    accessKeyId.trim().length > 0 && secretKey.trim().length > 0;

  const monitorPort = schedulerPort + 2;
  const WORKER_MONITOR_PORT = 50001;
  const portConflicts = [];
  if (schedulerPort === objectStoragePort)
    portConflicts.push("Scheduler port and object storage port must differ.");
  if (objectStoragePort === monitorPort)
    portConflicts.push(
      `Object storage port conflicts with the monitor port (scheduler + 2 = ${monitorPort}).`,
    );
  if (schedulerPort === WORKER_MONITOR_PORT)
    portConflicts.push(
      `Scheduler port conflicts with the Worker Monitor port (${WORKER_MONITOR_PORT}).`,
    );
  if (objectStoragePort === WORKER_MONITOR_PORT)
    portConflicts.push(
      `Object storage port conflicts with the Worker Monitor port (${WORKER_MONITOR_PORT}).`,
    );
  if (monitorPort === WORKER_MONITOR_PORT)
    portConflicts.push(
      `Monitor port (scheduler + 2 = ${monitorPort}) conflicts with the Worker Monitor port (${WORKER_MONITOR_PORT}).`,
    );

  const hasOciWm = workerManagers.some((wm) => wm.type === "oci_raw");
  const hasOciCredentials =
    ociUserId.trim().length > 0 &&
    ociTenancyId.trim().length > 0 &&
    ociFingerprint.trim().length > 0 &&
    ociPrivateKey.trim().length > 0;
  const ociWmErrors = workerManagers
    .filter((wm) => wm.type === "oci_raw")
    .flatMap((wm) => {
      const errs = [];
      if (!wm.ociCompartmentId?.trim()) errs.push(`OCI worker manager '${wm.id}' requires a Compartment ID`);
      if (!wm.ociAvailabilityDomain?.trim()) errs.push(`OCI worker manager '${wm.id}' requires an Availability Domain`);
      if (!wm.ociSubnetId?.trim()) errs.push(`OCI worker manager '${wm.id}' requires a Subnet ID`);
      return errs;
    });

  const checks = [
    {
      key: "aki",
      label: "AWS Access Key ID is required",
      ok: accessKeyId.trim().length > 0,
    },
    {
      key: "sk",
      label: "AWS Secret Access Key is required",
      ok: secretKey.trim().length > 0,
    },
    {
      key: "wm",
      label: "At least one worker manager must be configured",
      ok: workerManagers.length > 0,
    },
    {
      key: "ports",
      label: portConflicts.join(" "),
      ok: portConflicts.length === 0,
    },
    ...(hasOciWm
      ? [
          {
            key: "oci_creds",
            label: "An OCI worker manager is configured but OCI credentials have not been provided",
            ok: hasOciCredentials,
          },
          ...ociWmErrors.map((msg, i) => ({ key: `oci_wm_${i}`, label: msg, ok: false })),
        ]
      : []),
  ];
  const blocking = checks.filter((c) => !c.ok);
  const formReady = blocking.length === 0;
  const isRunning = phase === "provisioning" || phase === "destroying";
  const isPaused = phase === "paused";

  const handleLaunch = useCallback(async () => {
    const isResume = phase === "paused" && pausedOp === "deploy";
    if (!isResume && !formReady) return;
    setLog((prev) => (isResume ? [...prev, { text: "", cls: "dim" }] : []));
    if (!isResume) {
      try {
        localStorage.removeItem("scaler_log");
      } catch (_) {}
    }
    setPhase("provisioning");
    setPausedOp(null);
    const resumeState = isResume ? provState : null;
    const cfg = {
      region,
      nameSuffix: resumeState ? resumeState.name_suffix : randomSuffix(),
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
        { accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey },
        addLog,
        savePartial,
        (name, mat) => setKeyMaterial({ name, mat }),
        controller.signal,
        resumeState,
      );
      savePartial(state);
      setPhase("ready");
    } catch (err) {
      if (err.name === "RetryPausedError") {
        addLog(
          "\nDeployment paused — retries exhausted: " + err.message +
            "\nUse Resume to continue or switch to teardown.",
          "warn",
        );
        setPausedOp("deploy");
        setPhase("paused");
      } else if (err.name === "AbortError") {
        addLog(
          "\nAborted. Any resources created so far are saved — use Destroy to clean them up.",
          "warn",
        );
        setPhase("error");
      } else {
        const snapshot = partialRef.current;
        if (snapshot) {
          addLog("\nProvisioning failed: " + err.message, "err");
          addLog("Auto-tearing down created resources…", "warn");
          setPhase("destroying");
          const tdController = new AbortController();
          abortRef.current = tdController;
          try {
            await teardown(
              snapshot,
              { accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey },
              addLog,
              tdController.signal,
            );
            try {
              localStorage.removeItem("scaler_state");
              localStorage.removeItem("scaler_log");
            } catch (_) {}
            setProvState(null);
            partialRef.current = null;
            setKeyMaterial(null);
            setPhase("idle");
          } catch (tdErr) {
            if (tdErr.name === "RetryPausedError") {
              addLog(
                "\nAuto-teardown paused — retries exhausted. Use Resume Teardown.",
                "warn",
              );
              setPausedOp("teardown");
              setPhase("paused");
            } else {
              addLog("\nAuto-teardown failed: " + tdErr.message, "err");
              setPhase("error");
            }
          }
        } else {
          addLog("\nError: " + err.message, "err");
          setPhase("error");
        }
      }
    } finally {
      abortRef.current = null;
    }
  }, [
    phase,
    pausedOp,
    provState,
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
    ociUserId,
    ociTenancyId,
    ociFingerprint,
    ociPrivateKey,
    addLog,
    savePartial,
  ]);

  const handleAbort = useCallback(() => {
    if (abortRef.current) abortRef.current.abort();
  }, []);

  const handleDestroy = useCallback(async () => {
    if (!provState || !hasCredentials) return;
    const isResume = phase === "paused" && pausedOp === "teardown";
    if (!isResume) {
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
            (provState.iam && provState.iam.created
              ? "• IAM role & profile\n"
              : "") +
            "\nThis cannot be undone.",
        )
      )
        return;
    }
    setPhase("destroying");
    setPausedOp(null);
    if (!isResume) setActiveTab("deployment");
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      await teardown(
        provState,
        { accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey },
        addLog,
        controller.signal,
      );
      try {
        localStorage.removeItem("scaler_state");
        localStorage.removeItem("scaler_log");
      } catch (_) {}
      setProvState(null);
      setKeyMaterial(null);
      setPhase("idle");
    } catch (err) {
      if (err.name === "RetryPausedError") {
        addLog(
          "\nTeardown paused — retries exhausted: " + err.message +
            "\nUse Resume Teardown to retry.",
          "warn",
        );
        setPausedOp("teardown");
        setPhase("paused");
      } else if (err.name === "AbortError") {
        addLog(
          "\nTeardown aborted. Some resources may still exist — run Destroy again to retry.",
          "warn",
        );
        setPhase("ready");
      } else {
        addLog(
          "\nError during teardown: " + err.message + "\nFix the issue and run Destroy again to retry.",
          "err",
        );
        setPhase("ready");
      }
    } finally {
      abortRef.current = null;
    }
  }, [phase, pausedOp, provState, hasCredentials, accessKeyId, secretKey, addLog, setActiveTab]);

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
            color: dis
              ? "var(--text-dim)"
              : value === val
                ? "var(--text-accent)"
                : "var(--text-muted)",
            transition: "background 0.15s, color 0.15s",
          }}
        >
          {lbl}
        </button>
      ))}
    </div>
  );

  const _destroyBtnStyle = (disabled) => ({
    padding: "8px 20px",
    background: disabled
      ? "rgba(255,80,60,0.04)"
      : "linear-gradient(135deg, oklch(0.32 0.18 15) 0%, oklch(0.26 0.14 30) 100%)",
    border: "1px solid " + (disabled ? "var(--border-danger)" : "oklch(0.48 0.18 15)"),
    borderRadius: 4,
    color: disabled ? "var(--text-danger)" : "oklch(0.88 0.1 30)",
    fontFamily: "inherit",
    fontSize: 11,
    fontWeight: 700,
    cursor: disabled ? "default" : "pointer",
    transition: "all 0.2s",
    flexShrink: 0,
  });

  let launchControl;
  if (isPaused) {
    launchControl = (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div
          style={{
            padding: "6px 12px",
            background: "rgba(255,160,60,0.06)",
            border: "1px solid rgba(255,160,60,0.35)",
            borderRadius: 4,
            color: "var(--text-warning)",
            fontSize: 11,
          }}
        >
          {pausedOp === "teardown" ? "Teardown paused" : "Deploy paused"}
        </div>
        {pausedOp === "deploy" && (
          <button
            onClick={handleLaunch}
            style={{
              padding: "6px 14px",
              background: "linear-gradient(135deg, oklch(0.38 0.16 155) 0%, oklch(0.32 0.14 200) 100%)",
              border: "1px solid oklch(0.55 0.16 155)",
              borderRadius: 4,
              color: "oklch(0.92 0.1 155)",
              fontFamily: "inherit",
              fontSize: 11,
              fontWeight: 700,
              cursor: "pointer",
              flexShrink: 0,
            }}
          >
            Resume Deploy
          </button>
        )}
        <button
          onClick={handleDestroy}
          disabled={!hasCredentials}
          style={_destroyBtnStyle(!hasCredentials)}
        >
          {pausedOp === "teardown" ? "Resume Teardown" : "Switch to Teardown"}
          {!hasCredentials ? " (missing credentials)" : ""}
        </button>
      </div>
    );
  } else if (phase === "error" && provState) {
    launchControl = (
      <button
        onClick={handleDestroy}
        disabled={!hasCredentials}
        style={_destroyBtnStyle(!hasCredentials)}
      >
        Destroy Cluster{!hasCredentials ? " (missing credentials)" : ""}
      </button>
    );
  } else if (phase === "idle" || phase === "error") {
    const launchBtn = (
      <button
        onClick={formReady ? handleLaunch : undefined}
        disabled={!formReady}
        style={{
          padding: "8px 20px",
          background: !formReady
            ? "rgba(229,72,77,0.05)"
            : "linear-gradient(135deg, oklch(0.38 0.16 155) 0%, oklch(0.32 0.14 200) 100%)",
          border:
            "1px solid " +
            (!formReady ? "var(--border-danger)" : "oklch(0.55 0.16 155)"),
          borderRadius: 4,
          color: !formReady ? "var(--text-muted)" : "oklch(0.92 0.1 155)",
          fontFamily: "inherit",
          fontSize: 11,
          fontWeight: 700,
          cursor: !formReady ? "default" : "pointer",
          transition: "all 0.2s",
          display: "flex",
          alignItems: "center",
          gap: 7,
          pointerEvents: !formReady ? "none" : undefined,
        }}
      >
        {!formReady && (
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: "var(--text-danger)",
              flexShrink: 0,
            }}
          />
        )}
        Launch Scheduler
      </button>
    );
    launchControl = !formReady ? (
      <HelpTip text={blocking.map((c) => "- " + c.label).join("\n")} width={520}>{launchBtn}</HelpTip>
    ) : (
      launchBtn
    );
  } else if (phase === "ready") {
    launchControl = (
      <button
        onClick={handleDestroy}
        disabled={!hasCredentials}
        style={_destroyBtnStyle(!hasCredentials)}
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
            background:
              phase === "destroying"
                ? "rgba(255,80,60,0.04)"
                : "rgba(0,200,224,0.04)",
            border:
              "1px solid " +
              (phase === "destroying"
                ? "var(--border-danger)"
                : "var(--border-accent)"),
            borderRadius: 4,
            color:
              phase === "destroying"
                ? "var(--text-danger)"
                : "var(--text-muted)",
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
        showPostLaunch={
          (phase !== "idle" && phase !== "error") ||
          ["deployment", "logs", "worker-monitor"].includes(activeTab)
        }
        launchControl={launchControl}
        workerMonitorAddress={phase === "ready" ? provState?.worker_monitor_address : undefined}
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
                    const active = id === credTab;
                    const disabled = id === "ibm";
                    return (
                      <button
                        key={id}
                        disabled={disabled}
                        onClick={() => !disabled && setCredTab(id)}
                        style={{
                          padding: "5px 12px",
                          fontFamily: "inherit",
                          fontSize: 10,
                          letterSpacing: "0.08em",
                          textTransform: "uppercase",
                          cursor: disabled ? "default" : "pointer",
                          border: "none",
                          marginBottom: -1,
                          borderBottom: active
                            ? "2px solid var(--tab-active)"
                            : "2px solid transparent",
                          background: "transparent",
                          color: active
                            ? "var(--text-label)"
                            : "var(--text-dim)",
                          opacity: disabled ? 0.35 : 1,
                        }}
                      >
                        {lbl}
                      </button>
                    );
                  })}
                </div>
                {credTab === "aws" && (
                  <>
                    <div>
                      <Label help="The AWS region where your cluster will be deployed.">
                        AWS Region
                      </Label>
                      <RegionSelect value={region} onChange={setRegion} />
                    </div>
                    <div
                      style={{ display: "flex", flexDirection: "column", gap: 6 }}
                    >
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
                          Key ID
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
                          Secret
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
                        onMouseOver={(e) =>
                          (e.currentTarget.style.color = "var(--text-accent)")
                        }
                        onMouseOut={(e) =>
                          (e.currentTarget.style.color = "var(--text-muted)")
                        }
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
                        Your credentials are used from this browser to provision AWS
                        resources and are made available to the scheduler instance
                        for worker management. They are not stored by this
                        application.
                      </span>
                    </div>
                  </>
                )}
                {credTab === "oci" && (
                  <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                    <div>
                      <Label help="OCI user OCID - found in OCI Console under Profile.">User OCID</Label>
                      <div style={{ ...inp, display: "flex", alignItems: "center" }}>
                        <SecretInput
                          value={ociUserId}
                          onChange={setOciUserId}
                          placeholder="ocid1.user.oc1..aaa..."
                          style={{ flex: 1, fontSize: 12, color: "var(--text-primary)" }}
                        />
                      </div>
                    </div>
                    <div>
                      <Label help="OCI tenancy OCID - found in Administration > Tenancy Details.">Tenancy OCID</Label>
                      <div style={{ ...inp, display: "flex", alignItems: "center" }}>
                        <SecretInput
                          value={ociTenancyId}
                          onChange={setOciTenancyId}
                          placeholder="ocid1.tenancy.oc1..aaa..."
                          style={{ flex: 1, fontSize: 12, color: "var(--text-primary)" }}
                        />
                      </div>
                    </div>
                    <div>
                      <Label help="Fingerprint of the API key pair registered in OCI Console.">Fingerprint</Label>
                      <div style={{ ...inp, display: "flex", alignItems: "center" }}>
                        <SecretInput
                          value={ociFingerprint}
                          onChange={setOciFingerprint}
                          placeholder="aa:bb:cc:dd:ee:ff"
                          style={{ flex: 1, fontSize: 12, color: "var(--text-primary)" }}
                        />
                      </div>
                    </div>
                    <div>
                      <Label help="Paste the contents of your OCI API private key PEM file.">Private Key (PEM)</Label>
                      <textarea
                        value={ociPrivateKey}
                        onChange={(e) => setOciPrivateKey(e.target.value)}
                        placeholder={"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"}
                        style={{
                          ...inp,
                          fontFamily: "monospace",
                          fontSize: 10,
                          resize: "vertical",
                          minHeight: 80,
                          lineHeight: 1.5,
                          boxSizing: "border-box",
                        }}
                      />
                    </div>
                    <span
                      style={{ fontSize: 10, color: "var(--text-dim)", lineHeight: 1.5 }}
                    >
                      OCI credentials are written to ~/.oci/config on the scheduler
                      instance so the worker manager can authenticate with OCI.
                      They are not stored by this application.
                    </span>
                  </div>
                )}
              </PanelBox>

              <PanelBox title="General Options">
                {IS_ADVANCED && (
                  <div>
                    <Label
                      help={
                        "WebSocket - connect to your cluster from a browser or any WebSocket client.\n---\nTCP - direct socket connection; slightly lower overhead."
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
                      onSelect={setTransport}
                    />
                  </div>
                )}
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
                  <InstancePicker
                    value={schedulerType}
                    onChange={setSchedulerType}
                    defaultCat="all"
                  />
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
                {IS_ADVANCED && (
                  <div
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      gap: 10,
                    }}
                  >
                    <div>
                      <Label>Scheduler Port</Label>
                      <NumericStepper
                        value={schedulerPort}
                        onChange={setSchedPort}
                        min={1024}
                        max={65535}
                        width={80}
                      />
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
                      <div
                        style={{
                          color: "var(--text-danger)",
                          fontSize: 11,
                          lineHeight: 1.5,
                        }}
                      >
                        {portConflicts.map((msg, i) => (
                          <div key={i}>{msg}</div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
                <div>
                  <Label
                    help={
                      "- Installed on the scheduler instance\n- Shared by native worker manager workers (same instance)\n- opengris-scaler must be included"
                    }
                  >
                    requirements.txt
                  </Label>
                  <textarea
                    value={schedulerRequirements}
                    onChange={(e) => setSchedulerReqs(e.target.value)}
                    spellCheck={false}
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
                  {[
                    {
                      label: "Engine",
                      help: "Policy engine that controls task allocation and worker scaling.",
                      options: ["simple", "waterfall_v1"],
                    },
                    {
                      label: "Allocate",
                      help: "How tasks are assigned to workers. even_load distributes work evenly; capability routes tasks to workers that advertise matching capabilities.",
                      options: ["even_load", "capability"],
                    },
                    {
                      label: "Scaling",
                      help: "How the scheduler scales worker counts up or down. vanilla uses a task-to-worker ratio; capability scales per-capability group; no disables autoscaling.",
                      options: ["vanilla", "no", "capability"],
                    },
                  ].map(({ label, help, options }) => (
                    <div key={label}>
                      <Label help={help}>{label}</Label>
                      <div style={{ position: "relative" }}>
                        <select
                          disabled
                          style={{
                            width: "100%",
                            background: "var(--bg-surface)",
                            border: "1px solid var(--border-accent)",
                            borderRadius: 3,
                            padding: "7px 28px 7px 10px",
                            color: "var(--text-primary)",
                            fontFamily: "inherit",
                            fontSize: 12,
                            outline: "none",
                            appearance: "none",
                            WebkitAppearance: "none",
                          }}
                        >
                          {options.map((o) => (
                            <option key={o} value={o}>
                              {o}
                            </option>
                          ))}
                        </select>
                        <span
                          style={{
                            position: "absolute",
                            right: 10,
                            top: "50%",
                            display: "block",
                            width: 7,
                            height: 7,
                            borderRight: "1.5px solid var(--text-muted)",
                            borderBottom: "1.5px solid var(--text-muted)",
                            transform: "rotate(45deg)",
                            marginTop: "-5px",
                            pointerEvents: "none",
                          }}
                        />
                      </div>
                    </div>
                  ))}
                </PanelBox>
              </div>
            </div>

            {/* Column 3: Worker Managers + Cost Summary */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox
                title={`Worker Managers (${workerManagers.length})`}
                style={{ gap: 8, padding: "16px 22px" }}
              >
                <div
                  style={{
                    display: "flex",
                    marginLeft: -22,
                    marginRight: -22,
                    borderTop: "1px solid var(--border-accent)",
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
                      maxHeight: 420,
                      alignSelf: "flex-start",
                    }}
                  >
                    {workerManagers.map((wm) => (
                      <div
                        key={wm.id}
                        style={{
                          display: "flex",
                          alignItems: "stretch",
                          background:
                            selectedWmId === wm.id
                              ? "rgba(0,200,224,0.1)"
                              : "transparent",
                          borderLeft:
                            selectedWmId === wm.id
                              ? "2px solid var(--tab-active)"
                              : "2px solid transparent",
                          borderBottom: "1px solid rgba(255,255,255,0.04)",
                          transition: "background 0.12s",
                        }}
                      >
                        <button
                          title={wm.id}
                          onClick={() => setSelectedWmId(wm.id)}
                          style={{
                            flex: 1,
                            background: "transparent",
                            border: "none",
                            color:
                              selectedWmId === wm.id
                                ? "var(--text-accent)"
                                : "var(--text-muted)",
                            fontFamily: "inherit",
                            fontSize: 10,
                            padding: "10px 6px 10px 10px",
                            textAlign: "left",
                            cursor: "pointer",
                            letterSpacing: "0.05em",
                            transition: "color 0.12s",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                            minWidth: 0,
                          }}
                        >
                          {wm.id}
                        </button>
                        {workerManagers.length > 1 && (
                          <button
                            onClick={() => removeWorkerManager(wm.id)}
                            title="Remove"
                            style={{
                              background: "transparent",
                              border: "none",
                              color: "var(--text-muted)",
                              fontFamily: "inherit",
                              fontSize: 9,
                              padding: 0,
                              margin: "0 7px",
                              cursor: "pointer",
                              flexShrink: 0,
                              alignSelf: "center",
                              display: "flex",
                              alignItems: "center",
                              transition: "color 0.12s",
                            }}
                            onMouseEnter={(e) => {
                              e.currentTarget.style.color =
                                "var(--text-danger)";
                              e.currentTarget.querySelector(
                                "span",
                              ).style.borderColor = "var(--border-danger)";
                              e.currentTarget.querySelector(
                                "span",
                              ).style.background = "rgba(229,72,77,0.08)";
                            }}
                            onMouseLeave={(e) => {
                              e.currentTarget.style.color = "var(--text-muted)";
                              e.currentTarget.querySelector(
                                "span",
                              ).style.borderColor = "var(--border-accent)";
                              e.currentTarget.querySelector(
                                "span",
                              ).style.background = "transparent";
                            }}
                          >
                            <span
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                justifyContent: "center",
                                width: 14,
                                height: 14,
                                border: "1px solid var(--border-accent)",
                                borderRadius: 2,
                                transition:
                                  "border-color 0.12s, background 0.12s",
                              }}
                            >
                              ✕
                            </span>
                          </button>
                        )}
                      </div>
                    ))}
                    <button
                      onClick={addWorkerManager}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.background =
                          "rgba(0,200,224,0.08)";
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
                  <div style={{ flex: 1, padding: "14px 16px" }}>
                    {workerManagers
                      .filter((wm) => wm.id === selectedWmId)
                      .map((wm) => (
                        <React.Fragment key={wm._uid}>
                          <WorkerManagerCard
                            wm={wm}
                            onChange={(updated) => {
                              if (updated.id !== wm.id)
                                setSelectedWmId(updated.id);
                              updateWorkerManager(wm.id, updated);
                            }}
                            onRemove={() => removeWorkerManager(wm.id)}
                            allInstances={allInstances}
                            canRemove={workerManagers.length > 1}
                            fullWidth={true}
                          />
                        </React.Fragment>
                      ))}
                  </div>
                </div>
              </PanelBox>

              <PanelBox title="Cost Summary">
                {workerManagers.map((wm, idx) => {
                  const label = wm.id || `(wm ${idx + 1})`;
                  if (wm.type === "orb_aws_ec2") {
                    const inst = allInstances.find((i) => i.type === wm.instanceType) || { price: 0 };
                    const count =
                      wm.capMode === "instances"
                        ? Math.max(0, wm.instanceCap || 0)
                        : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
                    return (
                      <div key={wm._uid} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
                          {label} · {count}× {wm.instanceType}
                        </span>
                        <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                          USD {(count * inst.price).toFixed(2)}/h
                        </span>
                      </div>
                    );
                  }
                  if (wm.type === "oci_raw") {
                    const shape = wm.ociShape || "CI.Standard.A1.Flex";
                    const shapeName = shape === "CI.Standard.A1.Flex" ? "ARM - Ampere A1" : "x86 - Standard E4";
                    const pricing = OCI_SHAPE_PRICING[shape] || OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
                    const cost = pricing.ocpuPrice * (wm.ociOcpus || 4) + pricing.memPrice * (wm.ociMemoryGb || 30);
                    return (
                      <div key={wm._uid} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
                          {label} · {shapeName} · {wm.ociOcpus || 4} OCPU · {wm.ociMemoryGb || 30}GB
                        </span>
                        <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                          USD {cost.toFixed(2)}/h
                        </span>
                      </div>
                    );
                  }
                  return (
                    <div key={wm._uid} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                      <span style={{ fontSize: 10, color: "var(--text-muted)" }}>{label}</span>
                      <span style={{ fontSize: 11, color: "var(--text-dim)", fontStyle: "italic" }}>n/a</span>
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
                  <span
                    style={{ fontSize: 12, color: "var(--text-secondary)" }}
                  >
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

          {phase === "idle" && (
            <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
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
          <LiveTerminal
            lines={log}
            isRunning={isRunning}
            bare
            style={{ minHeight: 0 }}
          />
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
                    downloadText(
                      "scaler-state-" + provState.name_suffix + ".json",
                      JSON.stringify(provState, null, 2),
                    )
                  }
                  isRunning={isRunning}
                  keyMaterial={keyMaterial}
                />
                <GettingStartedCard
                  schedulerAddress={provState.scheduler_address}
                  ready={phase === "ready"}
                />
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
            <div style={{ color: "var(--text-muted)", fontSize: 12 }}>
              No instance deployed yet.
            </div>
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

      {/* ── Worker Monitor Tab ── */}
      <div
        style={{
          display: activeTab === "worker-monitor" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        {!provState?.worker_monitor_address ? (
          <div
            style={{
              padding: "20px 28px",
              color: "var(--text-muted)",
              fontSize: 12,
            }}
          >
            Worker Monitor address not yet available.
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
              <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                {provState.worker_monitor_address}
              </span>
              <a
                href={provState.worker_monitor_address}
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
              {workerMonitorReady ? (
                <span style={{ fontSize: 10, color: "var(--text-success)" }}>
                  server ready
                </span>
              ) : (
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
                  waiting for server… {workerMonitorElapsed}s
                </span>
              )}
            </div>
            {workerMonitorReady ? (
              <iframe
                src={provState.worker_monitor_address}
                style={{
                  flex: 1,
                  border: "none",
                  background: "var(--bg-page)",
                }}
                title="Scaler Worker Monitor"
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
                <div style={{ fontSize: 13 }}>
                  Waiting for Worker Monitor server to start
                </div>
                <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
                  {workerMonitorElapsed}s elapsed · retrying every 5s
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
